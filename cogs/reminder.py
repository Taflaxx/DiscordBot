from discord.ext import commands, tasks
import datetime
import re
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, String, create_engine
import uuid
import logging

# Set up logging
logger = logging.getLogger('sqlalchemy.engine')
logger.setLevel(logging.DEBUG)
handler = logging.FileHandler(filename='reminder.log', encoding='utf-8', mode='w')
handler.setFormatter(logging.Formatter('%(asctime)s:%(levelname)s:%(name)s: %(message)s'))
logger.addHandler(handler)

# Init DB
Base = declarative_base()
engine = create_engine('sqlite:///reminder.db', echo=False)
Session = sessionmaker(bind=engine)
db = Session()


class ReminderDB(Base):
    __bind_key__ = 'reminder'
    __tablename__ = "reminder"

    id = Column(String, primary_key=True)
    time = Column(String)
    user = Column(Integer)
    channel = Column(Integer)
    message = Column(String)


Base.metadata.create_all(engine)


class Reminder:
    # Create a new reminder
    def __init__(self, bot, user, channel, time, message, id=None, db_object=None):
        self.bot = bot
        self.time = time.replace(microsecond=0)
        self.user = user
        self.channel = channel
        self.message = message

        if id:
            self.id = id
        else:
            self.id = str(uuid.uuid4())

        if db_object:
            self.db_object = db_object
            print(f"Loaded reminder for {self.user} at {self.time} with message \"{self.message}\"")
        else:
            # Add reminder to db
            self.db_object = ReminderDB(id=self.id, time=self.time.strftime("%Y-%m-%d %H:%M:%S"), user=self.user.id,
                                        channel=self.channel.id, message=self.message)
            db.add(self.db_object)
            db.commit()
            print(f"Created new reminder for {self.user} at {self.time} with message \"{self.message}\"")

    # Load a reminder from the DB
    @classmethod
    def from_db(cls, bot, db_object: ReminderDB):
        bot = bot
        db_object = db_object
        id = db_object.id
        time = datetime.datetime.strptime(db_object.time, "%Y-%m-%d %H:%M:%S")
        user = bot.get_user(db_object.user)
        channel = bot.get_channel(db_object.channel)
        message = db_object.message
        return cls(bot=bot, db_object=db_object, id=id, time=time, user=user, channel=channel, message=message)

    def elapsed(self):
        if self.time <= datetime.datetime.now():
            return True
        return False

    def reminder_message(self):
        if self.message == "":
            return f"{self.user.mention} Reminder!"
        else:
            return f"{self.user.mention} Reminder: {self.message}"

    async def notify(self):
        await self.channel.send(self.reminder_message())
        print(f"Notified user {self.user}: {self.reminder_message()}")

        # Delete from db
        db.delete(self.db_object)
        db.commit()

    def __str__(self):
        return f"{self.time} | {self.user} | {self.message}"


class ReminderManager(commands.Cog, name="reminder"):
    def __init__(self, bot):
        self.bot = bot
        self.reminders = []

        # Load from db
        for reminder in db.query(ReminderDB):
            self.reminders.append(Reminder.from_db(self.bot, reminder))

        self.check_reminders.start()

    def cog_unload(self):
        self.check_reminders.cancel()

    @commands.group(name="remindme", aliases=["rm"], help="Reminds you of stuff")
    async def remind_me(self, ctx):
        if ctx.invoked_subcommand is None:
            await ctx.send_help("remindme")
            print(f"Unknown subcommand \"{ctx.message.content}\" by {ctx.author}. Sent help page")

    @remind_me.command(name="in", help="Remind me after some time has passed\n"
                                       "Example: !rm in 10h 15m 30s Meeting with Chris",
                       usage="[1h|1m|1s] [Message]")
    async def add_in(self, ctx, *args):
        hours, minutes, seconds = 0, 0, 0
        message = ""
        for arg in args:
            if re.match("^\d+h$", arg):
                hours += int(arg[:-1])
            elif re.match("^\d+m$", arg):
                minutes += int(arg[:-1])
            elif re.match("^\d+s$", arg):
                seconds += int(arg[:-1])
            else:
                message += arg + " "
        time = datetime.datetime.now() + datetime.timedelta(hours=hours, minutes=minutes, seconds=seconds)
        self.reminders.append(Reminder(self.bot, ctx.author, ctx.channel, time, message))
        await ctx.send(f"**`SUCCESS:`** I will remind you at {time.replace(microsecond=0)}")

    @remind_me.command(name="list", aliases=["l", "ls"], help="List all of your reminders")
    async def list(self, ctx):
        message = ""
        for reminder in self.reminders:
            if reminder.user == ctx.author:
                message += f"\n{str(reminder)}"
        if message == "":
            await ctx.send("Currently no reminders scheduled")
        else:
            await ctx.send("List of reminders:" + message)

    @remind_me.command(name="clear", help="Removes all of your reminders")
    async def clear(self, ctx):
        counter = 0
        for reminder in self.reminders:
            if reminder.user == ctx.author:
                self.reminders.remove(reminder)
                print(f"Removed reminder: {str(reminder)}")
                counter += 1
        if counter == 0:
            await ctx.send("Currently no reminders scheduled")
        else:
            await ctx.send(f"Removed {counter} reminder(s)")

    # Checks all reminders every 5 seconds
    @tasks.loop(seconds=5.0)
    async def check_reminders(self):
        for reminder in self.reminders:
            if reminder.elapsed():
                await reminder.notify()
                self.reminders.remove(reminder)


async def setup(bot):
    await bot.add_cog(ReminderManager(bot))
