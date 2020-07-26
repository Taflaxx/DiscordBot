from discord.ext import commands, tasks
import datetime


class Reminder:
    def __init__(self, ctx, time, message):
        self.ctx = ctx
        self.time = time.replace(microsecond=0)
        self.message = message
        print(f"Created new reminder for {self.ctx.author} at {self.time} with message \"{self.message}\"")

    def elapsed(self):
        if self.time <= datetime.datetime.now():
            return True
        return False

    def reminder_message(self):
        if self.message == "":
            return f"{self.ctx.author.mention} Reminder!"
        else:
            return f"{self.ctx.author.mention} Reminder: {self.message}"

    async def notify(self):
        await self.ctx.send(self.reminder_message())
        print(f"Notified user {self.ctx.author}: {self.reminder_message()}")

    def __str__(self):
        return f"{self.time} | {self.ctx.author} | {self.message}"


class ReminderManager(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.reminders = []
        self.check_reminders.start()

    def cog_unload(self):
        self.check_reminders.cancel()

    @commands.group(name="remindme", aliases=["rm"], help="Reminds you of stuff")
    async def remind_me(self, ctx):
        if ctx.invoked_subcommand is None:
            await ctx.send_help("remindme")
            print(f"Unknown subcommand \"{ctx.message.content}\" by {ctx.author}.Sent help page")

    @remind_me.command(name="in", help="Add a reminder in X time")
    async def add_in(self, ctx, hours, minutes, message=""):
        time = datetime.datetime.now() + datetime.timedelta(hours=int(hours), minutes=int(minutes))
        self.reminders.append(Reminder(ctx, time, message))
        await ctx.send(f"**`SUCCESS:`** I will remind you at {time.replace(microsecond=0)}")

    @remind_me.command(name="list", aliases=["l", "ls"], help="List all of your reminders")
    async def list(self, ctx):
        message = ""
        for reminder in self.reminders:
            if reminder.ctx.author == ctx.author:
                message += f"\n{str(reminder)}"
        if message == "":
            await ctx.send("Currently no reminders scheduled")
        else:
            await ctx.send("List of reminders:" + message)

    @remind_me.command(name="clear", help="Removes all of your reminders")
    async def clear(self, ctx):
        counter = 0
        for reminder in self.reminders:
            if reminder.ctx.author == ctx.author:
                self.reminders.remove(reminder)
                print(f"Removed reminder: {str(reminder)}")
                counter += 1
        if counter == 0:
            await ctx.send("Currently no reminders scheduled")
        else:
            await ctx.send(f"Removed {counter} reminders")

    # Checks all reminders every 5 seconds
    @tasks.loop(seconds=5.0)
    async def check_reminders(self):
        for reminder in self.reminders:
            if reminder.elapsed():
                await reminder.notify()
                self.reminders.remove(reminder)


def setup(bot):
    bot.add_cog(ReminderManager(bot))
