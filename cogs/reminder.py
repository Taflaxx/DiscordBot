from discord.ext import commands, tasks
import datetime


class Reminder:
    def __init__(self,ctx, time, message):
        self.ctx = ctx
        self.time = time.replace(microsecond=0)
        self.message = message
        print(f"Created new reminder for {self.ctx.author} at {self.time} with message \"{self.message}\"")

    def elapsed(self):
        if self.time <= datetime.datetime.now():
            return True
        return False

    def reminder_message(self):
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

    @commands.group(aliases=["rm", "r", "remind"])
    async def reminder(self, ctx):
        if ctx.invoked_subcommand is None:
            await ctx.send("Invalid remind command.")

    @reminder.command(aliases=["a"])
    async def add(self, ctx, hours, minutes, message):
        time = datetime.datetime.now() + datetime.timedelta(hours=int(hours), minutes=int(minutes))
        self.reminders.append(Reminder(ctx, time, message))

    @reminder.command(aliases=["l","ls"])
    async def list(self, ctx):
        if not self.reminders:
            await ctx.send("Currently no reminders scheduled")
        else:
            message = "List of reminders:"
            for reminder in self.reminders:
                message += f"\n{str(reminder)}"
            await ctx.send(message)

    # Checks all reminders every 5 seconds
    @tasks.loop(seconds=5.0)
    async def check_reminders(self):
        for reminder in self.reminders:
            if reminder.elapsed():
                await reminder.notify()
                self.reminders.remove(reminder)


def setup(bot):
    bot.add_cog(ReminderManager(bot))
