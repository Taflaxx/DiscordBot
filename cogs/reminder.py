from discord.ext import commands, tasks
import datetime


class Reminder:
    def __init__(self,ctx, time, message):
        self.ctx = ctx
        self.time = time
        self.message = message

    def elapsed(self):
        if self.time <= datetime.datetime.now():
            return True
        return False

    async def notify(self):
        await self.ctx.send(f"{self.ctx.author.mention}: {self.message}")
        print(f"Notified user {self.ctx.author}: {self.ctx.author.mention}: {self.message}")


class ReminderManager(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.reminders = []
        self.check_reminders.start()

    def cog_unload(self):
        self.check_reminders.cancel()

    @commands.command(aliases=["rm"])
    async def remindme(self, ctx, hours, minutes, message):
        time = datetime.datetime.now() + datetime.timedelta(hours=int(hours),minutes=int(minutes))
        self.reminders.append(Reminder(ctx, time, message))
        print(f"Added new reminder at {time}: {message}")

    # Checks all reminders every 5 seconds
    @tasks.loop(seconds=5.0)
    async def check_reminders(self):
        for reminder in self.reminders:
            if reminder.elapsed():
                await reminder.notify()
                self.reminders.remove(reminder)


def setup(bot):
    bot.add_cog(ReminderManager(bot))
