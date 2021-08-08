from discord.ext import commands
from discord import Embed, File, TextChannel
import logging
import os
import csv
from cogs.logmanager.utils import *
from cogs.logmanager.db import *
from sqlalchemy import func

# Set up logging
logger = logging.getLogger("sqlalchemy.engine")
logger.setLevel(logging.DEBUG)
handler = logging.FileHandler(filename="cogs/logmanager/logmanager.log", encoding="utf-8", mode="w")
handler.setFormatter(logging.Formatter("%(asctime)s:%(levelname)s:%(name)s: %(message)s"))
logger.addHandler(handler)


class LogManager(commands.Cog, name="LogManager"):
    def __init__(self, bot):
        self.bot = bot

    def cog_unload(self):
        pass

    @commands.group(name="log", aliases=["l"], help="For all your logging needs")
    async def log(self, ctx):
        if ctx.invoked_subcommand is None:
            await ctx.send_help("log")
            print(f"Unknown subcommand \"{ctx.message.content}\" by {ctx.author}. Sent help page")

    @log.command(name="add", help="Add logs to the database", usage="[log(s)]")
    async def add_logs(self, ctx, *, arg):
        # Find all links to logs in the message
        logs = re.findall("https:\/\/dps\.report\/[a-zA-Z\-0-9\_]+", arg)
        message = await ctx.send(f"Found {len(logs)} logs:")

        errors = 0  # Tracks the number of errors while adding logs
        for log in logs:
            r = await add_log(log)
            if r is not None:
                errors += 1
                await message.edit(content=f"{message.content}\n{r}")  # update original message with errors
        db.commit()
        await message.edit(content=f"{message.content}\nAdded {len(logs) - errors}/{len(logs)} logs to the database.")

    @log.command(name="filter", aliases=["f"], help="Search for logs",
                 usage="\nOptions:\n"
                       "-h, -help\tShows this page\n"
                       "-a <account>\tFilter by account name\n"
                       "-c <character>\tFilter by character name\n"
                       "-p <profession>\tFilter by profession\n"
                       "-b <boss>\tFilter by boss\n"
                       "-cm \tOnly show challenge mode bosses\n"
                       "-nm \tOnly show normal mode bosses\n"
                       "-before <YYYY-MM-DD>\tOnly show logs from before the date\n"
                       "-after <YYYY-MM-DD>\tOnly show logs after the date\n"
                       "-order <dps|dmg|date|duration>\tSet stat to order by\n"
                       "-asc\tAscending oder\n"
                       "-desc\tDescending order\n"
                       "-limit\tNumber of logs to show [1-30, default 10]\n"
                       "-csv\t Export query result as a csv file")
    async def filter_log(self, ctx, *args):
        if "-h" in args or "-help" in args:
            await ctx.send_help("log filter")
            return
        query = db.query(Player).join(Log)
        query = await filter_args(query, args)
        query, order, limit = await order_args(query, args)

        if query.count() == 0:
            await ctx.send("**:x: No logs found**")
            return

        if "-csv" in args:
            # Create csv, send it and delete it afterwards
            filename = f"cogs/logmanager/tmp/{datetime.now(tz=timezone.utc).strftime('export-%Y%m%d-%H%M%S')}.csv"
            with open(filename, mode="w", newline="") as file:
                csv_writer = csv.writer(file, delimiter=',')
                csv_writer.writerow(["link", "boss", "account", "character", "profession", "dps", "damage"])
                for row in query:
                    csv_writer.writerow([row.log.link, row.log.fight_name, row.account, row.character,
                                         row.profession, row.dps, row.damage])
            await ctx.send(file=File(filename))
            os.remove(filename)

        else:
            try:
                limit = int(limit)
            except ValueError:
                await ctx.send("**:x: Invalid limit**")
                return

            embed = Embed(title="Top Logs", color=0x0099ff)
            val = ""
            for i in range(0, min(limit, query.count(), 30)):
                row = query[i]
                val += f"[{i + 1}. {row.log.fight_name}:]({row.log.link})\n{row.character} - {row.profession}\n" \
                       f"DPS: {row.dps}\nDamage taken: {row.damage}\n\n"
                # Split into a new field every 5 logs because of character limits
                if (i + 1) % 5 == 0:
                    embed.add_field(name=f"Sorted by {order} [{i - 3} - {i+1}]:", value=val)
                    val = ""
                # For better formatting (max 2 fields next to each other)
                if (i + 1) % 10 == 0:
                    embed.add_field(name="\u200b", value="\u200b")

            await ctx.send(embed=embed)

    @log.command(name="history", help="Search a Discord channel for logs", usage="<channel> [message_limit]")
    @commands.is_owner()
    async def parse_channel(self, ctx, channel: TextChannel, limit: int = 100):
        messages = await channel.history(limit=limit).flatten()
        log_counter = 0
        errors = 0  # Tracks the number of errors while adding logs
        for message in messages:
            # Find all links to logs in the message
            logs = re.findall("https:\/\/dps\.report\/[a-zA-Z\-0-9\_]+", message.content)

            for log in logs:
                log_counter += 1
                r = await add_log(log)
                if r is not None:
                    print(r)
                    errors += 1
            db.commit()
        await ctx.send(f"Added {log_counter - errors}/{log_counter} logs to the database.")

    @log.group(name="stats", help="Log stats")
    async def stats(self, ctx):
        if ctx.invoked_subcommand is None:
            await ctx.send_help("log stats")
            print(f"Unknown subcommand \"{ctx.message.content}\" by {ctx.author}. Sent help page")

    @stats.command(name="general", help="Show some general stats about the logs")
    async def stats_general(self, ctx):
        # maybe merge "stats general" with "stats boss", add filter
        embed = Embed(title="Log Stats", color=0x0099ff)
        total_logs = db.query(Log.link).count()
        embed.add_field(name="Logs:", value=total_logs)
        embed.add_field(name="Distinct Accounts:", value=db.query(Player.account).distinct().count())
        embed.add_field(name="Distinct Characters:", value=db.query(Player.character).distinct().count())

        embed.add_field(name="Frequent accounts:", value=most_frequent_embed(db.query(Player.account).all()))
        embed.add_field(name="Frequent characters:", value=most_frequent_embed(db.query(Player.character).all()))
        embed.add_field(name="Frequent professions:", value=most_frequent_embed(db.query(Player.profession).all()))

        total_players = db.query(Player.id).count()
        total_dps = db.query(func.sum(Player.dps)).all()[0][0]
        embed.add_field(name="Average DPS:", value=f"Group: {round(total_dps / total_logs)}\nPlayer: {round(total_dps / total_players)}")

        total_damage = db.query(func.sum(Player.damage)).all()[0][0]
        embed.add_field(name="Average damage:", value=f"Group: {round(total_damage / total_logs)}\nPlayer: {round(total_damage / total_players)}")

        total_downs = db.query(func.sum(Player.downs)).all()[0][0]
        embed.add_field(name="Downs:", value=f"Total: {total_downs}\nPer fight: {round(total_downs / total_logs, 1)}", inline=False)

        total_deaths = db.query(func.sum(Player.deaths)).all()[0][0]
        embed.add_field(name="Deaths:", value=f"Total: {total_deaths}\nPer fight: {round(total_deaths / total_logs, 1)}")

        await ctx.send(embed=embed)

    @stats.command(name="boss", help="Show boss specific stats", usage="<boss>")
    async def stats_boss(self, ctx, boss):
        if boss in boss_abrv:
            boss = boss_abrv[boss]
        query = db.query(Log).join(Player)
        query = query.filter(Log.fight_name.ilike(boss))

        # Create embed
        embed = Embed(title=boss, color=0x0099ff)

        # First kill
        first_kill = query.order_by(Log.date_time.asc()).first()
        embed.add_field(name="First kill:", value=f"[{first_kill.date_time.strftime('%B %e, %Y')}]({first_kill.link})")

        # Latest kill
        latest_kill = query.order_by(Log.date_time.desc()).first()
        embed.add_field(name="Latest kill:", value=f"[{latest_kill.date_time.strftime('%B %e, %Y')}]({latest_kill.link})")

        # Fastest kills
        query_fastest = query.distinct(Log.link).order_by(Log.duration.asc())
        val = ""
        for i in range(0, min(5, query_fastest.count())):
            t = query_fastest[i].duration
            val += f"[{t.strftime('%Mm %Ss %f')[:-3]}ms]({query_fastest[i].link})\n"
        embed.add_field(name="Fastest kills:", value=val, inline=False)

        # Average DPS
        total_logs = db.query(Log.link).filter(Log.fight_name.ilike(boss)).count()
        total_players = db.query(Player.id).join(Log).filter(Log.fight_name.ilike(boss)).count()
        total_dps = db.query(func.sum(Player.dps)).join(Log).filter(Log.fight_name.ilike(boss)).all()[0][0]
        embed.add_field(name="Average DPS:", value=f"Group: {round(total_dps / total_logs)}\nPlayer: {round(total_dps / total_players)}")

        # Average Damage
        total_damage = db.query(func.sum(Player.damage)).join(Log).filter(Log.fight_name.ilike(boss)).all()[0][0]
        embed.add_field(name="Average damage:", value=f"Group: {round(total_damage / total_logs)}\nPlayer: {round(total_damage / total_players)}")

        # Downs
        total_downs = db.query(func.sum(Player.downs)).join(Log).filter(Log.fight_name.ilike(boss)).all()[0][0]
        embed.add_field(name="Downs:", value=f"Total: {total_downs}\nPer fight: {round(total_downs / total_logs, 1)}", inline=False)

        # Deaths
        total_deaths = db.query(func.sum(Player.deaths)).join(Log).filter(Log.fight_name.ilike(boss)).all()[0][0]
        embed.add_field(name="Deaths:", value=f"Total: {total_deaths}\nPer fight: {round(total_deaths / total_logs, 1)}")
        await ctx.send(embed=embed)

    @log.command(name="hos")
    async def hall_of_shame(self, ctx):
        embed = Embed(title=f"{self.bot.get_emoji(819226756698603600)} Hall of Shame", color=0x0099ff)
        emoji = [":one:", ":two:", ":three:", ":four:", ":five:"]

        # Minimum amount of logs per player
        limit = min(db.query(Log.link).count()/2, 50)

        # Damage
        _, averages = await get_player_stats(Player.damage, limit)
        val = ""
        for i in range(0, 5):
            val += f"{emoji[i]} **{averages[i][0]}:** {int(averages[i][1])}\n"
        embed.add_field(name=f"{self.bot.get_emoji(874013315901317140)} __Average damage taken:__", value=val, inline=False)

        # Downs
        downs, averages = await get_player_stats(Player.downs, limit)
        val = ""
        for i in range(0, 5):
            val += f"{emoji[i]} **{downs[i][0]}:** {downs[i][1]}\n"
        embed.add_field(name=f"{self.bot.get_emoji(874013566452252732)} __Total downs:__", value=val, inline=False)

        val = ""
        for i in range(0, 5):
            val += f"{emoji[i]} **{averages[i][0]}:** {round(averages[i][1], 2)}\n"
        embed.add_field(name=f"{self.bot.get_emoji(874013566452252732)} __Average downs:__", value=val, inline=False)

        # Deaths
        deaths, averages = await get_player_stats(Player.deaths, limit)
        val = ""
        for i in range(0, 5):
            val += f"{emoji[i]} **{deaths[i][0]}:** {deaths[i][1]}\n"
        embed.add_field(name=f"{self.bot.get_emoji(874013695154466816)} __Total deaths:__", value=val, inline=False)

        val = ""
        for i in range(0, 5):
            val += f"{emoji[i]} **{averages[i][0]}:** {round(averages[i][1], 2)}\n"
        embed.add_field(name=f"{self.bot.get_emoji(874013695154466816)} __Average deaths:__", value=val, inline=False)

        await ctx.send(embed=embed)


def setup(bot):
    bot.add_cog(LogManager(bot))
