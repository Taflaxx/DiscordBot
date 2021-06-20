from discord.ext import commands
from discord import Embed, File, TextChannel
import logging
import os
import csv
from cogs.logmanager.utils import *
from cogs.logmanager.db import *

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
                       "-order <dps|dmg>\tSet stat to order by\n"
                       "-asc\tAscending oder\n"
                       "-desc\tDescending order\n"
                       "-csv\t Export query result as a csv file")
    async def filter_log(self, ctx, *args):
        if "-h" in args or "-help" in args:
            await ctx.send_help("log filter")
            return
        result = db.query(Player).join(Log)

        # Parsing arguments
        export_csv = False
        order = "dps"
        for i, arg in enumerate(args):
            if arg == "-a" or arg == "--account":
                result = result.filter(Player.account.ilike(f"%{args[i + 1]}%"))
            elif arg == "-c" or arg == "--character":
                result = result.filter(Player.character.ilike(f"%{args[i + 1]}%"))
            elif arg == "-p" or arg == "--profession":
                result = result.filter(Player.profession.ilike(f"%{args[i + 1]}%"))
            elif arg == "-b" or arg == "--boss":
                if args[i + 1].lower() in boss_abrv:
                    boss = boss_abrv[args[i + 1]]
                else:
                    boss = args[i + 1]
                # Prevent "Qadim the Peerless" logs from showing up when searching for qadim
                if boss.lower() == "qadim":
                    result = result.filter(Log.fight_name.ilike(boss))
                else:
                    result = result.filter(Log.fight_name.ilike(f"%{boss}%"))  # case insensitive LIKE
            elif arg == "-csv":
                export_csv = True
            elif arg == "-cm":
                result = result.filter(Log.fight_name.ilike("% CM"))
            elif arg == "-nm":
                result = result.filter(Log.fight_name.notilike("% CM"))
            elif arg == "-order":
                order = args[i + 1]

        # Order By
        # TODO: Cleaner implementation
        if order == "dmg" or order == "damage":
            if "-desc" in args:
                result = result.order_by(Player.damage.desc())
            else:
                result = result.order_by(Player.damage.asc())
        else:
            if "-asc" in args:
                result = result.order_by(Player.dps.asc())
            else:
                result = result.order_by(Player.dps.desc())

        if result.count() == 0:
            await ctx.send("**:x: No logs found**")
            return

        if export_csv:
            # Create csv, send it and delete it afterwards
            filename = f"cogs/logmanager/tmp/{datetime.now(tz=timezone.utc).strftime('export-%Y%m%d-%H%M%S')}.csv"
            with open(filename, mode="w", newline="") as file:
                csv_writer = csv.writer(file, delimiter=',')
                csv_writer.writerow(["link", "boss", "account", "character", "profession", "dps", "damage"])
                for row in result:
                    csv_writer.writerow([row.log.link, row.log.fight_name, row.account, row.character,
                                         row.profession, row.dps, row.damage])
            await ctx.send(file=File(filename))
            os.remove(filename)

        else:
            # Create Embed
            # Limited to top 10 logs
            embed = Embed(title="Top Logs", color=0x0099ff)
            val = ""
            for i, row in enumerate(result[:5]):
                val += f"[{i + 1}. {row.log.fight_name}:]({row.log.link})\n{row.character} - {row.profession}\n" \
                       f"DPS: {row.dps}\nDamage taken: {row.damage}\n\n"
            embed.add_field(name=f"Sorted by {order} [1-5]:", value=val)

            val = ""
            if result[5:10]:
                for i, row in enumerate(result[5:10]):
                    val += f"[{i + 6}. {row.log.fight_name}:]({row.log.link})\n{row.character} - {row.profession}\n" \
                           f"DPS: {row.dps}\nDamage taken: {row.damage}\n\n"
                embed.add_field(name=f"Sorted by {order} [6-10]:", value=val)

            embed.add_field(name="\u200B",
                            value="If you find any bugs or your dps seems low you can submit a bugreport "
                                  "[here](https://www.youtube.com/watch?v=d1YBv2mWll0)", inline=False)
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
                    errors += 1
            db.commit()
        await ctx.send(f"Added {log_counter - errors}/{log_counter} logs to the database.")

    @log.command(name="stats", help="Show some general stats about the logs")
    async def stats(self, ctx):
        embed = Embed(title="Log Stats", color=0x0099ff)
        embed.add_field(name="Logs:", value=db.query(Log).count())
        embed.add_field(name="Distinct Accounts:", value=db.query(Player.account).distinct().count())
        embed.add_field(name="Distinct Characters:", value=db.query(Player.character).distinct().count())

        embed.add_field(name="Frequent accounts:", value=most_frequent_embed(db.query(Player.account).all()))
        embed.add_field(name="Frequent characters:", value=most_frequent_embed(db.query(Player.character).all()))
        embed.add_field(name="Frequent professions:", value=most_frequent_embed(db.query(Player.profession).all()))

        embed.add_field(name="Average group dps:", value=str(round(db.query(func.sum(Player.dps)).all()[0][0] /
                                                                   db.query(Log.link).distinct().count())))
        embed.add_field(name="Average group damage:", value=str(round(db.query(func.sum(Player.damage)).all()[0][0] /
                                                                      db.query(Log.link).distinct().count())))

        await ctx.send(embed=embed)


def setup(bot):
    bot.add_cog(LogManager(bot))
