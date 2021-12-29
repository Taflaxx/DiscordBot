from discord.ext import commands
from discord import Embed, File, TextChannel
import logging
import os
import csv
from cogs.logmanager.utils import *
from cogs.logmanager.db import *
from sqlalchemy import func, column
import pandas as pd
import difflib

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
                # Make sure embed is added when < 5 logs are left
                elif i + 1 == query.count():
                    embed.add_field(name=f"Sorted by {order}:", value=val)
                # For better formatting (max 2 fields next to each other) add an invisible field
                if (i + 1) % 10 == 0:
                    embed.add_field(name="\u200b", value="\u200b")
            await ctx.send(embed=embed)

    @log.command(name="history", help="Search a Discord channel for logs", usage="<channel> [message_limit]")
    @commands.is_owner()
    async def parse_channel(self, ctx, channel: TextChannel, limit: int = 100):
        messages = await channel.history(limit=limit).flatten()
        log_counter = 0
        errors = 0  # Tracks the number of errors while adding logs
        for idx, message in enumerate(messages):
            # Find all links to logs in the message
            logs = re.findall("https:\/\/dps\.report\/[a-zA-Z\-0-9\_]+", message.content)

            for log in logs:
                log_counter += 1
                r = await add_log(log)
                if r is not None:
                    print(r)
                    errors += 1
            db.commit()
            print(f"Messages parsed: {idx + 1}/{len(messages)}\n"
                  f"Logs parsed {log_counter - errors}/{log_counter} ({errors} errors)")
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
        query = query.filter(Log.fight_name.ilike(f"%{boss}") | Log.fight_name.ilike(f"%{boss} cm"))

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
            val += f"[{t.strftime('%Mm %Ss %f')[:-3]}ms ({query_fastest[i].date_time.strftime('%B %e, %Y')})]({query_fastest[i].link})\n"
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

        # Creating the fight duration plot
        # Query DB into a Pandas dataframe
        df = pd.read_sql(db.query(Log.date_time, Log.duration).filter((Log.fight_name.ilike(f"%{boss}") | Log.fight_name.ilike(f"%{boss} cm")))
                         .order_by(Log.date_time).statement, db.bind)
        # Convert datetime.time to int seconds
        for i in df.index:
            df.at[i, "duration"] = (df.at[i, "duration"].hour * 60 + df.at[i, "duration"].minute) * 60 + df.at[i, "duration"].second
        # Create line plot
        df.columns = ["Date", "Fight duration in seconds"]
        filepath, filename = plot_lineplot(df, boss)
        # Add file to embed and send it
        embed.set_image(url=f"attachment://{filename}")
        await ctx.send(embed=embed, file=File(filepath))
        # Remove file
        os.remove(filepath)

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

    @log.command(name="buff")
    async def buff(self, ctx, boss, *buffs):
        # If no buffs were specified fall back to default
        if not buffs:
            buffs = ["Might", "Quickness", "Alacrity"]
        if boss in boss_abrv:
            boss = boss_abrv[boss]
        data = []
        # Create embed
        embed = Embed(title=f"Buffs on {boss}", color=0x0099ff)
        embed.set_thumbnail(url="https://cdn.discordapp.com/emojis/818529609330851910.gif?v=1")

        # Create dataset
        for buff in buffs:
            # Check for valid Buff
            buff_map = db.query(BuffMap).filter(BuffMap.name.ilike(f"%{buff}%")).all()
            closest = []
            # If no Buff was found add en embed message and skip to next item
            if len(buff_map) == 0:
                embed.add_field(name="**Error**", value=f"Buff \"{buff}\" was not found.", inline=False)
                continue
            # If more than 1 match select the closest
            elif len(buff_map) > 1:
                # Create list of buff names
                buff_matches = []
                for b in buff_map:
                    buff_matches.append(b.name)
                # Calculate closest matching buff
                closest = difflib.get_close_matches(buff, buff_matches, 4, cutoff=0.1)
                # Return if no closest match could be determined
                if not closest:
                    embed.add_field(name="**Error**", value=f"Buff \"{buff}\" was not specific enough.", inline=False)
                    continue
                # Assign closest match
                buff_map = db.query(BuffMap).filter(BuffMap.name.ilike(closest[0])).all()
                closest.remove(closest[0])

            # Join Tables, filter by boss and buff, group by Log.link
            query = db.query(Log.date_time, func.avg(Buff.uptime), column(buff_map[0].name)).join(Player, Log.players).join(Buff, Player.buffs) \
                .filter((Log.fight_name.ilike(f"%{boss}") | Log.fight_name.ilike(f"%{boss} cm"))) \
                .filter(Buff.buff == buff_map[0].id).group_by(Log.link).all()
            if len(query) < 2:
                embed.add_field(name="**Error**", value=f"Not enough data for  \"{buff_map[0].name}\".", inline=False)
            else:
                # Convert query output to array for pandas
                for q in query:
                    # Multiply Might with 4 for better graphs since it only goes up to 25
                    if buff_map[0].name == "Might":
                        data.append([q[0], q[1] * 4, q[2]])
                    else:
                        data.append([q[0], q[1], q[2]])
                # Add buff description to embed
                description = buff_map[0].description
                # Suggest similarly spelled buffs in case the bot chose the wrong one
                if len(closest) > 0:
                    description += f"\nSimilar buffs: `"
                    for c in closest:
                        description += f"{c}, "
                    description = description.rstrip(", ")
                    description += "`"
                embed.add_field(name=f"**{buff_map[0].name}:**", value=description, inline=False)

        # Create dataframe from data
        df = pd.DataFrame(data, columns=["Date", "Uptime", "Boon"])
        # Update embed if there was only 1 valid buff selected
        if len(df["Boon"].unique()) == 1:
            buff_map = db.query(BuffMap).filter(BuffMap.name.ilike(df["Boon"][0])).all()
            embed.set_thumbnail(url=buff_map[0].icon)
            embed.title = f"{buff_map[0].name} on {boss}"
        # Check if dataframe actually contains any data
        if df.empty:
            await ctx.send(embed=embed)
        else:
            # Create line plot and add it to embed
            filepath, filename = plot_lineplot(df, boss, "Boon", True)
            embed.set_image(url=f"attachment://{filename}")
            # Suggest other close matches
            await ctx.send(embed=embed, file=File(filepath))
            # Remove file
            os.remove(filepath)

    @log.command(name="mech", help="Show mechanic stats", usage="<boss> [mechanic]")
    async def mech(self, ctx, boss, mechanic=None):
        if boss in boss_abrv:
            boss = boss_abrv[boss]

        # Check if boss exists in db
        boss = db.query(Log.fight_name).filter((Log.fight_name.ilike(f"%{boss}") | Log.fight_name.ilike(f"%{boss} cm"))).first()
        if not boss:
            await ctx.send_help("log mech")
            return
        else:
            boss = boss[0]

        embed = Embed(title=f"Mechanics on {boss}", color=0x0099ff)
        if mechanic:
            # List of all mechs on the boss
            mech_query = db.query(Mechanic.description).join(Player, Log.players).join(Mechanic, Player.mechanics).distinct(Mechanic.description)\
                .filter((Log.fight_name.ilike(f"%{boss}") | Log.fight_name.ilike(f"%{boss} cm")))\
                .filter(Mechanic.description.ilike(f"%{mechanic}%")).all()

            # Query 3 first matches
            for mech in mech_query[:3]:
                # Total amount of mechanic triggers for each player
                player_query = db.query(Log.fight_name, Player.account, Mechanic.description, func.sum(Mechanic.amount)).join(Player, Log.players).join(Mechanic, Player.mechanics)\
                    .filter((Log.fight_name.ilike(f"%{boss}") | Log.fight_name.ilike(f"%{boss} cm")))\
                    .filter(Mechanic.description.ilike(f"{mech[0]}")).group_by(Player.account)\
                    .order_by(func.sum(Mechanic.amount).desc()).limit(5).all()

                # Total amount of mechanic triggers
                total_query = db.query(Log.fight_name, Mechanic.description, func.sum(Mechanic.amount)).join(Player, Log.players).join(Mechanic, Player.mechanics)\
                    .filter((Log.fight_name.ilike(f"%{boss}") | Log.fight_name.ilike(f"%{boss} cm")))\
                    .filter(Mechanic.description.ilike(f"{mech[0]}")).all()

                description = f"**Total:** {total_query[0][2]}"
                for player in player_query:
                    description += f"\n{player[1]}: {player[3]}"
                embed.add_field(name=f"__{mech[0]}:__", value=description, inline=False)
            await ctx.send(embed=embed)

        # If no mechanic was specified
        else:
            # List of all mechs on the boss
            mech_query = db.query(Mechanic.description).join(Player, Log.players).join(Mechanic, Player.mechanics).distinct(Mechanic.description)\
                .filter((Log.fight_name.ilike(f"%{boss}") | Log.fight_name.ilike(f"%{boss} cm"))).all()

            # Number of logs of the specified boss
            fight_number = db.query(Log.fight_name).filter((Log.fight_name.ilike(f"%{boss}") | Log.fight_name.ilike(f"%{boss} cm"))).count()

            for mech in mech_query:
                # Total amount of mechanic triggers
                total_query = db.query(Log.fight_name, Mechanic.description, func.sum(Mechanic.amount))\
                    .join(Player, Log.players).join(Mechanic, Player.mechanics)\
                    .filter((Log.fight_name.ilike(f"%{boss}") | Log.fight_name.ilike(f"%{boss} cm")))\
                    .filter(Mechanic.description.ilike(f"{mech[0]}")).all()
                embed.add_field(name=f"__{mech[0]}:__", value=f"Total: {total_query[0][2]}\n Average: {round(total_query[0][2]/fight_number, 2)}", inline=False)
            await ctx.send(embed=embed)


def setup(bot):
    bot.add_cog(LogManager(bot))
