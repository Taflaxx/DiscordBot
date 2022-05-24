import configparser
from discord.ext import commands
from discord import Embed, File, TextChannel, app_commands, Interaction
import logging
import os
import csv
from cogs.logmanager.utils import *
from cogs.logmanager.db import *
from sqlalchemy import func, column
import pandas as pd
import difflib
from datetime import datetime, timezone, timedelta
from cogs.logmanager.views.filter import LogFilterView
import cogs.logmanager.choices as choices
import typing

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
        # Get messages
        messages = await channel.history(limit=limit).flatten()

        # Send confirmation message
        await ctx.send(f"Found {len(messages)} messages")

        log_counter = 0     # Tracks the number of logs in the messages
        errors = 0          # Tracks the number of errors while adding logs
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

    @commands.hybrid_command(name="weekly", help="Add weekly clear logs from the configured channel")
    async def weekly(self, ctx):
        # Get configured channel
        config = configparser.ConfigParser()
        config.read("config.ini")
        channel = None
        if config.has_section("LogManager"):
            if config.has_option("LogManager", "WeeklyChannel"):
                channel = self.bot.get_channel(int(config["LogManager"]["WeeklyChannel"]))

        # Return if channel has not been configured
        if not channel:
            await ctx.send("Please configure the weekly clear channel before using this command: \n"
                           f"{config['Bot']['prefix']}log config weekly <channel>")
            return

        # Load latest message
        message = await anext(channel.history(limit=1))

        # Find all links to logs in the message
        logs = re.findall("https:\/\/dps\.report\/[a-zA-Z\-0-9\_]+", message.content)
        message = await ctx.send(f"Found {len(logs)} logs:")

        errors = 0  # Tracks the number of errors while adding logs
        added_logs = []

        for log in logs:
            r = await add_log(log)
            if r is not None:
                errors += 1
                message = await message.edit(content=f"{message.content}\n{r}")  # update original message with errors
            else:
                added_logs.append(log)
        db.commit()
        await message.edit(content=f"{message.content}\nAdded {len(logs) - errors}/{len(logs)} logs to the database.")

        # Skip looking for records if no logs were added
        if len(added_logs) == 0:
            return

        # Check for new records
        records = ""
        records_dps = ""
        for log in added_logs:
            # Get log from db
            log_db = db.query(Log).filter(Log.link.ilike(log)).first()

            # Get logs from boss
            boss = log_db.fight_name
            query = db.query(Log)
            query = query.filter(Log.fight_name.ilike(boss) | Log.fight_name.ilike(f"{boss} cm"))

            # Check if boss kill is a new record
            query_fastest = query.distinct(Log.link).order_by(Log.duration.asc()).limit(3).all()
            if len(query_fastest) > 1:
                for i in range(0, len(query_fastest)):
                    if query_fastest[i].link == log:
                        if i == 0:
                            # Different text and emoji for first place
                            records += f"{rank_emojis[i+1]} **{log_db.fight_name}:** {log_db.duration.strftime('%Mm %Ss %f')[:-3]}ms " \
                                       f"(Old Record: {query_fastest[1].duration.strftime('%Mm %Ss %f')[:-3]}ms)\n"
                        else:
                            records += f"{rank_emojis[i+1]} **{log_db.fight_name}:** {log_db.duration.strftime('%Mm %Ss %f')[:-3]}ms " \
                                       f"(Record: {query_fastest[0].duration.strftime('%Mm %Ss %f')[:-3]}ms)\n"
                        break

            # Check for new DPS records
            log_db = db.query(Player).join(Log).filter(Log.link.ilike(log)).order_by(Player.dps.desc()).all()
            # Get highest DPS players
            query = db.query(Player).join(Log)
            query = query.filter(Log.fight_name.ilike(boss) | Log.fight_name.ilike(f"{boss} cm"))
            query_dps = query.order_by(Player.dps.desc()).limit(3).all()
            if len(query_fastest) > 1:
                for player in log_db:
                    for i in range(0, len(query_dps)):
                        if query_dps[i] == player:
                            records_dps += f"{rank_emojis[i+1]} **{boss}:** {player.dps} DPS by " \
                                           f"{player.character} - {player.profession}\n"
                            break

        # Check for stolen "Fiery Greatswords"
        fgs_stolen = {}
        for log in added_logs:
            # Get log from db
            fgs_logs = db.query(Buff).join(Player).join(Log).filter(Log.link.ilike(log)).filter(Buff.buff.ilike(15792)).all()
            for player in fgs_logs:
                # If player has buff and it not an elementalist: fgs was stolen
                if player.player.profession not in ["Elementalist", "Tempest", "Weaver", "Catalyst"]:
                    if player.player.account in fgs_stolen:
                        fgs_stolen[player.player.account] += 1
                    else:
                        fgs_stolen[player.player.account] = 1

        fgs_text = ""
        for account, value in fgs_stolen.items():
            fgs_text += f"<a:yoink:968193234931175424> {account}: {value}\n"

        # Create embed
        embed = Embed(title="Weekly Clear Stats", color=0x0099ff)

        # Calculate clear time
        first_kill = db.query(Log).filter(Log.link.ilike(added_logs[0])).first()
        last_kill = db.query(Log).filter(Log.link.ilike(added_logs[0])).first()
        for log in added_logs:
            log_db = db.query(Log).filter(Log.link.ilike(log)).first()
            if log_db.date_time < first_kill.date_time:
                first_kill = log_db
            elif log_db.date_time > last_kill.date_time:
                last_kill = log_db

        last_kill_duration = timedelta(minutes=last_kill.duration.minute, seconds=last_kill.duration.second,
                                       microseconds=last_kill.duration.microsecond)
        end_time = last_kill.date_time + last_kill_duration
        # Format clear_time since you cant use str format on a timedelta object
        clear_time = end_time - first_kill.date_time
        clear_time_hours, remainder = divmod(clear_time.total_seconds(), 3600)
        clear_time_minutes, clear_time_seconds = divmod(remainder, 60)

        embed.add_field(name=f"Clear Time:",
                        value=f"{int(clear_time_hours)}h {int(clear_time_minutes)}m {int(clear_time_seconds)}s")

        # Add kill time records
        if records == "":
            records = "No new records <:Sadge:780108805144838145>"
        embed = split_embed(embed, ":trophy: **Kill Time records in this weekly clear:**", records)

        # Add dps records
        if records_dps == "":
            records_dps = "No new records <:Sadge:780108805144838145>"
        embed = split_embed(embed, ":trophy: **DPS records in this weekly clear:**", records_dps)

        if fgs_stolen != "":
            embed = split_embed(embed, "<:fgs:968188503424897104> **Fiery Greatswords stolen:**", fgs_text)

        await ctx.send(embed=embed)

    @commands.is_owner()
    @log.group(name="config", help="Configure the logs cog")
    async def config(self, ctx):
        if ctx.invoked_subcommand is None:
            await ctx.send_help("log config")
            print(f"Unknown subcommand \"{ctx.message.content}\" by {ctx.author}. Sent help page")

    @config.command(name="weekly")
    async def config_weekly(self, ctx, channel: TextChannel):
        # Set configured channel
        config = configparser.ConfigParser()
        config.read("config.ini")
        if not config.has_section("LogManager"):
            config.add_section("LogManager")
        config.set("LogManager", "WeeklyChannel", str(channel.id))

        with open("config.ini", 'w') as configfile:
            config.write(configfile)

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

    @app_commands.command(name="boss", description="Show boss specific stats")
    async def stats_boss(self, interaction: Interaction,  boss: choices.bosses) -> None:
        query = db.query(Log).join(Player)
        query = query.filter(Log.fight_name.ilike(f"%{boss}") | Log.fight_name.ilike(f"%{boss} cm"))

        if query.count() == 0:
            await interaction.response.send_message("**:x: No logs found**")
            return

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
        total_logs = db.query(Log.link).filter(Log.fight_name.ilike(f"%{boss}") | Log.fight_name.ilike(f"%{boss} cm")).count()
        total_players = db.query(Player.id).join(Log).filter(Log.fight_name.ilike(f"%{boss}") | Log.fight_name.ilike(f"%{boss} cm")).count()
        total_dps = db.query(func.sum(Player.dps)).join(Log).filter(Log.fight_name.ilike(f"%{boss}") | Log.fight_name.ilike(f"%{boss} cm")).all()[0][0]
        embed.add_field(name="Average DPS:", value=f"Group: {round(total_dps / total_logs)}\nPlayer: {round(total_dps / total_players)}")

        # Average Damage
        total_damage = db.query(func.sum(Player.damage)).join(Log).filter(Log.fight_name.ilike(f"%{boss}") | Log.fight_name.ilike(f"%{boss} cm")).all()[0][0]
        embed.add_field(name="Average damage:", value=f"Group: {round(total_damage / total_logs)}\nPlayer: {round(total_damage / total_players)}")

        # Add top DPS
        top_dps = db.query(Player.character, Player.account, Player.dps, Player.profession, Log.link).join(Log)\
            .filter(Log.fight_name.ilike(f"%{boss}") | Log.fight_name.ilike(f"%{boss} cm"))\
            .order_by(Player.dps.desc()).all()
        top_dps_str = ""
        for i in range(0, 3):
            top_dps_str += f"[{top_dps[i][2]} DPS by {top_dps[i][0]} ({top_dps[i][1]}) - {top_dps[i][3]}]({top_dps[i][4]})\n"
        embed.add_field(name="Top DPS:", value=top_dps_str, inline=False)

        # Downs
        total_downs = db.query(func.sum(Player.downs)).join(Log).filter(Log.fight_name.ilike(f"%{boss}") | Log.fight_name.ilike(f"%{boss} cm")).all()[0][0]
        embed.add_field(name="Downs:", value=f"Total: {total_downs}\nPer fight: {round(total_downs / total_logs, 1)}")

        # Deaths
        total_deaths = db.query(func.sum(Player.deaths)).join(Log).filter(Log.fight_name.ilike(f"%{boss}") | Log.fight_name.ilike(f"%{boss} cm")).all()[0][0]
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
        await interaction.response.send_message(embed=embed, file=File(filepath))
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

    @app_commands.command(name="buffs", description="Show stats about specific buffs at a boss")
    async def buffs(self, interaction: Interaction, boss: choices.bosses, buffs: typing.Optional[str]) -> None:
        # If no buffs were specified fall back to default
        if not buffs:
            buffs = ["Might", "Quickness", "Alacrity"]
        else:
            # Allow comma seperated buffs
            buffs = re.split(",\s+|,", buffs)

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
            await interaction.response.send_message(embed=embed)
        else:
            # Create line plot and add it to embed
            filepath, filename = plot_lineplot(df, boss, "Boon", True)
            embed.set_image(url=f"attachment://{filename}")
            # Suggest other close matches
            await interaction.response.send_message(embed=embed, file=File(filepath))
            # Remove file
            os.remove(filepath)

    @app_commands.command(name="mechs", description="Show mechanic stats")
    async def mechs(self, interaction: Interaction, boss: choices.bosses, mechanics: typing.Optional[str]) -> None:
        # Check if boss exists in db
        boss_db = db.query(Log.fight_name).filter((Log.fight_name.ilike(f"%{boss}") | Log.fight_name.ilike(f"%{boss} cm"))).first()
        if not boss_db:
            await interaction.response.send_message("**:x: No logs found**")
            return

        embed = Embed(title=f"Mechanics on {boss}", color=0x0099ff)

        # If no mechanic was specified
        if not mechanics:
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
            await interaction.response.send_message(embed=embed)

        else:
            # Allow comma seperated mechs
            mechanics = re.split(",\s+|,", mechanics)

            # Create dataset
            data = []
            for mechanic in mechanics:
                # Check for valid Mechanic
                mechanic_map = db.query(Mechanic) \
                    .join(Player, Log.players) \
                    .join(Mechanic, Player.mechanics) \
                    .filter((Log.fight_name.ilike(f"%{boss}") | Log.fight_name.ilike(f"%{boss} cm"))) \
                    .filter(Mechanic.description.ilike(f"%{mechanic}%")).all()
                closest = []
                # If no Mechanic was found add en embed message and skip to next item
                if len(mechanic_map) == 0:
                    embed.add_field(name="**Error**", value=f"Mechanic \"{mechanic}\" on boss \"{boss}\" was not found .", inline=False)
                    continue
                # If more than 1 match select the closest
                elif len(mechanic_map) > 1:
                    # Create list of buff names
                    mechanic_matches = []
                    for b in mechanic_map:
                        mechanic_matches.append(b.description)
                    # Calculate closest matching mechanic
                    closest = difflib.get_close_matches(mechanic, mechanic_matches, 4, cutoff=0.1)
                    # Return if no closest match could be determined
                    if not closest:
                        embed.add_field(name="**Error**", value=f"Mechanic \"{mechanic}\" was not specific enough.",
                                        inline=False)
                        continue
                    # Assign closest match
                    mechanic_map = db.query(Mechanic).filter(Mechanic.description.ilike(closest[0])).all()
                    closest.remove(closest[0])

                # Join Tables, filter by boss and mechanic, group by Log.link
                query = db.query(Log.date_time, func.sum(Mechanic.amount), column(mechanic_map[0].description))\
                    .join(Player,Log.players)\
                    .join(Mechanic, Player.mechanics) \
                    .filter((Log.fight_name.ilike(f"%{boss}") | Log.fight_name.ilike(f"%{boss} cm"))) \
                    .filter(Mechanic.description == mechanic_map[0].description).group_by(Log.link).all()
                if len(query) < 2:
                    embed.add_field(name="**Error**", value=f"Not enough data for  \"{mechanic_map[0].name}\".",
                                    inline=False)
                else:
                    # Convert query output to array for pandas
                    log_list = []
                    for q in query:
                        data.append([q[0], q[1], q[2]])
                        log_list.append(q[0])
                    # Add buff description to embed
                    description = mechanic_map[0].name
                    # Get all logs where mechanic was not triggered and add them with the amount 0 to the data
                    log_query = db.query(Log.date_time).filter((Log.fight_name.ilike(f"%{boss}") | Log.fight_name.ilike(f"%{boss} cm"))).all()
                    for log in log_query:
                        if log[0] not in log_list:
                            data.append([log[0], 0, mechanic_map[0].description])
                    embed.add_field(name=f"**{mechanic_map[0].description}:**", value=description, inline=False)

            # Create dataframe from data
            df = pd.DataFrame(data, columns=["Date", "Amount", "Mechanic"])
            # Update embed if there was only 1 valid buff selected
            if len(df["Mechanic"].unique()) == 1:
                mechanic_map = db.query(Mechanic).filter(Mechanic.description.ilike(df["Mechanic"][0])).all()
                embed.title = f"{mechanic_map[0].description} on {boss}"
            # Check if dataframe actually contains any data
            if df.empty:
                await interaction.response.send_message(embed=embed)
            else:
                # Create line plot and add it to embed
                filepath, filename = plot_lineplot(df, boss, "Mechanic", False)
                embed.set_image(url=f"attachment://{filename}")
                # Suggest other close matches
                await interaction.response.send_message(embed=embed, file=File(filepath))
                # Remove file
                os.remove(filepath)

    @app_commands.command(name="logs", description="Search for logs")
    async def search_logs(self, interaction: Interaction) -> None:
        view = LogFilterView()

        await interaction.response.send_message(view=view)
        view.message = await interaction.original_message()


async def setup(bot):
    await bot.add_cog(LogManager(bot))
