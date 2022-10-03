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
from datetime import datetime, timezone
from cogs.logmanager.views.filter import LogFilterView, create_log_embed
from cogs.logmanager.views.confirmation import ConfirmationView
import cogs.logmanager.choices as choices
from cogs.logmanager.dicts import bosses
import typing
import traceback
import sys
from sqlalchemy import select, update

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

    @commands.guild_only()
    @commands.group(name="log", aliases=["l"], help="For all your logging needs")
    async def log(self, ctx):
        if ctx.invoked_subcommand is None:
            await ctx.send_help("log")
            print(f"Unknown subcommand \"{ctx.message.content}\" by {ctx.author}. Sent help page")

    @commands.guild_only()
    @log.command(name="add", help="Add logs to the database", usage="[log(s)]")
    async def add_logs(self, ctx: commands.Context, *, arg):
        # Find all links to logs in the message
        logs = re.findall("https:\/\/dps\.report\/[a-zA-Z\-0-9\_]+", arg)
        message = await ctx.send(f"Found {len(logs)} logs:")

        errors = 0  # Tracks the number of errors while adding logs
        for log in logs:
            r = await add_log(log, ctx.guild.id)
            if r is not None:
                errors += 1
                await message.edit(content=f"{message.content}\n{r}")  # update original message with errors
        db.commit()
        await message.edit(content=f"{message.content}\nAdded {len(logs) - errors}/{len(logs)} logs to the database.")

    @commands.guild_only()
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
    async def filter_log(self, ctx: commands.Context, *args):
        if "-h" in args or "-help" in args:
            await ctx.send_help("log filter")
            return
        query = db.query(Player).join(Log).filter(Log.guild_id == ctx.guild.id)
        query = await filter_args(query, args)
        query, order, limit = await order_args(query, args)

        if query.count() == 0:
            await ctx.send("**:x: No logs found**", ephemeral=True)
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

            embed = create_log_embed(query, order, 0, min(limit, query.count(), 30))
            await ctx.send(embed=embed, content=":exclamation:**Please use the new `/logs` command. "
                                                "This command might get removed soon.**")

    @app_commands.guild_only
    @app_commands.checks.cooldown(1, 600, key=lambda i: i.guild_id)
    @app_commands.checks.bot_has_permissions(send_messages=True) # Need send_messages perm here to update progress
    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.command(name="history", description="Search a Discord channel for logs")
    async def parse_channel(self, interaction: Interaction, channel: TextChannel, limit: typing.Optional[int] = None):
        # Check if bot can view the given channel
        if not channel.permissions_for(channel.guild.me).read_messages:
            await interaction.response.send_message(content="I don't have permissions to view that channel", ephemeral=True)
            return

        # Get messages
        messages = channel.history(limit=limit)

        # Send confirmation message
        response = "**Finding logs:** "
        await interaction.response.send_message(content=response)
        # Get full message instead of interaction message to prevent webhook timeout
        response_message = await interaction.original_response()
        response_message = await response_message.fetch()

        logs = []
        async for message in messages:
            # Find all links to logs in the message
            logs.extend(re.findall("https:\/\/dps\.report\/[a-zA-Z\-0-9\_]+", message.content))

        # Send confirmation message
        response += f"{len(logs)} logs found."
        await response_message.edit(content=response + f"\nParsed 0/{len(logs)} logs.")

        # Add logs
        errors = 0  # Tracks the number of errors while adding logs
        for idx, log in enumerate(logs):
            # "Ignore" errors here as very old logs with weird bugs/changes might get added that can just be ignored
            try:
                r = await add_log(log, interaction.guild_id)
            except Exception as error:
                r = error
                traceback.print_exception(type(error), error, error.__traceback__, file=sys.stderr)

            if r is not None:
                print(r)
                errors += 1

            # Periodically update user on progress
            if (idx+1) % 10 == 0:
                await response_message.edit(content=f"{response}\nParsed {idx+1}/{len(logs)} logs.")
                db.commit()

        await response_message.edit(content=f"{response}\nParsed {len(logs)}/{len(logs)} logs.\n"
                                            f"**Added {len(logs) - errors}/{len(logs)} logs to the database.**")

    @parse_channel.error
    async def on_parse_channel_error(self, interaction: Interaction, error: app_commands.AppCommandError):
        if isinstance(error, app_commands.CommandOnCooldown):
            await interaction.response.send_message(str(error), ephemeral=True)
        elif isinstance(error, app_commands.MissingPermissions):
            await interaction.response.send_message(content="Only Administrators of the server can use this command.",
                                                    ephemeral=True)
        elif isinstance(error, app_commands.BotMissingPermissions):
            await interaction.response.send_message(content=f"I don't have permissions to send messages in "
                                                            f"{interaction.channel.mention}\n"
                                                            f"Missing Permissions: {error.missing_permissions}",
                                                    ephemeral=True)
        else:
            # Print Traceback in case of different errors
            traceback.print_exception(type(error), error, error.__traceback__, file=sys.stderr)

    @app_commands.guild_only
    @app_commands.command(name="weekly", description="Add weekly clear logs from the configured channel")
    async def weekly(self, interaction: Interaction):
        # Get configured channel
        channel = (await db.execute(select(Config.log_channel_id).filter(Config.guild_id == interaction.guild_id))).scalar()
        channel = self.bot.get_channel(channel)
        # Return if channel has not been configured
        if not channel:
            await interaction.response.send_message(content="Please configure the log channel before using this command\n"
                                                            "Admins can use `/config weekly` to set it",
                                                    ephemeral=True)
            return

        # Load latest message
        message = await anext(channel.history(limit=1))

        # Find all links to logs in the message
        logs = re.findall("https:\/\/dps\.report\/[a-zA-Z\-0-9\_]+", message.content)
        response = f"**Found {len(logs)} logs:**"
        await interaction.response.send_message(content=response)

        errors = 0  # Tracks the number of errors while adding logs
        error_str = ""
        added_logs = []

        for idx, log in enumerate(logs):
            r = await add_log(log, interaction.guild_id)
            if r is not None:
                errors += 1
                error_str += f"\n{r}"
                if "Already in Database" in r:
                    added_logs.append(log)
            else:
                added_logs.append(log)
            await interaction.edit_original_response(content=f"{response}\nParsed {idx + 1}/{len(logs)} logs.")
        await db.commit()
        response += f"\nParsed {len(logs)}/{len(logs)} logs."
        response += f"\n**Added {len(logs) - errors}/{len(logs)} logs to the database.**{error_str}"
        await interaction.edit_original_response(content=response)

        # Skip looking for records if no logs were added
        if len(added_logs) == 0:
            return

        # Check for new records
        records = ""
        records_dps = ""
        for log in added_logs:
            # Get log from db
            log_db = (await db.execute(select(Log).filter(Log.link == log))).scalar()

            # Ignore emboldened records
            if log_db.emboldened > 0:
                continue

            # Get top logs from boss
            boss = log_db.fight_name
            statement = select(Log).filter(Log.guild_id == interaction.guild_id)\
                .filter(Log.fight_name.ilike(boss) | Log.fight_name.ilike(f"{boss} cm"))\
                .order_by(Log.duration.asc()).limit(3)
            query_fastest = (await db.execute(statement)).scalars().all()

            # Check if boss kill is a new record
            if len(query_fastest) > 1:
                for i in range(0, len(query_fastest)):
                    if query_fastest[i].link == log:
                        if i == 0:
                            # Different text and emoji for first place
                            records += f"{rank_emojis[i+1]} **{log_db.fight_name}:** {strfdelta(log_db.duration)} " \
                                       f"(Old Record: {strfdelta(query_fastest[1].duration)})\n"
                        else:
                            records += f"{rank_emojis[i+1]} **{log_db.fight_name}:** {strfdelta(log_db.duration)} " \
                                       f"(Record: {strfdelta(query_fastest[0].duration)})\n"
                        break

            # Check for new DPS records
            log_db = (await db.execute(select(Player).join(Log).filter(Log.link.ilike(log)).order_by(Player.dps.desc()))).scalars().all()
            # Get highest DPS players
            statement = select(Player).join(Log).filter(Log.guild_id == interaction.guild_id)\
                .filter(Log.fight_name.ilike(boss) | Log.fight_name.ilike(f"{boss} cm"))\
                .order_by(Player.dps.desc()).limit(3)
            query_dps = (await db.execute(statement)).scalars().all()
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
            statement = select(BuffUptimes).join(Player).join(Log).filter(Log.link.ilike(log)).filter(BuffUptimes.buff.ilike(15792))
            fgs_logs = (await db.execute(statement)).scalars().all()
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
        statement = select(Log.date_time, Log.duration).filter(Log.link.in_(added_logs)).order_by(Log.date_time.asc())
        log_times = (await db.execute(statement)).all()
        first_kill = log_times[0]
        last_kill = log_times[-1]

        end_time = last_kill.date_time + last_kill.duration
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

        await interaction.edit_original_response(embed=embed)

    config_group = app_commands.Group(name="config", description="Configure the bot")

    @app_commands.guild_only
    @app_commands.checks.has_permissions(administrator=True)
    @config_group.command(name="weekly", description="Set the channel where the clear logs are posted in")
    async def config_weekly(self, interaction: Interaction, channel: TextChannel) -> None:
        # Check if bot can view the given channel
        if not channel.permissions_for(channel.guild.me).read_messages:
            await interaction.response.send_message(content="I don't have permissions to view that channel", ephemeral=True)
            return

        # Add to DB
        config = (await db.execute(select(Config).filter(Config.guild_id == interaction.guild_id))).first()
        if config:
            await db.execute(update(Config).filter(Config.guild_id == interaction.guild_id).values(log_channel_id=channel.id))
        else:
            config = Config(guild_id=interaction.guild_id, log_channel_id=channel.id)
            db.add(config)
        await db.commit()
        await interaction.response.send_message(content=f"Set channel to {channel.mention}")

    @config_weekly.error
    async def on_parse_channel_error(self, interaction: Interaction, error: app_commands.AppCommandError):
        if isinstance(error, app_commands.MissingPermissions):
            await interaction.response.send_message(content="Only Administrators of the server can use this command.",
                                                    ephemeral=True)
        else:
            # Print Traceback in case of different errors
            traceback.print_exception(type(error), error, error.__traceback__, file=sys.stderr)

    @app_commands.guild_only
    @app_commands.command(name="stats", description="Show some general stats about the logs")
    async def stats_general(self, interaction: Interaction) -> None:
        # Defer to prevent interaction timeout
        await interaction.response.defer()

        # Create embed
        # TODO: add image, emojis...
        embed = Embed(title="Log Stats", color=0x0099ff)

        # Get distinct accounts, characters
        total_logs = (await db.execute(select(func.count(Log.link)).filter(Log.guild_id == interaction.guild_id))).scalar()
        embed.add_field(name="Logs:", value=total_logs)
        accounts, characters = (await db.execute(select(func.count(func.distinct(Player.account)),
                                                        func.count(func.distinct(Player.character)))
                                                 .filter(Player.guild_id == interaction.guild_id))).first()
        embed.add_field(name="Distinct Accounts:", value=accounts)
        embed.add_field(name="Distinct Characters:", value=characters)

        # Get most frequent accounts, characters, professions
        accounts = await db.execute(select(Player.account).filter(Player.guild_id == interaction.guild_id))
        characters = await db.execute(select(Player.character).filter(Player.guild_id == interaction.guild_id))
        professions = await db.execute(select(Player.profession).filter(Player.guild_id == interaction.guild_id))
        embed.add_field(name="Frequent accounts:", value=most_frequent_embed(accounts.all()))
        embed.add_field(name="Frequent characters:", value=most_frequent_embed(characters.all()))
        embed.add_field(name="Frequent professions:", value=most_frequent_embed(professions.all()))

        # Get Average DPS, Average damage, Downs, Deaths
        statement = select(func.count(Player.id),
                           func.sum(Player.dps),
                           func.sum(Player.damage),
                           func.sum(Player.downs),
                           func.sum(Player.deaths)).filter(Player.guild_id == interaction.guild_id)
        total_players, total_dps, total_damage, total_downs, total_deaths = (await db.execute(statement)).first()

        # Add embeds
        embed.add_field(name="Average DPS:", value=f"Group: {round(total_dps / total_logs)}\nPlayer: {round(total_dps / total_players)}")
        embed.add_field(name="Average damage:", value=f"Group: {round(total_damage / total_logs)}\nPlayer: {round(total_damage / total_players)}")
        embed.add_field(name="\u200b", value="\u200b")   # Add invisible field for better formatting
        embed.add_field(name="Downs:", value=f"Total: {total_downs}\nAverage: {round(total_downs / total_logs, 1)}")
        embed.add_field(name="Deaths:", value=f"Total: {total_deaths}\nAverage: {round(total_deaths / total_logs, 1)}")

        await interaction.followup.send(embed=embed)

    @app_commands.guild_only
    @app_commands.command(name="boss", description="Show boss specific stats")
    async def stats_boss(self, interaction: Interaction,  boss: choices.bosses) -> None:
        # Defer to prevent interaction timeout
        await interaction.response.defer()

        # Get all logs of the selected boss
        statement = select(Log).join(Player).filter(Log.guild_id == interaction.guild_id)\
            .filter(Log.fight_name.ilike(f"%{boss}") | Log.fight_name.ilike(f"%{boss} cm"))\
            .distinct(Log.link)
        total_logs = len((await db.execute(statement)).all())
        print(total_logs)
        if total_logs == 0:
            await interaction.followup.send("**:x: No logs found**", ephemeral=True)
            return

        # Create embed
        embed = Embed(title="Boss Stats", color=0x0099ff)
        embed.set_author(name=boss, icon_url=bosses[boss]["icon"])

        # First kill
        first_kill = (await db.execute(statement.order_by(Log.date_time.asc()))).scalar()
        embed.add_field(name="First kill:", value=f"[{first_kill.date_time.strftime('%B %e, %Y')}]({first_kill.link})")

        # Latest kill
        latest_kill = (await db.execute(statement.order_by(Log.date_time.desc()))).scalar()
        embed.add_field(name="Latest kill:", value=f"[{latest_kill.date_time.strftime('%B %e, %Y')}]({latest_kill.link})")

        # Total kills
        embed.add_field(name="Number of kills:", value=total_logs)

        # Fastest kills
        query_fastest = (await db.execute(statement.filter(Log.emboldened == 0).order_by(Log.duration.asc()))).scalars().all()
        val = ""
        for i in range(0, min(5, len(query_fastest))):
            val += f"[{strfdelta(query_fastest[i].duration)} ({query_fastest[i].date_time.strftime('%B %e, %Y')})]({query_fastest[i].link})\n"
        embed.add_field(name="Fastest kills:", value=val, inline=False)

        # Average DPS and damage taken
        statement = select(func.count(Player.id), func.sum(Player.dps), func.sum(Player.damage)).join(Log)\
            .filter(Log.guild_id == interaction.guild_id) \
            .filter(Log.fight_name.ilike(f"%{boss}") | Log.fight_name.ilike(f"%{boss} cm"))
        total_players, total_dps, total_damage = (await db.execute(statement)).first()

        # Add embeds
        embed.add_field(name="Average DPS:", value=f"Group: {round(total_dps / total_logs)}\nPlayer: {round(total_dps / total_players)}")
        embed.add_field(name="Average damage:", value=f"Group: {round(total_damage / total_logs)}\nPlayer: {round(total_damage / total_players)}")

        # Add top DPS
        top_dps = (await db.execute(
            select(Player.character, Player.account, Player.dps, Player.profession, Log.link).join(Log)
            .filter(Log.guild_id == interaction.guild_id)
            .filter(Log.emboldened == 0)
            .filter(Log.fight_name.ilike(f"%{boss}") | Log.fight_name.ilike(f"%{boss} cm"))
            .order_by(Player.dps.desc()))).all()
        top_dps_str = ""
        for i in range(0, 3):
            top_dps_str += f"[{top_dps[i][2]} DPS by {top_dps[i][0]} ({top_dps[i][1]}) - {top_dps[i][3]}]({top_dps[i][4]})\n"
        embed.add_field(name="Top DPS:", value=top_dps_str, inline=False)

        # Downs & Downs
        total_downs, total_deaths = (await db.execute(
            select(func.sum(Player.downs), func.sum(Player.deaths))
            .join(Log).filter(Log.guild_id == interaction.guild_id)
            .filter(Log.fight_name.ilike(f"%{boss}") | Log.fight_name.ilike(f"%{boss} cm")))).first()
        embed.add_field(name="Downs:", value=f"Total: {total_downs}\nAverage: {round(total_downs / total_logs, 1)}")
        embed.add_field(name="Deaths:", value=f"Total: {total_deaths}\nAverage: {round(total_deaths / total_logs, 1)}")

        # Creating the fight duration plot
        # Query DB into a Pandas dataframe
        r = await db.execute(select(Log.date_time, Log.duration).filter(Log.guild_id == interaction.guild_id)
                             .filter(Log.emboldened == 0)
                             .filter((Log.fight_name.ilike(f"%{boss}") | Log.fight_name.ilike(f"%{boss} cm")))
                             .order_by(Log.date_time))
        df = pd.DataFrame(r.fetchall())
        # Convert timedelta to int seconds
        for i in df.index:
            df.at[i, 1] = df.at[i, 1].seconds

        # Create line plot
        df.columns = ["Date", "Fight duration in seconds"]
        filepath, filename = plot_lineplot(df, boss)
        # Add file to embed and send it
        embed.set_image(url=f"attachment://{filename}")
        await interaction.followup.send(embed=embed, file=File(filepath))
        # Remove file
        os.remove(filepath)

    # TODO: remove??
    @commands.is_owner()
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

    @app_commands.guild_only
    @app_commands.command(name="buffs", description="Show stats about specific buffs at a boss")
    async def buffs(self, interaction: Interaction, boss: choices.bosses, buffs: typing.Optional[str]) -> None:
        # Defer to prevent interaction timeout
        await interaction.response.defer()

        # Check if logs for this boss exists in db
        statement = select(Log.fight_name).filter(Log.guild_id == interaction.guild_id)\
            .filter((Log.fight_name.ilike(f"%{boss}") | Log.fight_name.ilike(f"%{boss} cm")))
        boss_db = (await db.execute(statement)).first()
        if not boss_db:
            await interaction.followup.send("**:x: No logs found**", ephemeral=True)
            return

        # If no buffs were specified fall back to default
        if not buffs:
            buffs = ["Might", "Quickness", "Alacrity"]
        else:
            # Allow comma seperated buffs
            buffs = re.split(",\s+|,", buffs)

        data = []
        # Create embed
        embed = Embed(title=f"Buff Uptimes", color=0x0099ff)
        embed.set_author(name=boss, icon_url=bosses[boss]["icon"])
        embed.set_thumbnail(url="https://cdn.discordapp.com/emojis/818529609330851910.gif?v=1")

        # Create dataset
        for buff in buffs:
            # Check for valid Buff
            buff_map = (await db.execute(select(BuffMap).filter(BuffMap.name.ilike(f"%{buff}%")))).scalars().all()
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
                buff_map = (await db.execute(select(BuffMap).filter(BuffMap.name == closest[0]))).first()
                closest.remove(closest[0])

            # Join Tables, filter by boss and buff, group by Log.link
            statement = select(Log.date_time, func.avg(BuffUptimes.uptime), column(buff_map[0].name)) \
                .join(Player, Log.players).join(BuffUptimes, Player.buff_uptimes) \
                .filter(Player.guild_id == interaction.guild_id) \
                .filter((Log.fight_name.ilike(f"%{boss}") | Log.fight_name.ilike(f"%{boss} cm"))) \
                .filter(BuffUptimes.buff == buff_map[0].id).group_by(Log.link)
            query = (await db.execute(statement)).all()
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
            buff_map = (await db.execute(select(BuffMap).filter(BuffMap.name == df["Boon"][0]))).scalar()
            embed.set_thumbnail(url=buff_map.icon)
            embed.title = f"{buff_map.name} on {boss}"
        # Check if dataframe actually contains any data
        if df.empty:
            await interaction.followup.send(embed=embed)
        else:
            # Create line plot and add it to embed
            filepath, filename = plot_lineplot(df, boss, "Boon", True)
            embed.set_image(url=f"attachment://{filename}")
            # Suggest other close matches
            await interaction.followup.send(embed=embed, file=File(filepath))
            # Remove file
            os.remove(filepath)

    @app_commands.guild_only
    @app_commands.command(name="mechs", description="Show mechanic stats")
    async def mechs(self, interaction: Interaction, boss: choices.bosses, mechanics: typing.Optional[str]) -> None:
        # Defer to prevent interaction timeout
        await interaction.response.defer()

        # Check if logs for this boss exists in db
        statement = select(Log.fight_name).filter(Log.guild_id == interaction.guild_id)\
            .filter((Log.fight_name.ilike(f"%{boss}") | Log.fight_name.ilike(f"%{boss} cm")))
        boss_db = (await db.execute(statement)).first()
        if not boss_db:
            await interaction.followup.send("**:x: No logs found**", ephemeral=True)
            return

        embed = Embed(title=f"Mechanics", color=0x0099ff)
        embed.set_author(name=boss, icon_url=bosses[boss]["icon"])

        # If no mechanic was specified
        if not mechanics:
            # List of all mechs on the boss
            statement = select(Mechanic.description).join(Player, Log.players).join(Mechanic, Player.mechanics)\
                .filter(Log.guild_id == interaction.guild_id).distinct(Mechanic.description)\
                .filter((Log.fight_name.ilike(f"%{boss}") | Log.fight_name.ilike(f"%{boss} cm")))
            mechs = (await db.execute(statement)).all()

            # Number of logs of the specified boss
            statement = select(func.count(Log.link)).filter(Log.guild_id == interaction.guild_id)\
                .filter((Log.fight_name.ilike(f"%{boss}") | Log.fight_name.ilike(f"%{boss} cm")))
            fight_number = (await db.execute(statement)).scalar()

            for mech in mechs:
                # Total amount of mechanic triggers
                statement = select(Log.fight_name, Mechanic.description, func.sum(Mechanic.amount))\
                     .join(Player, Log.players).join(Mechanic, Player.mechanics)\
                     .filter(Log.guild_id == interaction.guild_id)\
                     .filter((Log.fight_name.ilike(f"%{boss}") | Log.fight_name.ilike(f"%{boss} cm")))\
                     .filter(Mechanic.description.ilike(f"{mech[0]}"))
                total_query = (await db.execute(statement)).all()
                embed.add_field(name=f"__{mech[0]}:__", value=f"Total: {total_query[0][2]}\n Average: {round(total_query[0][2]/fight_number, 2)}", inline=False)
            await interaction.followup.send(embed=embed)

        else:
            # Allow comma seperated mechs
            mechanics = re.split(",\s+|,", mechanics)

            # Create dataset
            data = []
            for mechanic in mechanics:
                # Check for valid Mechanic
                statement = select(Mechanic)\
                    .join(Player, Log.players)\
                    .join(Mechanic, Player.mechanics)\
                    .filter((Log.fight_name.ilike(f"%{boss}") | Log.fight_name.ilike(f"%{boss} cm")))\
                    .filter(Mechanic.description.ilike(f"%{mechanic}%"))
                mechanic_map = (await db.execute(statement)).scalars().all()
                closest = []
                # If no Mechanic was found add en embed message and skip to next item
                if len(mechanic_map) == 0:
                    embed.add_field(name="**Error**", value=f"Mechanic \"{mechanic}\" on boss \"{boss}\" was not found .", inline=False)
                    continue
                # If more than 1 match select the closest
                elif len(mechanic_map) > 1:
                    # Create list of mechanic names
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
                    mechanic_map = (await db.execute(select(Mechanic).filter(Mechanic.description == closest[0]))).scalars().all()
                    closest.remove(closest[0])

                # Join Tables, filter by boss and mechanic, group by Log.link
                statement = select(Log.date_time, func.sum(Mechanic.amount))\
                    .join(Player, Log.players)\
                    .join(Mechanic, Player.mechanics) \
                    .filter(Log.guild_id == interaction.guild_id)                    \
                    .filter((Log.fight_name.ilike(f"%{boss}") | Log.fight_name.ilike(f"%{boss} cm"))) \
                    .filter(Mechanic.description == mechanic_map[0].description).group_by(Log.link)
                query = (await db.execute(statement)).all()
                if len(query) < 1:
                    embed.add_field(name="**Error**", value=f"Not enough data for  \"{mechanic_map[0].name}\".",
                                    inline=False)
                else:
                    # Convert query output to array for pandas
                    log_list = []
                    for q in query:
                        data.append([q[0], q[1], mechanic_map[0].name])
                        log_list.append(q[0])
                    # Add buff description to embed
                    description = mechanic_map[0].name
                    # Get all logs where mechanic was not triggered and add them with the amount 0 to the data
                    statement = select(Log.date_time).filter(Log.guild_id == interaction.guild_id)\
                        .filter((Log.fight_name.ilike(f"%{boss}") | Log.fight_name.ilike(f"%{boss} cm")))
                    log_query = (await db.execute(statement)).all()
                    for log in log_query:
                        if log[0] not in log_list:
                            data.append([log[0], 0, mechanic_map[0].description])
                    embed.add_field(name=f"**{mechanic_map[0].description}:**", value=description, inline=False)

            # Create dataframe from data
            df = pd.DataFrame(data, columns=["Date", "Amount", "Mechanic"])
            # Update embed if there was only 1 valid buff selected
            if len(df["Mechanic"].unique()) == 1:
                mechanic_map = (await db.execute(select(Mechanic).filter(Mechanic.description == (df["Mechanic"][0])))).scalar()
                embed.title = f"{mechanic_map.description} on {boss}"
            # Check if dataframe actually contains any data
            if df.empty:
                await interaction.followup.send(embed=embed)
            else:
                # Create line plot and add it to embed
                filepath, filename = plot_lineplot(df, boss, "Mechanic", False)
                embed.set_image(url=f"attachment://{filename}")
                # Suggest other close matches
                await interaction.followup.send(embed=embed, file=File(filepath))
                # Remove file
                os.remove(filepath)

    @app_commands.guild_only
    @app_commands.command(name="logs", description="Search for logs")
    @app_commands.describe(emboldened="Include emboldened logs? (Default: False)")
    async def search_logs(self, interaction: Interaction, emboldened: typing.Optional[bool] = False) -> None:
        view = LogFilterView(emboldened)

        await interaction.response.send_message(view=view, ephemeral=True)

    @app_commands.guild_only
    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.command(name="delete", description="Delete logs [Admin only]")
    @app_commands.describe(logs="Logs you want to delete")
    async def delete_logs(self, interaction: Interaction, logs: str) -> None:
        await interaction.response.defer(ephemeral=True)
        # Find all logs in message
        logs = re.findall("https:\/\/dps\.report\/[a-zA-Z\-0-9\_]+", logs)

        response = ""
        for log in logs:
            # Find log in DB
            log_db = (await db.execute(select(Log).filter(Log.guild_id == interaction.guild_id).filter(Log.link == log))).scalar()
            # Check if log exists in db
            if log_db:
                await db.delete(log_db)
                response += f"{log} | Deleted\n"
            else:
                response += f"{log} | Not found\n"

        await db.commit()
        await interaction.followup.send(content=response, ephemeral=True)

    @app_commands.command(name="reindex", description="Reindex the Database")
    @app_commands.guilds(688413515366531139)
    @app_commands.choices(mode=[
        app_commands.Choice(name="All", value=0),
        app_commands.Choice(name="< 10 Players", value=1)
    ])
    async def reindex(self, interaction: Interaction, mode: app_commands.Choice[int]):
        # Make sure user is bot owner
        # TODO: fix when updating to newer discord.py version
        if not interaction.user.id == 100226718182170624:
            await interaction.response.send_message(content=f"Only the bot owner can use this command", ephemeral=True)
            return

        # Query relevant logs
        query = db.query(Log)
        match mode.value:
            case 1:
                # Find logs with less than 10 players
                query = query.outerjoin(Log.players).group_by(Log).having(func.count(Log.players) < 10)

        if query.count() == 0:
            await interaction.response.send_message(content=f"No logs found", ephemeral=True)
            return

        await interaction.response.send_message(content=f"Are you sure you want to reindex {query.count()} logs?",
                                                ephemeral=True, view=ConfirmationView(self.reindex_db, query))
        return

    async def reindex_db(self, interaction: Interaction, query):
        # Send confirmation message
        response = f"**Updating {query.count()} logs:**"
        await interaction.response.send_message(content=response)
        # Get full message instead of interaction message to prevent webhook timeout
        response_message = await interaction.original_message()
        response_message = await response_message.fetch()

        # Delete all logs from db and then add them again to update values
        errors = 0
        error_str = ""
        count = query.count()
        for idx, log in enumerate(query):
            db.delete(log)
            # "Ignore" errors here as very old logs with weird bugs/changes might get added that can just be ignored
            try:
                r = await add_log(log=log.link, guild_id=log.guild_id)
            except Exception as error:
                r = error
                traceback.print_exception(type(error), error, error.__traceback__, file=sys.stderr)

            if r is not None:
                print(r)
                errors += 1
                error_str += f"\n{r}"

            # Periodically update user on progress
            if (idx + 1) % 10 == 0:
                await response_message.edit(content=f"{response}\nParsed {idx + 1}/{count} logs.{error_str}")
                db.commit()
        await response_message.edit(content=f"{response}\nParsed {count}/{count} logs.{error_str}\n"
                                            f"**Added {count - errors}/{count} logs to the database.**")


async def setup(bot):
    await db.init()
    await db.create_all()
    await bot.add_cog(LogManager(bot))
