import aiohttp
from discord.ext import commands
from discord import Embed, File
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy import Column, Integer, String, create_engine, ForeignKey, DateTime
import logging
import os
import re
from datetime import datetime, timezone
import csv

# Boss name abbreviations for easier searching
boss_abrv = {"sab": "Sabetha the Saboteur", "gors": "Gorseval the Multifarious", "vg": "Vale Guardian",
             "matt" : "Matthias Gabrel", "sloth": "Slothasor", "kc": "Keep Construct",
             "mo": "Mursaat Overseer", "sam": "Samarog", "dei": "Deimos", "sh": "Soulless Horror",
             "tl": "Twin Largos", "ca": "Conjured Amalgamate", "qpeer": "Qadim the Peerless",
             "q1": "Qadim", "q2": "Qadim the Peerless", "sabir": "Cardinal Sabir", "adina": "Cardinal Adina"}

# Set up logging
logger = logging.getLogger('sqlalchemy.engine')
logger.setLevel(logging.DEBUG)
handler = logging.FileHandler(filename='gw2_log_man.log', encoding='utf-8', mode='w')
handler.setFormatter(logging.Formatter('%(asctime)s:%(levelname)s:%(name)s: %(message)s'))
logger.addHandler(handler)

# Init DB
Base = declarative_base()
engine = create_engine('sqlite:///gw2_log_manager.db', echo=False)
Session = sessionmaker(bind=engine)
db = Session()


class Log(Base):
    __bind_key__ = "gw2_log_manager"
    __tablename__ = "logs"

    link = Column(String, primary_key=True)
    fight_name = Column(String)
    date_time = Column(DateTime)
    players = relationship("Player", back_populates="log")


class Player(Base):
    __bind_key__ = "gw2_log_manager"
    __tablename__ = "players"

    id = Column(Integer, primary_key=True)
    log_link = Column(String, ForeignKey("logs.link"))
    log = relationship("Log", back_populates="players")
    account = Column(String)
    character = Column(String)
    profession = Column(String)
    dps = Column(Integer)
    damage = Column(Integer)


Base.metadata.create_all(engine)
Log.__table__.create(bind=engine, checkfirst=True)
Player.__table__.create(bind=engine, checkfirst=True)
db.commit()


class LogManager(commands.Cog, name="log"):
    def __init__(self, bot):
        self.bot = bot

    def cog_unload(self):
        pass

    @commands.group(name="log", aliases=["l"], help="For all your logging needs")
    async def log(self, ctx):
        if ctx.invoked_subcommand is None:
            await ctx.send_help("log")
            print(f"Unknown subcommand \"{ctx.message.content}\" by {ctx.author}. Sent help page")

    @log.command(name="add", aliases=["a"], help="Add logs to the database", usage="[log(s)]")
    async def add_logs(self, ctx, *, arg):
        # Find all links to logs in the message
        logs = re.findall("https:\/\/dps\.report\/[a-zA-Z\-0-9\_]+", arg)
        print(f"Found {len(logs)} Logs")
        message = await ctx.send(f"Found {len(logs)} logs:")

        errors = 0  # Tracks the number of errors while adding logs
        for log in logs:
            r = await self.add_log(log)
            if r is not None:
                errors += 1
                await message.edit(content=f"{message.content}\n{r}")   # update original message with errors
        db.commit()
        await message.edit(content=f"{message.content}\nAdded {len(logs)-errors}/{len(logs)} logs to the database.")

    async def add_log(self, log):
        # Check if log already exists in the database
        if db.query(Log).filter_by(link=log).first():
            print(f"{log} | Already in Database")
            return f"{log} | Already in Database"

        # Get json data
        # Using aiohttp as it works async
        async with aiohttp.ClientSession() as session:
            async with session.get("https://dps.report/getJson?permalink=" + log) as r:
                if r.status == 200:
                    data = await r.json()
                else:
                    return f"{log} | {r.status}"

        # Check if boss was killed
        if not data["success"]:
            print(f"{log} | Boss was not killed")
            return f"{log} | Boss was not killed"

        # Create log in DB
        log_db = Log(link=log, fight_name=data["fightName"])
        print(f"{log} | {data['fightName']}:")

        # Convert time to utc
        log_db.date_time = datetime.strptime(data["timeStartStd"], "%Y-%m-%d %H:%M:%S %z").astimezone(timezone.utc)

        # Parse json data for each player
        for player in data["players"]:
            # Check if the player is an actual player and not a NPC
            if re.match("^[a-zA-Z]+\.(\d{4})$", player["account"]):
                player_db = Player(account=player["account"])
                player_db.character = player["name"]
                player_db.profession = player["profession"]
                player_db.dps = player["dpsAll"][0]["dps"]
                player_db.damage = player["defenses"][0]["damageTaken"]
                log_db.players.append(player_db)
                db.add(player_db)
                print(f"{player_db.account} | {player_db.character} | {player_db.profession}")
            else:
                print(f"{player['account']} | Not a player")
        db.add(log_db)

    @log.command(name="filter", usage="[-a/-account][-c/-character][-p/-profession][-b/-boss][-cm][-nm][-csv]")
    async def filter_log(self, ctx, *args):
        result = db.query(Player).join(Log)

        # Parsing arguments
        export_csv = False
        for i, arg in enumerate(args):
            if arg == "-a" or arg == "-account":
                result = result.filter(Player.account.ilike(f"%{args[i + 1]}%"))
            elif arg == "-c" or arg == "-character":
                result = result.filter(Player.character.ilike(f"%{args[i + 1]}%"))
            elif arg == "-p" or arg == "-profession":
                result = result.filter(Player.profession.ilike(f"%{args[i + 1]}%"))
            elif arg == "-b" or arg == "-boss":
                if args[i + 1].lower() in boss_abrv:
                    boss = boss_abrv[args[i + 1]]
                else:
                    boss = args[i + 1]
                result = result.filter(Log.fight_name.ilike(f"%{boss}%"))   # case insensitive LIKE
            elif arg == "-csv":
                export_csv = True
            elif arg == "-cm":
                result = result.filter(Log.fight_name.ilike("% CM"))
            elif arg == "-nm":
                result = result.filter(Log.fight_name.notilike("% CM"))
        result = result.order_by(Player.dps.desc())

        if result.count() == 0:
            await ctx.send("**:x: No logs found**")
            return

        if export_csv:
            # Create csv, send it and delete it afterwards
            filename = f"tmp/{datetime.now(tz=timezone.utc).strftime('export-%Y%m%d-%H%M%S')}.csv"
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
                val += f"[{i+1}. {row.log.fight_name}:]({row.log.link})\n{row.character} - {row.profession}\n" \
                       f"DPS: {row.dps}\nDamage taken: {row.damage}\n\n"
            embed.add_field(name=f"Sorted by dps [1-5]:", value=val)

            val = ""
            if result[5:10]:
                for i, row in enumerate(result[5:10]):
                    val += f"[{i+6}. {row.log.fight_name}:]({row.log.link})\n{row.character} - {row.profession}\n" \
                           f"DPS: {row.dps}\nDamage taken: {row.damage}\n\n"
                embed.add_field(name=f"Sorted by dps [6-10]:", value=val)

            embed.add_field(name="\u200B", value="If you find any bugs or your dps seems low you can submit a bugreport "
                                                 "[here](https://www.youtube.com/watch?v=d1YBv2mWll0)", inline=False)
            await ctx.send(embed=embed)


def setup(bot):
    bot.add_cog(LogManager(bot))
