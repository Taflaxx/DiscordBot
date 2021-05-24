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
    character_name = Column(String)
    profession = Column(String)
    dps_all = Column(Integer)
    damage_taken = Column(Integer)


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

    @log.command(name="add", aliases=["a"], help="Add logs to the database", usage="[log]")
    async def add_logs(self, ctx, *, arg):
        # Find all links to logs in the message
        logs = re.findall("https:\/\/dps\.report\/[a-zA-Z\-0-9\_]+", arg)
        print(f"Found {len(logs)} Logs")
        for log in logs:
            await self.add_log(log)
        db.commit()

    async def add_log(self, log):
        # Check if log already exists in the database
        if db.query(Log).filter_by(link=log).first():
            print(f"{log} | Already in Database")
            return

        # Get json data
        # Using aiohttp as it works async
        async with aiohttp.ClientSession() as session:
            async with session.get("https://dps.report/getJson?permalink=" + log) as r:
                if r.status == 200:
                    data = await r.json()

        # Check if boss was killed
        if not data["success"]:
            print(f"{log} | Boss was not killed")
            return

        # Create log in DB
        log_db = Log(link=log, fight_name=data["fightName"])
        print(f"{log} | {data['fightName']}:")

        # Convert time to utc
        log_db.date_time = datetime.strptime(data["timeStartStd"], '%Y-%m-%d %H:%M:%S %z', )

        # Parse json data for each player
        for player in data["players"]:
            # Check if the player is an actual player and not a NPC
            if re.match("^[a-zA-Z]+\.(\d{4})$", player["account"]):
                player_db = Player(account=player["account"])
                player_db.character_name = player["name"]
                player_db.profession = player["profession"]
                player_db.dps_all = player["dpsAll"][0]["dps"]
                player_db.damage_taken = player["defenses"][0]["damageTaken"]
                log_db.players.append(player_db)
                db.add(player_db)
                print(f"{player_db.account} | {player_db.character_name} | {player_db.profession}")
            else:
                print(f"{player['account']} | Not a player")
        db.add(log_db)

    @log.command(name="filter")
    async def filter_log(self, ctx, *args):
        result = db.query(Player).join(Log)

        # Parsing arguments
        count = 0
        export_csv = False
        for arg in args:
            if arg == "-a":
                result = result.filter(Player.account == args[count + 1])
            elif arg == "-c":
                result = result.filter(Player.character_name == args[count + 1])
            elif arg == "-p":
                result = result.filter(Player.profession == args[count + 1])
            elif arg == "-b":
                if args[count + 1] in boss_abrv:
                    boss = boss_abrv[args[count + 1]]
                else:
                    boss = args[count + 1]
                result = result.filter(Log.fight_name == boss)
            elif arg == "-csv":
                export_csv = True
            count += 1
        result = result.order_by(Player.dps_all.desc())

        if result.count() == 0:
            await ctx.send("**`ERROR:`** No logs found")
            return
        if export_csv:
            filename = f"tmp/{datetime.now(tz=timezone.utc).strftime('export-%Y%m%d-%H%M%S')}.csv"
            with open(filename, mode="w", newline="") as file:
                csv_writer = csv.writer(file, delimiter=',')
                for row in result:
                    csv_writer.writerow([row.log.link, row.log.fight_name, row.account, row.character_name,
                                         row.profession, row.dps_all, row.damage_taken])
            await ctx.send(file=File(filename))
            os.remove(filename)

        else:
            # Create Embed
            # Limited to top 9 logs
            embed = Embed(title="Top Logs")
            val = ""
            for row in result[:5]:
                val += f"[{row.log.fight_name}:]({row.log.link})\n{row.character_name} - {row.profession}\n" \
                       f"DPS: {row.dps_all}\nDamage taken: {row.damage_taken}\n\n"
            embed.add_field(name=f"Sorted by dps", value=val)
            await ctx.send(embed=embed)


def setup(bot):
    bot.add_cog(LogManager(bot))
