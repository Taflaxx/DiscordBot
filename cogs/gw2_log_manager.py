from discord.ext import commands, tasks
from discord import Embed
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy import Column, Integer, String, create_engine, ForeignKey
import logging
import requests
import json
import re


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

    @log.command(name="add", aliases=["a"], help="Add a log to the database", usage="[log]")
    async def add_log(self, ctx, log):
        # Check if log already exists in the database
        if db.query(Log).filter_by(link=log).first():
            await ctx.send("**`ERROR:`** Log already exists in the database")
            print("Log is already in Database. Skipping...")
            return

        # Get json data
        r = requests.get("https://dps.report/getJson?permalink=" + log)
        data = json.loads(r.content)

        # Check if boss was killed
        if not data["success"]:
            await ctx.send("**`ERROR:`** Boss was not killed")
            return

        # Create log in DB
        log_db = Log(link=log, fight_name=data["fightName"])

        # Parse json data for each player
        for player in data["players"]:
            # Check if the player is an actual player and not a NPC
            if re.match("^[a-zA-Z]+\.(\d{4})$", player["account"]):
                print(f"{log} | {data['fightName']} | {player['account']}")
                player_db = Player(account=player["account"])
                player_db.character_name = player["name"]
                player_db.profession = player["profession"]
                player_db.dps_all = player["dpsAll"][0]["dps"]
                player_db.damage_taken = player["defenses"][0]["damageTaken"]
                log_db.players.append(player_db)
                db.add(player_db)
            else:
                print(f"{player['account']} is not a player. Skipping...")
        db.add(log_db)
        db.commit()
        print("DB commit")

    @log.command(name="filter")
    async def filter_log(self, ctx, *args):
        result = db.query(Player).join(Log)
        count = 0
        for arg in args:
            if arg == "-a":
                print(args[count+1])
                result = result.filter(Player.account == args[count + 1])
            elif arg == "-c":
                result = result.filter(Player.character_name == args[count + 1])
            elif arg == "-b":
                result = result.filter(Log.fight_name == args[count + 1])
            count += 1
        result = result.order_by(Player.dps_all.desc())
        print(result)
        embed = Embed(title="Logs")
        for row in result:
            embed.add_field(name=f"{row.log.fight_name}", value=f"[Link]({row.log.link})\n DPS: {row.dps_all}")
        await ctx.send(embed=embed)

    @log.command(name="sql")
    async def sql_log(self, ctx, *text):
        db.execute(" ".join(text))


def setup(bot):
    bot.add_cog(LogManager(bot))
