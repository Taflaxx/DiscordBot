from datetime import datetime, timezone
import re
import aiohttp
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import  relationship
from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from cogs.logmanager.utils import boss_abrv

# Init DB
engine = create_engine("sqlite:///cogs/logmanager/logmanager.db", echo=False)
Session = sessionmaker(bind=engine)
db = Session()
Base = declarative_base()


class Log(Base):
    __bind_key__ = "logmanager"
    __tablename__ = "logs"

    link = Column(String, primary_key=True)
    fight_name = Column(String)
    date_time = Column(DateTime)
    players = relationship("Player", back_populates="log")


class Player(Base):
    __bind_key__ = "logmanager"
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


async def add_log(log):
    # Check if log already exists in the database
    if db.query(Log).filter_by(link=log).first():
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
        return f"{log} | Boss was not killed"

    # Create log in DB
    log_db = Log(link=log, fight_name=data["fightName"])

    # Convert time to utc
    log_db.date_time = datetime.strptime(data["timeStartStd"], "%Y-%m-%d %H:%M:%S %z").astimezone(timezone.utc)

    # Parse json data for each player
    for player in data["players"]:
        # Check if the player is an actual player and not a NPC
        if re.match("^[a-zA-Z]+\.(\d{4})$", player["account"]):
            player_db = Player(account=player["account"])
            player_db.character = player["name"]
            player_db.profession = player["profession"]
            player_db.dps = player["dpsTargets"][0][0]["dps"]
            player_db.damage = player["defenses"][0]["damageTaken"]
            log_db.players.append(player_db)
            db.add(player_db)
    db.add(log_db)


async def filter_args(query, args):
    order = "dps"
    limit = 10
    for i, arg in enumerate(args):
        if arg == "-a" or arg == "--account":
            query = query.filter(Player.account.ilike(f"%{args[i + 1]}%"))
        elif arg == "-c" or arg == "--character":
            query = query.filter(Player.character.ilike(f"%{args[i + 1]}%"))
        elif arg == "-p" or arg == "--profession":
            query = query.filter(Player.profession.ilike(f"%{args[i + 1]}%"))
        elif arg == "-b" or arg == "--boss":
            if args[i + 1].lower() in boss_abrv:
                boss = boss_abrv[args[i + 1]]
            else:
                boss = args[i + 1]
            # Prevent "Qadim the Peerless" logs from showing up when searching for qadim
            if boss.lower() == "qadim":
                query = query.filter(Log.fight_name.ilike(boss))
            else:
                query = query.filter(Log.fight_name.ilike(f"%{boss}%"))  # case insensitive LIKE
        elif arg == "-cm":
            query = query.filter(Log.fight_name.ilike("% CM"))
        elif arg == "-nm":
            query = query.filter(Log.fight_name.notilike("% CM"))
        elif arg == "-order":
            order = args[i + 1]
        elif arg == "-limit":
            limit = args[i + 1]

    # Order By
    # TODO: Cleaner implementation
    if order == "dmg" or order == "damage":
        if "-desc" in args:
            query = query.order_by(Player.damage.desc())
        else:
            query = query.order_by(Player.damage.asc())
    else:
        order = "dps"
        if "-asc" in args:
            query = query.order_by(Player.dps.asc())
        else:
            query = query.order_by(Player.dps.desc())
    return query, order, limit
