from datetime import datetime, timezone, timedelta
import re
import aiohttp
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, Float, Boolean, Interval, func, select
from cogs.logmanager.utils import boss_abrv, sort_dict
import cogs.logmanager.dicts as dicts

Base = declarative_base()


class AsyncDatabaseSession:
    def __init__(self):
        self._session = None
        self._engine = None

    def __getattr__(self, name):
        return getattr(self._session, name)

    async def init(self):
        self._engine = create_async_engine("sqlite+aiosqlite:///cogs/logmanager/logmanager.db", echo=False,)
        self._session = sessionmaker(self._engine, expire_on_commit=False, class_=AsyncSession)()

    async def create_all(self):
        async with self._engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)


db = AsyncDatabaseSession()


class Log(Base):
    __bind_key__ = "logmanager"
    __tablename__ = "logs"

    link = Column(String, primary_key=True)
    guild_id = Column(Integer)
    fight_name = Column(String)
    duration = Column(Interval)
    date_time = Column(DateTime)
    players = relationship("Player", back_populates="log", cascade="all, delete, delete-orphan")
    emboldened = Column(Integer)


class Player(Base):
    __bind_key__ = "logmanager"
    __tablename__ = "players"

    id = Column(Integer, primary_key=True)
    guild_id = Column(Integer)
    log_link = Column(String, ForeignKey("logs.link"))
    log = relationship("Log", back_populates="players")
    account = Column(String)
    character = Column(String)
    profession = Column(String)
    dps = Column(Integer)
    breakbar = Column(Integer)
    damage = Column(Integer)
    downs = Column(Integer)
    deaths = Column(Integer)
    buff_generation = relationship("BuffGeneration", back_populates="player", cascade="all, delete, delete-orphan")
    buff_uptimes = relationship("BuffUptimes", back_populates="player", cascade="all, delete, delete-orphan")
    mechanics = relationship("Mechanic", back_populates="player", cascade="all, delete, delete-orphan")


class BuffUptimes(Base):
    __bind_key__ = "logmanager"
    __tablename__ = "buff_uptimes"

    id = Column(Integer, primary_key=True)
    guild_id = Column(Integer)
    player_id = Column(Integer, ForeignKey("players.id"))
    player = relationship("Player", back_populates="buff_uptimes")
    buff = Column(Integer)
    uptime = Column(Float)


class BuffGeneration(Base):
    __bind_key__ = "logmanager"
    __tablename__ = "buff_generation"

    id = Column(Integer, primary_key=True)
    guild_id = Column(Integer)
    player_id = Column(Integer, ForeignKey("players.id"))
    player = relationship("Player", back_populates="buff_generation")
    buff = Column(Integer)
    uptime = Column(Float)


class Mechanic(Base):
    __bind_key__ = "logmanager"
    __tablename__ = "mechanics"

    id = Column(Integer, primary_key=True)
    guild_id = Column(Integer)
    player_id = Column(Integer, ForeignKey("players.id"))
    player = relationship("Player", back_populates="mechanics")
    name = Column(String)
    description = Column(String)
    amount = Column(Integer)


class BuffMap(Base):
    __bind_key__ = "logmanager"
    __tablename__ = "buffMaps"

    id = Column(Integer, primary_key=True)
    name = Column(String)
    icon = Column(String)
    stacking = Column(Boolean)
    description = Column(String)


class Config(Base):
    __bind_key__ = "logmanager"
    __tablename__ = "config"

    guild_id = Column(Integer, primary_key=True)
    log_channel_id = Column(Integer)


async def add_log(log: str, guild_id: int):
    # Check if log already exists in the database
    if (await db.execute(select(Log).filter_by(link=log))).first():
        return f"{log} | Already in Database"

    # Get json data
    # Using aiohttp as it works async
    async with aiohttp.ClientSession() as session:
        async with session.get("https://dps.report/getJson?permalink=" + log) as r:
            if r.status == 200:
                try:
                    data = await r.json()
                except Exception as e:
                    return f"{log} | {e}"
            else:
                return f"{log} | {r.status}"

    # Check if boss was killed
    if not data["success"]:
        return f"{log} | Boss was not killed"

    # Create log
    log_db = Log(link=log, guild_id=guild_id)

    # Check if language is supported
    if not data["language"] in dicts.translate_to_english.keys():
        return f"{log} | Language error"
    # # Check if boss is supported
    if data["fightName"].replace(" CM", "") in dicts.translate_to_english[data["language"]].keys():
        log_db.fight_name = dicts.translate_to_english[data["language"]][data["fightName"].replace(" CM", "")]
        # Add "CM" back to fight name
        if data["fightName"].endswith(" CM"):
            log_db.fight_name += " CM"
    else:
        return f"{log} | Boss not supported"

    # Convert time to utc
    log_db.date_time = datetime.strptime(data["timeStartStd"], "%Y-%m-%d %H:%M:%S %z").astimezone(timezone.utc)

    # Get fight duration
    t = data["duration"].split(" ")
    log_db.duration = timedelta(minutes=int(t[0][:-1]), seconds=int(t[1][:-1]), microseconds=int(t[2][:-2])*1000)

    # Set initial emboldened value
    log_db.emboldened = 0

    # Parse json data for each player
    for player in data["players"]:
        # Check if the player is an actual player and not a NPC
        if not re.match("^[a-zA-Z\s]+\.(\d{4})$", player["account"]):
            continue

        # Player was probably dead the entire fight or switched character mid-fight, so we can skip
        if "selfBuffsActive" not in player:
            continue

        # General stuff
        player_db = Player(account=player["account"], guild_id=guild_id)
        player_db.character = player["name"]
        player_db.profession = player["profession"]

        # Add DPS
        if log_db.fight_name.startswith("Dhuum"):
            # Special case for Dhuum to ignore the long pre-event
            # Only include "Dhuum Fight"
            player_db.dps = player["dpsTargets"][0][3]["dps"]
        elif log_db.fight_name.startswith("Twin Largos"):  # Because Twin Largos is 2 bosses
            targets = ["Nikare", "Kenut"]
            player_db.dps = get_dps_from_targets(data, player, targets)
        elif log_db.fight_name.startswith("Captain Mai Trin"):  # Because Mai Trin is 2 bosses
            targets = ["Captain Mai Trin", "Echo of Scarlet Briar"]
            player_db.dps = get_dps_from_targets(data, player, targets)
        elif log_db.fight_name.startswith("The Dragonvoid"):  # Because HT has many bosses
            targets = ["The JormagVoid", "The PrimordusVoid", "The KralkatorrikVoid",
                       "The MordremothVoid", "The ZhaitanVoid", "The SooWonVoid"]
            player_db.dps = get_dps_from_targets(data, player, targets)
        else:
            player_db.dps = player["dpsTargets"][0][0]["dps"]

        # Add breakbar
        if "breakbarDamage" in player["dpsAll"][0]:
            player_db.breakbar = player["dpsAll"][0]["breakbarDamage"]
        else:
            # Old logs might not have a breakbar stat
            player_db.breakbar = 0

        # Add defensive stats
        player_db.damage = player["defenses"][0]["damageTaken"]
        player_db.downs = player["defenses"][0]["downCount"]
        player_db.deaths = player["defenses"][0]["deadCount"]

        # Check for emboldened
        for buff in player["buffUptimes"]:
            if buff["id"] == 68087:
                log_db.emboldened = max(log_db.emboldened, round(buff["buffData"][0]["uptime"]))

        # Add buff uptimes
        for buff in player["buffUptimesActive"]:
            buff_db = BuffUptimes(buff=buff["id"], guild_id=guild_id)
            buff_db.uptime = buff["buffData"][0]["uptime"]
            player_db.buff_uptimes.append(buff_db)
            db.add(buff_db)

        # Add buff generation
        for buff in player["selfBuffsActive"]:
            buff_db = BuffGeneration(buff=buff["id"], guild_id=guild_id)
            buff_db.uptime = buff["buffData"][0]["generation"]
            player_db.buff_generation.append(buff_db)
            db.add(buff_db)

        # Add mechanics
        for mech in data["mechanics"]:
            mech_db = Mechanic(name=mech["name"], description=mech["description"], amount=0, guild_id=guild_id)
            for mech_data in mech["mechanicsData"]:
                if mech_data["actor"] == player_db.character:
                    mech_db.amount += 1
            # Only add mech if player interacted with it
            if mech_db.amount > 0:
                player_db.mechanics.append(mech_db)
                db.add(mech_db)

        # Add to DB
        log_db.players.append(player_db)
        db.add(player_db)
    db.add(log_db)

    # BuffMap
    for buff_map in data["buffMap"]:
        # Check if this buff already exists in DB
        if not (await db.execute(select(BuffMap.id).filter(BuffMap.id == buff_map[1:]))).first():
            buff_map_db = BuffMap(id=buff_map[1:])
            buff_map_db.name = data["buffMap"][buff_map]["name"]
            buff_map_db.icon = data["buffMap"][buff_map]["icon"]
            buff_map_db.stacking = data["buffMap"][buff_map]["stacking"]
            if "descriptions" in data["buffMap"][buff_map]:
                description = ""
                for d in data["buffMap"][buff_map]["descriptions"]:
                    description += f"{d}\n"
                buff_map_db.description = description.rstrip()
            db.add(buff_map_db)


async def filter_args(query, args):
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
        elif arg == "-after":
            query = query.filter(Log.date_time > datetime.fromisoformat(args[i + 1]).astimezone(timezone.utc))
        elif arg == "-before":
            query = query.filter(Log.date_time < datetime.fromisoformat(args[i + 1]).astimezone(timezone.utc))
    return query

order_obj = {"dps": Player.dps,
             "dmg": Player.damage, "damage": Player.damage,
             "date": Log.date_time,
             "time": Log.duration, "duration": Log.duration}


async def order_args(query, args):
    order = "dps"
    limit = 10
    for i, arg in enumerate(args):
        if arg == "-order":
            order = args[i + 1]
        elif arg == "-limit":
            limit = args[i + 1]

    # Order By
    if order in order_obj:
        if "-asc" in args:
            query = query.order_by(order_obj[order].asc())
        else:
            query = query.order_by(order_obj[order].desc())
    else:
        if "-asc" in args:
            query = query.order_by(Player.dps.asc())
        else:
            query = query.order_by(Player.dps.desc())
    return query, order, limit


async def get_player_stats(player_stat, min_appearances=50):
    stats = {}
    appearances = {}
    # Iterate over all distinct accounts
    for player in db.query(Player.account).distinct():
        player = player[0]
        # Count how often the player appears in the logs
        appearance = db.query(Player.account).filter(Player.account.ilike(player)).count()
        # Only add players that are in at least [min_appearances] logs (Default: 10)
        if appearance >= min_appearances:
            appearances[player] = appearance
            stats[player] = db.query(func.sum(player_stat)).filter(Player.account.ilike(player)).all()[0][0]
    # Calculate averages
    averages = {}
    for player in appearances.keys():
        averages[player] = stats[player] / appearances[player]
    # Sort, convert to a list and reverse the order
    stats = sort_dict(stats)[::-1]
    averages = sort_dict(averages)[::-1]
    return stats, averages


def get_dps_from_targets(log, player, targets):
    # Get IDs of the targets
    ids = []
    for idx, target in enumerate(log["targets"]):
        if target["name"] in targets:
            ids.append(idx)
    # Get the dps dealt to the targets
    dps = 0
    for id in ids:
        dps += player["dpsTargets"][id][0]["dps"]
    return dps