import sqlite3
import traceback
import sys
from cogs.logmanager.db import Log, add_log
import datetime

# Create new column
con = sqlite3.connect("../cogs/logmanager/logmanager.db")
cur = con.cursor()
cur.execute("ALTER TABLE logs ADD COLUMN emboldened INTEGER DEFAULT 0")

from cogs.logmanager.db import db

# Delete all logs from db and then add them again to update values
date = datetime.datetime(year=2022, month=6, day=28)
query = db.query(Log).filter(Log.date_time >= date)
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
        print(f"Parsed {idx + 1}/{count} logs.{error_str}")
        db.commit()