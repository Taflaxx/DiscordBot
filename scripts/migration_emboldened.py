import traceback
import sys
import datetime
from cogs.logmanager.db import Log, add_log, db, engine
from sqlalchemy import inspect
import asyncio

async def update_db():
    # Delete all logs from db and then add them again to update values
    date = datetime.datetime(year=2022, month=6, day=28)
    query = db.query(Log).filter(Log.date_time >= date)

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
        # Periodically update user on progress
        if (idx + 1) % 10 == 0:
            print(f"Progress: {idx + 1}/{count} logs")
            db.commit()

if __name__ == "__main__":
    # check if column exists and commit
    insp = inspect(engine)
    exists = False
    for column in insp.get_columns("logs"):
        if column["name"] == "emboldened":
            print("Column already exists")
            exists = True
            break
    if not exists:
        print("Creating column...")
        db.execute("ALTER TABLE logs ADD COLUMN emboldened INTEGER DEFAULT 0")
        print("Finished creating column")
    else:
        print("Skipping column creation")

    # Update logs in the db
    print("Updating logs...")
    asyncio.run(update_db())
    print("Finished updating logs")
