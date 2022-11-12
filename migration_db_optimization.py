from cogs.logmanager.db import *
import asyncio


async def update():
    db = AsyncDatabaseSession()
    await db.init()

    # Update primary key in logs table
    await db.execute("ALTER TABLE logs RENAME TO old_logs;")
    await db.create_all()
    await db.execute("INSERT INTO logs SELECT * FROM old_logs;")
    await db.execute("DROP TABLE old_logs;")

    # create mech_map table

    await db.execute("VACUUM;")
    await db.commit()
    await db.close()

if __name__ == "__main__":
    asyncio.run(update())

