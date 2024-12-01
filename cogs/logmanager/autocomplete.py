from typing import List

import discord
from discord import app_commands

from cogs.logmanager.dicts import bosses


async def bosses_autocomplete(interaction: discord.Interaction, current: str) -> List[app_commands.Choice[str]]:
    return [
        app_commands.Choice(name=fruit, value=fruit)
        for fruit in bosses.keys() if current.lower() in fruit.lower()
    ][:25]
