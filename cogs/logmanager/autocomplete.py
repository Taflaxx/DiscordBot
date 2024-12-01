from typing import List

import discord
from discord import app_commands

from cogs.logmanager.dicts import bosses, professions


async def bosses_autocomplete(interaction: discord.Interaction, current: str) -> List[app_commands.Choice[str]]:
    return [
        app_commands.Choice(name=boss, value=boss)
        for boss in bosses.keys() if current.lower() in boss.lower()
    ][:25]


async def professions_autocomplete(interaction: discord.Interaction, current: str) -> List[app_commands.Choice[str]]:
    return [
        app_commands.Choice(name=profession, value=profession)
        for profession in professions.keys() if current.lower() in profession.lower()
    ][:25]
