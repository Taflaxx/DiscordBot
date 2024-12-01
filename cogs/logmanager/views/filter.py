import math

import discord
from discord import Embed

from cogs.logmanager.db import *


class SimpleDropdown(discord.ui.Select):
    def __init__(self, options_list: [], placeholder: str = None, min_values: int = 1, max_values: int = 1):
        options = []
        for item in options_list:
            options.append(discord.SelectOption(label=item))

        super().__init__(placeholder=placeholder, min_values=min_values, max_values=max_values, options=options)

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer()


class EmojiDropdown(discord.ui.Select):
    def __init__(self, options_dict: {}, placeholder: str = None, min_values: int = 1, max_values: int = 1):
        options = []
        for key, value in options_dict.items():
            options.append(discord.SelectOption(label=key, emoji=value["emoji"]))

        super().__init__(placeholder=placeholder, min_values=min_values, max_values=max_values, options=options)

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer()


order_dict = {"Target DPS": Player.dps.desc(),
              "Breakbar": Player.breakbar.desc(),
              "Damage taken": Player.damage.desc(),
              "Date": Log.date_time.desc(),
              "Duration": Log.duration.asc()}


def create_log_embed(query, order, start: int = 0, end: int = 10):
    embed = Embed(title="Top Logs", color=0x0099ff)
    val = ""
    for i in range(start, min(end, len(query))):
        row = query[i]
        val += f"{i + 1}: [{row.log.fight_name}:]({row.log.link})\n{row.character} - {row.profession}\n" \
               f"DPS: {row.dps}\nDamage taken: {row.damage}\n\n"
        # Split into a new field every 5 logs because of character limits
        if (i + 1) % 5 == 0:
            embed.add_field(name=f"Sorted by {order} [{i - 3} - {i + 1}]:", value=val)
            val = ""
        # Make sure embed is added when < 5 logs are left
        elif i + 1 == len(query):
            embed.add_field(name=f"Sorted by {order}:", value=val)
        # For better formatting (max 2 fields next to each other) add an invisible field
        if (i + 1) % 10 == 0:
            embed.add_field(name="\u200b", value="\u200b")

    return embed


class LogPaginationView(discord.ui.View):
    def __init__(self, query, order, logs_per_page: int = 10):
        super().__init__()
        self.message = None

        self.query = query
        self.order = order
        self.logs_per_page = logs_per_page
        self.current_page = 0
        self.last_page = math.ceil(len(query) / logs_per_page) - 1
        self.children[2].label = f"{1}/{self.last_page + 1}"

    def switch_to_page(self, page: int):
        # create updated embed
        start = page * self.logs_per_page
        end = page * self.logs_per_page + self.logs_per_page
        embed = create_log_embed(self.query, self.order, start, end)

        # update page counter
        self.current_page = page
        self.children[2].label = f"{page + 1}/{self.last_page + 1}"

        return embed

    @discord.ui.button(emoji="⏮️", style=discord.ButtonStyle.blurple)
    async def first(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.current_page > 0:
            await interaction.response.edit_message(embed=self.switch_to_page(0), view=self)
        else:
            await interaction.response.defer()

    @discord.ui.button(emoji="⬅️", style=discord.ButtonStyle.blurple)
    async def back(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.current_page > 0:
            await interaction.response.edit_message(embed=self.switch_to_page(self.current_page - 1), view=self)
        else:
            await interaction.response.defer()

    @discord.ui.button(label="1", style=discord.ButtonStyle.gray, disabled=True)
    async def page_number(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()

    @discord.ui.button(emoji="➡️", style=discord.ButtonStyle.blurple)
    async def forward(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.current_page < self.last_page:
            await interaction.response.edit_message(embed=self.switch_to_page(self.current_page + 1), view=self)
        else:
            await interaction.response.defer()

    @discord.ui.button(emoji="⏭️", style=discord.ButtonStyle.blurple)
    async def last(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.current_page < self.last_page:
            await interaction.response.edit_message(embed=self.switch_to_page(self.last_page), view=self)
        else:
            await interaction.response.defer()

    async def on_timeout(self):
        # disable everything on timeout
        for item in self.children:
            item.disabled = True

        await self.message.edit(view=self)
