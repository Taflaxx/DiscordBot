import math

import discord
from discord import Embed
from cogs.logmanager.db import *
from cogs.logmanager.dicts import professions, bosses
import itertools


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
        for option, emoji in options_dict.items():
            options.append(discord.SelectOption(label=option, emoji=emoji))

        super().__init__(placeholder=placeholder, min_values=min_values, max_values=max_values, options=options)

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer()


class TextInput(discord.ui.TextInput):
    def __init__(self, label):
        super().__init__(label=label, required=False, style=discord.TextStyle.short)


order_dict = {"Target DPS": Player.dps.desc(),
             "Damage taken": Player.damage.desc(),
             "Date": Log.date_time.desc(),
             "Duration": Log.duration.asc()}


class LogSearchView(discord.ui.View):
    def __init__(self):
        super().__init__()
        self.message = None

        # Adds the dropdown to our view object.
        self.add_item(EmojiDropdown(bosses, "Select a Boss", 0, len(bosses)))

        # Since limit of dropdown is 25 options: professions are split into 2 dropdowns
        self.add_item(EmojiDropdown(dict(itertools.islice(professions.items(), 0, 24)), "Select a Profession (Heavy, Medium)", 0, 24))
        self.add_item(EmojiDropdown(dict(itertools.islice(professions.items(), 24, 36)), "Select a Profession (Light)", 0, 12))

        # Add order_by dropdown
        self.add_item(SimpleDropdown(order_dict.keys(), "Order logs by...", 1, 1))

    async def on_timeout(self):
        # disable everything on timeout
        for item in self.children:
            item.disabled = True

        await self.message.edit(view=self)

    @discord.ui.button(label="Search", style=discord.ButtonStyle.green, row=4)
    async def search(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Get values
        selected_bosses: [str] = self.children[1].values
        selected_professions: [str] = self.children[2].values
        selected_professions.extend(self.children[3].values)

        # Default to PLayer.dps if nothing was selected
        selected_order = "Target DPS"
        if self.children[4].values:
            selected_order = self.children[4].values[0]

        # Add CM version of bosses
        for boss in selected_bosses.copy():     # Use a copy of the list to prevent infinite loop
            selected_bosses.append(f"{boss} CM")

        print(selected_bosses, selected_professions, order_dict[selected_order])

        # Query DB
        query = db.query(Player).join(Log)
        if selected_bosses:
            query = query.filter(Log.fight_name.in_(selected_bosses))
        if selected_professions:
            query = query.filter(Player.profession.in_(selected_professions))
        query = query.order_by(order_dict[selected_order])

        if query.count() == 0:
            await interaction.response.send_message(content="**:x: No logs found**", )
            return

        embed = create_log_embed(query, selected_order)

        view = LogPaginationView(query, selected_order)
        await interaction.response.send_message(embed=embed, view=view)
        view.message = await interaction.original_message()


def create_log_embed(query, order, start: int = 0, end: int = 10):
    embed = Embed(title="Top Logs", color=0x0099ff)
    val = ""
    for i in range(start, min(end, query.count())):
        row = query[i]
        val += f"[{i + 1}. {row.log.fight_name}:]({row.log.link})\n{row.character} - {row.profession}\n" \
               f"DPS: {row.dps}\nDamage taken: {row.damage}\n\n"
        # Split into a new field every 5 logs because of character limits
        if (i + 1) % 5 == 0:
            embed.add_field(name=f"Sorted by {order} [{i - 3} - {i + 1}]:", value=val)
            val = ""
        # Make sure embed is added when < 5 logs are left
        elif i + 1 == query.count():
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
        self.last_page = math.ceil(query.count() / logs_per_page) - 1

    def switch_to_page(self, page: int):
        # create updated embed
        start = page * self.logs_per_page
        end = page * self.logs_per_page + self.logs_per_page
        embed = create_log_embed(self.query, self.order, start, end)

        # update page counter
        self.current_page = page
        self.children[2].label = f"{page}/{self.last_page}"

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
