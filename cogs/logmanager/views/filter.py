import math
import discord
from discord import Embed
from cogs.logmanager.db import *
from cogs.logmanager.dicts import professions, bosses, boons
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


class LogFilterView(discord.ui.View):
    def __init__(self):
        super().__init__()
        self.nameModal = NameFilter()
        self.boonModal = BoonFilter()

        # Adds the dropdowns to our view object
        self.bosses = EmojiDropdown(bosses, "Select a Boss", 0, len(bosses))
        self.add_item(self.bosses)

        # Since limit of dropdown is 25 options: professions are split into 2 dropdowns
        self.professions_1 = EmojiDropdown(dict(itertools.islice(professions.items(), 0, 24)), "Select a Profession (Heavy, Medium)", 0, 24)
        self.professions_2 = EmojiDropdown(dict(itertools.islice(professions.items(), 24, 36)), "Select a Profession (Light)", 0, 12)
        self.add_item(self.professions_1)
        self.add_item(self.professions_2)

        # Order dropdown
        self.order = SimpleDropdown(order_dict.keys(), "Order logs by...", 1, 1)
        self.add_item(self.order)

    @discord.ui.button(label="Name Filter", style=discord.ButtonStyle.gray, row=4)
    async def name(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(self.nameModal)

    @discord.ui.button(label="Boons Generated", style=discord.ButtonStyle.gray, row=4)
    async def boons(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(self.boonModal)

    @discord.ui.button(label="Search", style=discord.ButtonStyle.green, row=4)
    async def search(self, interaction: discord.Interaction, button: discord.ui.Button):
        # defer at the beginning to prevent failed interactions in case the db query takes too long
        await interaction.response.defer()

        # Get values
        selected_bosses: [str] = self.bosses.values
        selected_professions: [str] = self.professions_1.values
        selected_professions.extend(self.professions_2.values)

        # Default to Player.dps if nothing was selected
        selected_order = "Target DPS"
        if self.order.values:
            selected_order = self.order.values[0]

        # Add CM version of bosses
        for boss in selected_bosses.copy():  # Use a copy of the list to prevent infinite loop
            selected_bosses.append(f"{boss} CM")

        # Create a string to show the selected values
        filter_str = "__**Search Settings:**__\n"

        # Query DB
        query = db.query(Player).join(Log)
        if selected_bosses:
            query = query.filter(Log.fight_name.in_(selected_bosses))
            filter_str += f"**Bosses:** {', '.join(selected_bosses[:len(selected_bosses) // 2])}\n"
        if selected_professions:
            query = query.filter(Player.profession.in_(selected_professions))
            filter_str += f"**Professions:** {', '.join(selected_professions)}\n"
        if self.nameModal.account.value:
            query = query.filter(Player.account.ilike(f"%{self.nameModal.account.value}%"))
            filter_str += f"**Account:** {self.nameModal.account.value}\n"
        if self.nameModal.character.value:
            query = query.filter(Player.character.ilike(f"%{self.nameModal.character.value}%"))
            filter_str += f"**Character:** {self.nameModal.character.value}\n"
        if self.boonModal.boon.values:
            val = 60
            if self.boonModal.value.value.isdigit():
                if 0 <= int(self.boonModal.value.value) <= 100:
                    val = int(self.boonModal.value.value)
            buff_id = db.query(BuffMap).filter(BuffMap.name.ilike(f"{self.boonModal.boon.values[0]}")).first().id
            query = query.join(BuffGeneration).filter((BuffGeneration.buff == buff_id) & (BuffGeneration.uptime >= val))
            filter_str += f"**Minimum Boon Generation:** {val}% {self.boonModal.boon.values[0]}\n"

        query = query.order_by(order_dict[selected_order])
        filter_str += f"**Ordered by:** {selected_order}\n"

        if query.count() == 0:
            # Send ephemeral message if no logs were found
            await interaction.response.send_message(content="**:x: No logs found**\n" + filter_str, ephemeral=True)
            return

        # defer to prevent timeout
        await interaction.response.defer()

        embed = create_log_embed(query, selected_order)

        # Create paginated log view
        view = LogPaginationView(query, selected_order)
        view.message = await interaction.channel.send(content=f"{interaction.user.mention}\n{filter_str}", embed=embed, view=view)
        self.stop()


class NameFilter(discord.ui.Modal, title="Name Settings"):
    account = discord.ui.TextInput(label="Account Name", required=False)
    character = discord.ui.TextInput(label="Character Name", required=False)

    def __init__(self):
        super().__init__()

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer()

        # Show old values as placeholder if modal is opened again
        self.account.placeholder = self.account.value
        self.character.placeholder = self.character.value


class BoonFilter(discord.ui.Modal, title="Boon Settings (Generation Self)"):
    boon = EmojiDropdown(boons, "Select a Boon")
    value = discord.ui.TextInput(label="Minimum Generation (Default: 60)", required=False, placeholder="Number between 0 and 100")

    def __init__(self):
        super().__init__()

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer()


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
