import discord


class ConfirmationView(discord.ui.View):
    def __init__(self, func, *args):
        super().__init__()
        self.func = func
        self.args = args

    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.green)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.func(interaction, *self.args)