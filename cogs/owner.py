import discord
from discord.ext import commands
from discord import app_commands, Interaction
import cogs.logmanager.choices as choices
import typing


class Owner(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @commands.group(name="owner", hidden=True)
    @commands.is_owner()
    async def owner(self, ctx):
        if ctx.invoked_subcommand is None:
            await ctx.send_help("owner")
            print(f"Unknown subcommand \"{ctx.message.content}\" by {ctx.author}.Sent help page")

    @owner.group(name="cog")
    @commands.is_owner()
    async def cog(self, ctx):
        if ctx.invoked_subcommand is None:
            await ctx.send_help("cog")
            print(f"Unknown subcommand \"{ctx.message.content}\" by {ctx.author}.Sent help page")

    @cog.command(name="load")
    @commands.is_owner()
    async def load_cog(self, ctx, cog):
        try:
            await self.bot.load_extension("cogs." + cog)
            await ctx.send(f"**`SUCCESS:`** Loaded {cog}")
        except Exception as e:
            await ctx.send(f"**`ERROR:`** Failed to load {cog}: {e}")

    @cog.command(name="unload")
    @commands.is_owner()
    async def unload_cog(self, ctx, cog):
        try:
            await self.bot.unload_extension("cogs." + cog)
            await ctx.send(f"**`SUCCESS:`** Unloaded {cog}")
        except Exception as e:
            await ctx.send(f"**`ERROR:`** Failed to unload {cog}: {e}")

    @cog.command(name="reload")
    @commands.is_owner()
    async def reload_cog(self, ctx, cog):
        try:
            await self.bot.unload_extension("cogs." + cog)
            await self.bot.load_extension("cogs." + cog)
            await ctx.send(f"**`SUCCESS:`** Reloaded {cog}")
        except Exception as e:
            await ctx.send(f"**`ERROR:`** Failed to reload {cog}: {e}")

    @owner.group(name="guild")
    @commands.is_owner()
    async def guild(self, ctx):
        if ctx.invoked_subcommand is None:
            await ctx.send_help("owner guild")
            print(f"Unknown subcommand \"{ctx.message.content}\" by {ctx.author}.Sent help page")

    @guild.command(name="list", help="List all Guilds")
    @commands.is_owner()
    async def list_guilds(self, ctx):
        message = f"```{'ID':^18}|{'Name':^20}|{'Members':^7}|{'Owner':^15}"
        message += f"\n{'-'*65:^65}"
        for guild in self.bot.guilds:
            message += f"\n{str(guild.id):^18}|{str(guild.name):^20}|{str(guild.member_count):^7}|{str(guild.owner):^15}"
        message += "```"
        await ctx.send(message)

    @guild.command(name="leave", help="Leave a Guild")
    @commands.is_owner()
    async def leave_guild(self, ctx, guild: int):
        guild = self.bot.get_guild(guild)
        if guild:
            await guild.leave()
            print(f"Left guild {guild.name}")
            # Don't try to send leave message if we left that guild
            if not ctx.guild.id == guild.id:
                await ctx.send(f"**`SUCCESS:`** Left {guild.name}")
        else:
            await ctx.send(f"**`ERROR:`** No guild with that ID found")

    @owner.command("sync")
    @commands.is_owner()
    async def sync(self, ctx: commands.Context, sync_global: typing.Optional[bool] = False) -> None:
        if sync_global:
            await self.bot.tree.sync()
            await ctx.send("Synced commands globally")
        else:
            self.bot.tree.copy_global_to(guild=ctx.guild)
            await self.bot.tree.sync(guild=ctx.guild)
            await ctx.send("Synced commands to this guild")

    @app_commands.guilds(688413515366531139)
    @app_commands.command(name="activity", description="Set the activity of the bot")
    async def activity(self, interaction: Interaction, activity: choices.activity, status: str):
        # Make sure user is bot owner
        if not await self.bot.is_owner(interaction.user):
            await interaction.response.send_message(content=f"Only the bot owner can use this command", ephemeral=True)
            return

        target_activity = None
        match activity:
            case "Playing":
                target_activity = discord.Activity(type=discord.ActivityType.playing, name=status)
            case "Listening":
                target_activity = discord.Activity(type=discord.ActivityType.listening, name=status)
            case "Watching":
                target_activity = discord.Activity(type=discord.ActivityType.watching, name=status)

        if target_activity:
            await self.bot.change_presence(activity=target_activity)
            await interaction.response.send_message(content="Set activity", ephemeral=True)
            return
        await interaction.response.send_message(content="Invalid activity", ephemeral=True)


async def setup(bot):
    await bot.add_cog(Owner(bot))
