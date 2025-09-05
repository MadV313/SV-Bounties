# cogs/admin_assign.py
import discord
from discord import app_commands
from discord.ext import commands
from utils.settings import load_settings, save_settings
from tracer.config import MAPS

def admin_check():
    # allow server admins or anyone with Manage Guild; customize if you prefer role IDs
    def pred(i: discord.Interaction):
        perms = i.user.guild_permissions if hasattr(i.user, "guild_permissions") else None
        return bool(perms and (perms.administrator or perms.manage_guild))
    return app_commands.check(lambda i: pred(i))

class AdminAssign(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(name="assignchannel", description="Set the SV Bounties channel and pick the active map")
    @app_commands.describe(channel="Channel to use for SV bounties", map_choice="Which map should be active?")
    @app_commands.choices(
        map_choice=[app_commands.Choice(name=cfg["name"], value=key) for key, cfg in MAPS.items()]
    )
    @admin_check()
    async def assignchannel(self, interaction: discord.Interaction,
                            channel: discord.TextChannel,
                            map_choice: app_commands.Choice[str]):
        settings = save_settings({
            "bounty_channel_id": channel.id,
            "active_map": map_choice.value.lower()
        })
        await interaction.response.send_message(
            f"✅ Saved.\n• Bounty channel: {channel.mention}\n• Active map: **{MAPS[settings['active_map']]['name']}**",
            ephemeral=True
        )

    @app_commands.command(name="setmap", description="Switch the active map")
    @app_commands.describe(map_choice="Map to activate")
    @app_commands.choices(
        map_choice=[app_commands.Choice(name=cfg["name"], value=key) for key, cfg in MAPS.items()]
    )
    @admin_check()
    async def setmap(self, interaction: discord.Interaction, map_choice: app_commands.Choice[str]):
        settings = save_settings({"active_map": map_choice.value.lower()})
        await interaction.response.send_message(
            f"🗺️ Active map set to **{MAPS[settings['active_map']]['name']}**.",
            ephemeral=True
        )

    @app_commands.command(name="settings", description="Show current bot settings")
    @admin_check()
    async def settings(self, interaction: discord.Interaction):
        s = load_settings()
        ch = f"<#{s['bounty_channel_id']}>" if s.get("bounty_channel_id") else "*not set*"
        mp = MAPS.get(s.get("active_map") or "", {}).get("name", "*unknown*")
        await interaction.response.send_message(
            f"**Current Settings**\n• Bounty channel: {ch}\n• Active map: **{mp}**",
            ephemeral=True
        )

async def setup(bot: commands.Bot):
    await bot.add_cog(AdminAssign(bot))
