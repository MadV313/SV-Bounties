# cogs/admin_assign.py
import discord
from discord import app_commands
from discord.ext import commands
from utils.settings import load_settings, save_settings
from tracer.config import MAPS

def admin_check():
    def pred(i: discord.Interaction):
        perms = getattr(i.user, "guild_permissions", None)
        return bool(perms and (perms.administrator or perms.manage_guild))
    return app_commands.check(lambda i: pred(i))

class AdminAssign(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(
        name="setchannels",
        description="Set the PRIVATE admin channel (trace output) and PUBLIC bounty channel."
    )
    @admin_check()
    @app_commands.describe(
        admin_channel="Private admin channel (trace output, internal logs)",
        bounty_channel="Public channel for bounty posts"
    )
    async def setchannels(self, interaction: discord.Interaction,
                          admin_channel: discord.TextChannel,
                          bounty_channel: discord.TextChannel):
        s = save_settings({
            "admin_channel_id": admin_channel.id,
            "bounty_channel_id": bounty_channel.id
        })
        await interaction.response.send_message(
            f"✅ Saved.\n• Admin channel: {admin_channel.mention}\n• Bounty channel: {bounty_channel.mention}",
            ephemeral=True
        )

    @app_commands.command(name="setmap", description="Switch the active map")
    @app_commands.describe(map_choice="Map to activate")
    @app_commands.choices(
        map_choice=[app_commands.Choice(name=cfg["name"], value=key) for key, cfg in MAPS.items()]
    )
    @admin_check()
    async def setmap(self, interaction: discord.Interaction, map_choice: app_commands.Choice[str]):
        s = save_settings({"active_map": map_choice.value.lower()})
        await interaction.response.send_message(
            f"🗺️ Active map set to **{MAPS[s['active_map']]['name']}**.",
            ephemeral=True
        )

    @app_commands.command(name="settings", description="Show current bot settings")
    @admin_check()
    async def settings(self, interaction: discord.Interaction):
        s = load_settings()
        admin_ch = f"<#{s['admin_channel_id']}>" if s.get("admin_channel_id") else "*not set*"
        bounty_ch = f"<#{s['bounty_channel_id']}>" if s.get("bounty_channel_id") else "*not set*"
        mp = MAPS.get(s.get("active_map") or "", {}).get("name", "*unknown*")
        await interaction.response.send_message(
            f"**Current Settings**\n• Admin channel: {admin_ch}\n• Bounty channel: {bounty_ch}\n• Active map: **{mp}**",
            ephemeral=True
        )

async def setup(bot: commands.Bot):
    await bot.add_cog(AdminAssign(bot))
