# cogs/admin_misc.py
import discord
from discord import app_commands
from discord.ext import commands
from utils.settings import load_settings
from tracer.config import MAPS

def admin_check():
    def pred(i: discord.Interaction):
        perms = getattr(i.user, "guild_permissions", None)
        return bool(perms and (perms.administrator or perms.manage_guild))
    return app_commands.check(lambda i: pred(i))

class AdminMisc(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @app_commands.command(name="sync", description="Force sync slash commands (admin only)")
    @admin_check()
    async def sync(self, interaction: discord.Interaction):
        cmds = await interaction.client.tree.sync()
        await interaction.response.send_message(f"✅ Synced {len(cmds)} command(s).", ephemeral=True)

    @app_commands.command(name="settings_here", description="Show settings for this guild")
    @admin_check()
    async def settings_here(self, interaction: discord.Interaction):
        gid = interaction.guild_id
        s = load_settings(gid)
        admin_ch = f"<#{s['admin_channel_id']}>" if s.get("admin_channel_id") else "*not set*"
        bounty_ch = f"<#{s['bounty_channel_id']}>" if s.get("bounty_channel_id") else "*not set*"
        mp = MAPS.get(s.get("active_map") or "", {}).get("name", "*unknown*")
        ext = s.get("external_links_path") or "*not set*"

        await interaction.response.send_message(
            f"**Current Settings for {interaction.guild.name}**\n"
            f"• Admin channel: {admin_ch}\n"
            f"• Bounty channel: {bounty_ch}\n"
            f"• Active map: **{mp}**\n"
            f"• External links: {ext}",
            ephemeral=True
        )

async def setup(bot):
    await bot.add_cog(AdminMisc(bot))
