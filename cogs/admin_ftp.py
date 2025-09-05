# cogs/admin_ftp.py
import discord
from discord import app_commands
from discord.ext import commands
from utils.ftp_config import set_ftp_config, get_ftp_config, clear_ftp_config

def admin_check():
    def pred(i: discord.Interaction):
        perms = getattr(i.user, "guild_permissions", None)
        return bool(perms and (perms.administrator or perms.manage_guild))
    return app_commands.check(lambda i: pred(i))

class AdminFTP(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @app_commands.command(name="setftp", description="Configure FTP credentials for ADM scanning (per guild)")
    @admin_check()
    @app_commands.describe(
        host="FTP hostname or IP",
        username="FTP username",
        password="FTP password (stored locally, use a dedicated account)",
        port="Port (default 21)",
        adm_dir="Path to ADM logs directory (e.g., /adm/)",
        interval_sec="Polling interval seconds (default 10)"
    )
    async def setftp(self, interaction: discord.Interaction,
                     host: str, username: str, password: str,
                     port: int = 21, adm_dir: str = "/", interval_sec: int = 10):
        set_ftp_config(interaction.guild_id, host, username, password, port, adm_dir, interval_sec)
        # Let the core bot know it can restart this guild's poller (optional handler in bot.py)
        interaction.client.dispatch("ftp_config_updated", interaction.guild_id)
        await interaction.response.send_message("✅ FTP credentials saved (per-guild).", ephemeral=True)

    @app_commands.command(name="showftp", description="Show the current FTP config (password redacted)")
    @admin_check()
    async def showftp(self, interaction: discord.Interaction):
        cfg = get_ftp_config(interaction.guild_id)
        if not cfg:
            return await interaction.response.send_message("ℹ️ No FTP config set.", ephemeral=True)
        redacted = {**cfg, "password": "••••••••"}
        await interaction.response.send_message(f"```json\n{redacted}\n```", ephemeral=True)

    @app_commands.command(name="clearftp", description="Clear saved FTP credentials for this guild")
    @admin_check()
    async def clearftp(self, interaction: discord.Interaction):
        clear_ftp_config(interaction.guild_id)
        # Notify core to stop the poller if running (optional handler in bot.py)
        interaction.client.dispatch("ftp_config_updated", interaction.guild_id)
        await interaction.response.send_message("🧹 FTP config cleared.", ephemeral=True)

async def setup(bot: commands.Bot):
    await bot.add_cog(AdminFTP(bot))
