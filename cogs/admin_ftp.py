# cogs/admin_ftp.py
import json
import discord
from discord import app_commands
from discord.ext import commands

from utils.ftp_config import set_ftp_config, get_ftp_config, clear_ftp_config


def admin_check():
    def pred(i: discord.Interaction):
        perms = getattr(i.user, "guild_permissions", None)
        return bool(perms and (perms.administrator or perms.manage_guild))
    return app_commands.check(lambda i: pred(i))


def _redact_config(d: dict) -> dict:
    """Return a shallow-copy with common secret fields redacted."""
    redacted = dict(d or {})
    for k in list(redacted.keys()):
        lk = str(k).lower()
        if "password" in lk or "token" in lk:
            redacted[k] = "••••••••"
    return redacted


class AdminFTP(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @app_commands.command(
        name="setftp",
        description="Configure FTP (and optional Nitrado API) for ADM scanning (per guild)",
    )
    @admin_check()
    @app_commands.describe(
        host="FTP hostname or IP",
        username="FTP username",
        password="FTP password (use a dedicated account)",
        port="FTP port (default 21)",
        adm_dir="Path to ADM logs directory (e.g., /adm/ or /dayzxb/config)",
        interval_sec="Polling interval seconds (default 10)",
        nitrado_api_token="(Optional) Nitrado HTTP API token for active ADM follow",
        nitrado_service_id="(Optional) Nitrado service ID (numeric string)",
        nitrado_log_folder_prefix="(Optional) Log directory for API (e.g., /games/.../dayzxb/config)",
    )
    async def setftp(
        self,
        interaction: discord.Interaction,
        host: str,
        username: str,
        password: str,
        port: int = 21,
        adm_dir: str = "/",
        interval_sec: int = 10,
        nitrado_api_token: str | None = None,
        nitrado_service_id: str | None = None,
        nitrado_log_folder_prefix: str | None = None,
    ):
        """Save FTP config and (optionally) Nitrado API details into the same per-guild config."""
        extras = {}
        if nitrado_api_token:
            extras["nitrado_api_token"] = nitrado_api_token.strip()
        if nitrado_service_id:
            extras["nitrado_service_id"] = str(nitrado_service_id).strip()
        if nitrado_log_folder_prefix:
            extras["nitrado_log_folder_prefix"] = nitrado_log_folder_prefix.strip()

        # Try saving with extras first (if set_ftp_config supports **kwargs).
        saved_extras = bool(extras)
        try:
            set_ftp_config(
                interaction.guild_id,
                host,
                username,
                password,
                port,
                adm_dir,
                interval_sec,
                **extras,  # type: ignore[arg-type]
            )
        except TypeError:
            # Older implementations may not support **extras — fall back to core FTP fields only.
            set_ftp_config(interaction.guild_id, host, username, password, port, adm_dir, interval_sec)
            if extras:
                saved_extras = False  # We couldn't persist the API fields via this helper.

        # Notify the core to (re)start the poller for this guild.
        interaction.client.dispatch("ftp_config_updated", interaction.guild_id)

        # Build a user message with secrets redacted.
        cfg = get_ftp_config(interaction.guild_id) or {}
        redacted = _redact_config(cfg)

        # Add a small note if extras were provided but likely not saved.
        note = ""
        if extras and not saved_extras:
            note = (
                "\n\n⚠️ **Note:** Your utils.ftp_config.set_ftp_config() doesn’t accept API fields. "
                "The FTP settings were saved, but API fields were ignored by that helper. "
                "If you want the active-ADM API fallback, extend set_ftp_config/get_ftp_config "
                "to persist `nitrado_api_token`, `nitrado_service_id`, and `nitrado_log_folder_prefix`."
            )

        await interaction.response.send_message(
            content=f"✅ Config saved for this guild.\n```json\n{json.dumps(redacted, indent=2)}\n```{note}",
            ephemeral=True,
        )

    @app_commands.command(name="showftp", description="Show the current FTP/API config (secrets redacted)")
    @admin_check()
    async def showftp(self, interaction: discord.Interaction):
        cfg = get_ftp_config(interaction.guild_id)
        if not cfg:
            return await interaction.response.send_message("ℹ️ No FTP config set.", ephemeral=True)
        redacted = _redact_config(cfg)
        await interaction.response.send_message(
            f"```json\n{json.dumps(redacted, indent=2)}\n```", ephemeral=True
        )

    @app_commands.command(name="clearftp", description="Clear saved FTP/API configuration for this guild")
    @admin_check()
    async def clearftp(self, interaction: discord.Interaction):
        clear_ftp_config(interaction.guild_id)
        interaction.client.dispatch("ftp_config_updated", interaction.guild_id)
        await interaction.response.send_message("🧹 FTP/API config cleared.", ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(AdminFTP(bot))
