# cogs/admin_ftp.py
import json
import re
import discord
from discord import app_commands
from discord.ext import commands

from utils.ftp_config import set_ftp_config, get_ftp_config, clear_ftp_config
from utils.settings import save_settings
from tracer.config import MAPS


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


# ---- map helpers (local, tiny) ---------------------------------------------
def _resolve_map_key(raw: str | None) -> str | None:
    if not raw:
        return None
    k = raw.strip().casefold()
    # direct key
    for key in MAPS.keys():
        if key.casefold() == k:
            return key
    # display name
    for key, cfg in MAPS.items():
        if str(cfg.get("name", key)).strip().casefold() == k:
            return key
    return None

def _map_display_name(key: str) -> str:
    cfg = MAPS.get(key) or {}
    return str(cfg.get("name", key))
# ---------------------------------------------------------------------------


def _sanitize_segment(s: str) -> str:
    """
    Safe-ish path segment: keep letters, numbers, dot, underscore, dash.
    Replace everything else with underscore. Ensure non-empty.
    """
    s = re.sub(r'[^A-Za-z0-9._-]+', '_', (s or '').strip())
    return s or "user"


class AdminFTP(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @app_commands.command(
        name="set_creds",
        description="Configure Nitrado token/API and (optional) FTP for ADM scanning (per guild)",
    )
    @admin_check()
    @app_commands.describe(
        nitrado_api_token="Nitrado HTTP API token (for active ADM follow)",
        nitrado_service_id="Nitrado service ID (numeric string)",
        host="FTP hostname or IP",
        username="FTP username",
        password="FTP password (use a dedicated account)",
        port="FTP port (default 21)",
        console="Console platform (chooses ADM folder + API prefix automatically)",
        interval_sec="Polling interval seconds (default 10)",
        map_choice="(Optional) Set the active map",
    )
    @app_commands.choices(
        console=[
            app_commands.Choice(name="Xbox", value="xbox"),
            app_commands.Choice(name="PlayStation", value="playstation"),
        ],
        map_choice=[app_commands.Choice(name=cfg.get("name", key), value=key) for key, cfg in MAPS.items()],
    )
    async def set_creds(
        self,
        interaction: discord.Interaction,
        nitrado_api_token: str | None = None,
        nitrado_service_id: str | None = None,
        host: str = "",
        username: str = "",
        password: str = "",
        port: int = 21,
        console: app_commands.Choice[str] = None,  # required
        interval_sec: int = 10,
        map_choice: app_commands.Choice[str] | None = None,
    ):
        """
        Save API/FTP config. 'console' determines:
          - FTP adm_dir: /dayzxb/config (Xbox) or /dayzps/config (PlayStation)
          - API prefix (stored as nitrado_log_folder_prefix): /games/{username}/noftp/{console}/config
        """
        gid = interaction.guild_id
        if not gid:
            return await interaction.response.send_message("❌ Guild-only command.", ephemeral=True)

        # Decide ADM dir from console
        console_val = (console.value if console else "").lower()
        if console_val not in ("xbox", "playstation"):
            return await interaction.response.send_message("❌ Choose a console: Xbox or PlayStation.", ephemeral=True)

        adm_dir = "/dayzxb/config" if console_val == "xbox" else "/dayzps/config"

        # Build extras and auto-generate prefix from username + console
        extras = {}
        if nitrado_api_token:
            extras["nitrado_api_token"] = nitrado_api_token.strip()
        if nitrado_service_id:
            extras["nitrado_service_id"] = str(nitrado_service_id).strip()

        user_seg = _sanitize_segment(username)
        extras["nitrado_log_folder_prefix"] = f"/games/{user_seg}/noftp/{console_val}/config"

        # Save FTP core (+ extras if supported)
        saved_extras = bool(extras)
        try:
            set_ftp_config(
                gid,
                host,
                username,
                password,
                port,
                adm_dir,
                interval_sec,
                **extras,  # type: ignore[arg-type]
            )
        except TypeError:
            # Older helper that doesn't accept extras:
            set_ftp_config(gid, host, username, password, port, adm_dir, interval_sec)
            if extras:
                saved_extras = False

        # Optional: set active map into the normal settings store
        map_line = ""
        if map_choice:
            chosen_key = _resolve_map_key(map_choice.value) or map_choice.value
            save_settings(gid, {"active_map": chosen_key})
            map_line = f"\nActive map: **{_map_display_name(chosen_key)}**"

        # Notify the core to (re)start the poller for this guild.
        interaction.client.dispatch("ftp_config_updated", gid)

        # Build a user message with secrets redacted.
        cfg = get_ftp_config(gid) or {}
        redacted = _redact_config(cfg)

        note = ""
        if not saved_extras:
            note = (
                "\n\n⚠️ **Note:** Your utils.ftp_config.set_ftp_config() doesn’t accept API fields. "
                "FTP settings were saved, but API fields were ignored by that helper. "
                "Extend set_ftp_config/get_ftp_config to persist `nitrado_api_token`, "
                "`nitrado_service_id`, and the auto-generated `nitrado_log_folder_prefix`."
            )

        # Friendly header summarizing derived bits
        summary = (
            f"Console: **{console.name}**\n"
            f"ADM dir: `{adm_dir}`\n"
            f"API prefix: `{'/games/'+user_seg+'/noftp/'+console_val+'/config'}`"
        )

        await interaction.response.send_message(
            content=f"✅ Config saved for this guild.{map_line}\n{summary}\n```json\n{json.dumps(redacted, indent=2)}\n```{note}",
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
