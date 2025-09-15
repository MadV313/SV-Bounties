# cogs/admin_assign.py
import discord
from discord import app_commands
from discord.ext import commands
from typing import Optional

from utils.settings import load_settings, save_settings
from tracer.config import MAPS


# -------- admin gate (local) ----------
def admin_check():
    def pred(i: discord.Interaction):
        perms = getattr(i.user, "guild_permissions", None)
        return bool(perms and (perms.administrator or perms.manage_guild))
    return app_commands.check(lambda i: pred(i))


# ---------- map helpers (canonical keys) ----------
def _resolve_map_key(value: Optional[str]) -> Optional[str]:
    """
    Accept a map key (any case) or display name and return the canonical MAPS key.
    """
    if not value:
        return None
    val = value.strip()

    # direct key (case-insensitive)
    for k in MAPS.keys():
        if k.casefold() == val.casefold():
            return k

    # display name match (case-insensitive)
    for k, cfg in MAPS.items():
        name = str(cfg.get("name", "")).strip()
        if name and name.casefold() == val.casefold():
            return k
    return None


def _map_display_name(key: Optional[str]) -> str:
    if not key:
        return "*unknown*"
    cfg = MAPS.get(key)
    return cfg.get("name", key) if cfg else key
# --------------------------------------------------


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
    async def setchannels(
        self,
        interaction: discord.Interaction,
        admin_channel: discord.TextChannel,
        bounty_channel: discord.TextChannel
    ):
        gid = interaction.guild_id
        save_settings(gid, {
            "admin_channel_id": admin_channel.id,
            "bounty_channel_id": bounty_channel.id
        })
        await interaction.response.send_message(
            f"✅ Saved.\n• Admin channel: {admin_channel.mention}\n• Bounty channel: {bounty_channel.mention}",
            ephemeral=True
        )

    @app_commands.command(name="settings", description="Show current bot settings")
    @admin_check()
    async def settings(self, interaction: discord.Interaction):
        gid = interaction.guild_id
        s = load_settings(gid) or {}

        admin_ch = f"<#{s['admin_channel_id']}>" if s.get("admin_channel_id") else "*not set*"
        bounty_ch = f"<#{s['bounty_channel_id']}>" if s.get("bounty_channel_id") else "*not set*"

        # Coerce stored map value to canonical key for display
        raw_map_val = s.get("active_map")
        map_key = _resolve_map_key(raw_map_val) or raw_map_val
        map_name = _map_display_name(map_key)

        await interaction.response.send_message(
            f"**Current Settings**\n"
            f"• Admin channel: {admin_ch}\n"
            f"• Bounty channel: {bounty_ch}\n"
            f"• Active map: **{map_name}**",
            ephemeral=True
        )


async def setup(bot: commands.Bot):
    await bot.add_cog(AdminAssign(bot))
