# utils/live_pulse.py
import asyncio
import logging
from typing import Dict, Tuple, Optional

import discord
from utils.settings import load_settings
from tracer.tracker import subscribe_to_points

logger = logging.getLogger(__name__)

# key = (guild_id, gamertag_lower)
_active: Dict[Tuple[int, str], Dict] = {}
_bot: Optional[discord.Client] = None

def init(bot: discord.Client):
    """Call once in setup to allow message edits."""
    global _bot
    _bot = bot
    # subscribe only once
    subscribe_to_points(_on_point)
    logger.debug("live_pulse initialized and subscribed to tracker events.")

def _fmt_coord(x, z):
    return f"{int(x)},{int(z)}"

async def _ensure_message(guild_id: int, gamertag: str) -> Optional[discord.Message]:
    if _bot is None:
        return None
    key = (guild_id, gamertag.lower())
    info = _active.get(key)

    # find per-guild bounty channel from settings
    s = load_settings(guild_id)
    ch_id = s.get("bounty_channel_id")
    ch = _bot.get_channel(int(ch_id)) if ch_id else None
    if not isinstance(ch, discord.TextChannel):
        logger.debug(f"[Guild {guild_id}] Bounty channel not set or not a text channel.")
        return None

    if info and "message_id" in info:
        try:
            msg = await ch.fetch_message(info["message_id"])
            return msg
        except Exception:
            # fetch failed, will create a fresh message below
            pass

    # create new message
    try:
        embed = discord.Embed(
            title="🎯 Bounty Tracking",
            description=f"Target: `{gamertag}`\nStatus: **LIVE**",
            color=discord.Color.orange()
        )
        msg = await ch.send(embed=embed)
        _active[key] = {"message_id": msg.id, "channel_id": ch.id}
        logger.info(f"[Guild {guild_id}] Created live pulse message for {gamertag} in #{ch.name}.")
        return msg
    except Exception as e:
        logger.error(f"[Guild {guild_id}] Failed to create live pulse message: {e}", exc_info=True)
        return None

async def _on_point(guild_id, gamertag, point: dict):
    """Called by tracker whenever a point is appended."""
    if not guild_id:
        return
    key = (guild_id, gamertag.lower())
    # Only pulse if this target is marked active
    if key not in _active:
        return

    msg = await _ensure_message(guild_id, gamertag)
    if not msg:
        return

    try:
        x, z = point["x"], point["z"]
        t = point.get("ts", "")
        embed = discord.Embed(
            title="🎯 Bounty Tracking",
            description=f"Target: `{gamertag}`\nStatus: **LIVE**",
            color=discord.Color.orange()
        )
        embed.add_field(name="Current pos", value=_fmt_coord(x, z), inline=True)
        if t:
            embed.set_footer(text=f"Last update: {t}")
        await msg.edit(embed=embed)
        logger.debug(f"[Guild {guild_id}] Live pulse updated for {gamertag} -> ({int(x)},{int(z)})")
    except Exception as e:
        logger.error(f"[Guild {guild_id}] Failed to edit live pulse for {gamertag}: {e}", exc_info=True)

def start_for(guild_id: int, gamertag: str):
    """Begin pulsing for this target (creates/claims the message on next point)."""
    key = (guild_id, gamertag.lower())
    if key not in _active:
        _active[key] = {}
        logger.info(f"[Guild {guild_id}] Live pulse START for {gamertag}")

def stop_for(guild_id: int, gamertag: str):
    """Stop pulsing (message remains, but no more edits)."""
    key = (guild_id, gamertag.lower())
    if key in _active:
        _active.pop(key, None)
        logger.info(f"[Guild {guild_id}] Live pulse STOP for {gamertag}")

def stop_all_for_guild(guild_id: int):
    to_remove = [k for k in _active.keys() if k[0] == guild_id]
    for k in to_remove:
        _active.pop(k, None)
    if to_remove:
        logger.info(f"[Guild {guild_id}] Live pulse STOP for all ({len(to_remove)} target(s))")
