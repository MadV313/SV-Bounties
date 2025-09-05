# utils/live_pulse.py
import asyncio
from datetime import datetime
from typing import Dict, Tuple, Optional

import discord
from utils.settings import load_settings
from tracer.tracker import subscribe_to_points

# key = (guild_id, gamertag_lower)
_active: Dict[Tuple[int, str], Dict] = {}
_bot: Optional[discord.Client] = None

def init(bot: discord.Client):
    """Call once in setup to allow message edits."""
    global _bot
    _bot = bot
    # subscribe only once
    subscribe_to_points(_on_point)

def _fmt_coord(x, z):
    return f"{int(x)},{int(z)}"

async def _ensure_message(guild_id: int, gamertag: str) -> Optional[discord.Message]:
    if _bot is None:
        return None
    key = (guild_id, gamertag.lower())
    info = _active.get(key)
    ch = None

    # find channel from settings
    s = load_settings()
    ch_id = s.get("bounty_channel_id")
    if ch_id:
        ch = _bot.get_channel(int(ch_id))
    if not isinstance(ch, discord.TextChannel):
        return None

    if info and "message_id" in info:
        try:
            msg = await ch.fetch_message(info["message_id"])
            return msg
        except Exception:
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
        return msg
    except Exception:
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
    except Exception:
        pass

def start_for(guild_id: int, gamertag: str):
    """Begin pulsing for this target (creates/claims the message on next point)."""
    key = (guild_id, gamertag.lower())
    if key not in _active:
        _active[key] = {}

def stop_for(guild_id: int, gamertag: str):
    """Stop pulsing (message remains, but no more edits)."""
    key = (guild_id, gamertag.lower())
    _active.pop(key, None)

def stop_all_for_guild(guild_id: int):
    for k in list(_active.keys()):
        if k[0] == guild_id:
            _active.pop(k, None)
