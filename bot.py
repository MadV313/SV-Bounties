# bot.py
import os
import asyncio
import logging
import discord
from discord.ext import commands

from utils import live_pulse
from utils.ftp_config import get_ftp_config
from tracer.log_fetcher import poll_guild
from tracer.scanner import scan_adm_line

# --- Logging ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Discord setup ---
INTENTS = discord.Intents.default()
BOT = commands.Bot(command_prefix="!", intents=INTENTS)

# --- Poller bookkeeping per guild ---
_poll_stops: dict[int, asyncio.Event] = {}
_poll_tasks: dict[int, asyncio.Task] = {}

async def line_callback(guild_id: int, line: str, source_ref: str, ts):
    """Pass each ADM line to the scanner."""
    await scan_adm_line(guild_id, line, source_ref, ts)

async def start_poll_for_guild(guild_id: int):
    """Start a poller for a single guild if FTP config exists."""
    if guild_id in _poll_tasks and not _poll_tasks[guild_id].done():
        return  # already running

    cfg = get_ftp_config(guild_id)
    if not cfg:
        logger.info(f"[Guild {guild_id}] No FTP config; poller not started.")
        return

    stop_event = asyncio.Event()
    _poll_stops[guild_id] = stop_event

    async def _runner():
        try:
            await poll_guild(guild_id, line_callback, stop_event)
        except Exception as e:
            logger.error(f"[Guild {guild_id}] poller crashed: {e}", exc_info=True)

    task = asyncio.create_task(_runner(), name=f"poller:{guild_id}")
    _poll_tasks[guild_id] = task
    logger.info(f"[Guild {guild_id}] FTP poller started.")

async def stop_poll_for_guild(guild_id: int):
    """Stop a running poller for a guild."""
    ev = _poll_stops.get(guild_id)
    if ev and not ev.is_set():
        ev.set()
    task = _poll_tasks.get(guild_id)
    if task and not task.done():
        try:
            await asyncio.wait_for(task, timeout=2.0)
        except asyncio.TimeoutError:
            task.cancel()
    _poll_stops.pop(guild_id, None)
    _poll_tasks.pop(guild_id, None)
    logger.info(f"[Guild {guild_id}] FTP poller stopped.")

async def start_polls():
    """Start pollers for all guilds with configs."""
    await BOT.wait_until_ready()
    for g in BOT.guilds:
        await start_poll_for_guild(g.id)

@BOT.event
async def on_ready():
    logger.info(f"Logged in as {BOT.user} ({BOT.user.id})")
    try:
        synced = await BOT.tree.sync()
        logger.info(f"Synced {len(synced)} command(s).")
    except Exception as e:
        logger.error(f"Slash sync failed: {e}", exc_info=True)
    asyncio.create_task(start_polls())

@BOT.event
async def on_guild_join(guild: discord.Guild):
    logger.info(f"Joined guild {guild.name} ({guild.id})")
    await start_poll_for_guild(guild.id)

@BOT.event
async def on_guild_remove(guild: discord.Guild):
    logger.info(f"Removed from guild {guild.name} ({guild.id})")
    await stop_poll_for_guild(guild.id)

async def main():
    async with BOT:
        # Allow live pulse to edit messages & subscribe to tracker events
        live_pulse.init(BOT)

        # Load cogs
        await BOT.load_extension("cogs.admin_assign")
        await BOT.load_extension("cogs.admin_ftp")
        await BOT.load_extension("cogs.admin_links")
        await BOT.load_extension("cogs.link")
        await BOT.load_extension("cogs.trace")
        await BOT.load_extension("cogs.bounty")
        await BOT.load_extension("cogs.admin_misc")

        token = os.environ.get("DISCORD_TOKEN")
        if not token:
            raise RuntimeError("DISCORD_TOKEN env var not set.")
        await BOT.start(token)

if __name__ == "__main__":
    asyncio.run(main())
