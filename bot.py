# bot.py (additions)
import asyncio, discord, os
from discord.ext import commands

import logging
logging.basicConfig(level=logging.INFO)

from utils.ftp_config import get_ftp_config
from tracer.log_fetcher import poll_guild
from tracer.scanner import scan_adm_line
from utils import live_pulse

INTENTS = discord.Intents.default()
BOT = commands.Bot(command_prefix="!", intents=discord.Intents.default())

# Keep per-guild stop events so we can restart polls if needed
poll_stops: dict[int, asyncio.Event] = {}

async def line_callback(guild_id: int, line: str, source_ref: str, ts):
    await scan_adm_line(guild_id, line, source_ref, ts)

async def start_polls():
    await BOT.wait_until_ready()
    for guild in BOT.guilds:
        cfg = get_ftp_config(guild.id)
        if not cfg or guild.id in poll_stops:
            continue
        stop_event = asyncio.Event()
        poll_stops[guild.id] = stop_event
        BOT.loop.create_task(poll_guild(guild.id, line_callback, stop_event))

@BOT.event
async def on_ready():
    print(f"Logged in as {BOT.user} ({BOT.user.id})")
    try:
        synced = await BOT.tree.sync()
        print(f"Synced {len(synced)} command(s).")
    except Exception as e:
        print(f"Slash sync failed: {e}")
    BOT.loop.create_task(start_polls())  # keep your poll starter here if you had it

async def main():
    async with BOT:
        live_pulse.init(BOT)  # <-- allow pulse manager to edit messages & subscribe
        await BOT.load_extension("cogs.admin_assign")
        await BOT.load_extension("cogs.admin_ftp")
        await BOT.load_extension("cogs.admin_links")
        await BOT.load_extension("cogs.link")
        await BOT.load_extension("cogs.trace")
        await BOT.load_extension("cogs.bounty")
        await BOT.load_extension("cogs.admin_misc")
        await BOT.start(os.environ["DISCORD_TOKEN"])

if __name__ == "__main__":
    asyncio.run(main())
