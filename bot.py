# bot.py (snippet)
import os, asyncio, discord
from discord.ext import commands

INTENTS = discord.Intents.default()
BOT = commands.Bot(command_prefix="!", intents=INTENTS)

@BOT.event
async def on_ready():
    print(f"Logged in as {BOT.user} ({BOT.user.id})")

# bot.py (additions)
async def main():
    async with BOT:
        await BOT.load_extension("cogs.admin_assign")
        await BOT.load_extension("cogs.admin_links")
        await BOT.load_extension("cogs.link")
        await BOT.load_extension("cogs.trace")
        await BOT.load_extension("cogs.bounty")
        # (and any other cogs like cogs.track)
        await BOT.start(os.environ["DISCORD_TOKEN"])

if __name__ == "__main__":
    asyncio.run(main())
