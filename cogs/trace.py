# cogs/trace.py
import discord
from discord import app_commands
from discord.ext import commands
from datetime import datetime, timedelta, timezone

from utils.linking import resolve_from_any
from tracer.tracker import load_track
from tracer.map_renderer import render_track_png

def _parse_dt(s: str) -> datetime | None:
    if not s: return None
    s = s.strip()
    # Try several common formats
    fmts = ["%Y-%m-%d %H:%M", "%Y-%m-%d", "%m/%d/%Y %H:%M", "%m/%d/%Y"]
    for f in fmts:
        try:
            dt = datetime.strptime(s, f).replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            continue
    return None

class TraceCog(commands.Cog):
    def __init__(self, bot): self.bot = bot

    @app_commands.command(name="trace", description="Render a player's movement path")
    @app_commands.describe(
        user="Discord user (optional if you provide gamertag)",
        gamertag="Gamertag (optional if you select a user)",
        start="Start datetime (e.g., 2025-09-04 13:30)",
        end="End datetime (optional). Default: now",
        window_hours="Alternative to start/end. Default 24h."
    )
    async def trace(self,
        interaction: discord.Interaction,
        user: discord.User | None = None,
        gamertag: str | None = None,
        start: str | None = None,
        end: str | None = None,
        window_hours: int | None = 24
    ):
        await interaction.response.defer(thinking=True)

        # Resolve identity
        did = str(user.id) if user else None
        if not (did or gamertag):
            # try self
            did = str(interaction.user.id)
        resolved_did, resolved_tag = resolve_from_any(discord_id=did, gamertag=gamertag)
        if not resolved_tag:
            return await interaction.followup.send("❌ Couldn’t resolve that player. Use `/link` first or provide a gamertag.")

        # Time window
        dt_start = _parse_dt(start) if start else None
        dt_end = _parse_dt(end) if end else datetime.now(timezone.utc)

        # If explicit start/end provided, ignore window_hours, otherwise use window_hours from now
        if dt_start:
            if not dt_end or dt_end <= dt_start:
                dt_end = dt_start + timedelta(hours=1)  # 1h default window if only start given
            window_hours = None
        else:
            # last N hours
            if not window_hours:
                window_hours = 24

        # Load points
        pid, doc = load_track(resolved_tag, window_hours=window_hours, max_points=1000)
        if not doc or not doc.get("points"):
            return await interaction.followup.send(f"ℹ️ No track points found for `{resolved_tag}` in that window.")

        # Filter by explicit range if provided
        if dt_start:
            pts = []
            for p in doc["points"]:
                try:
                    ts = datetime.fromisoformat(p["ts"].replace("Z","+00:00"))
                except Exception:
                    continue
                if ts >= dt_start and ts <= dt_end:
                    pts.append(p)
            doc = {**doc, "points": pts}
            if not pts:
                return await interaction.followup.send(f"ℹ️ No points for `{resolved_tag}` in that time range.")

        img = render_track_png(doc)
        file = discord.File(img, filename=f"trace_{doc['gamertag']}.png")
        caption = f"**{doc['gamertag']}** — {len(doc['points'])} points"
        if dt_start:
            caption += f"\nRange: `{dt_start.isoformat()}` to `{dt_end.isoformat()}`"
        else:
            caption += f"\nRange: last {window_hours}h"
        await interaction.followup.send(caption, file=file)

async def setup(bot: commands.Bot):
    await bot.add_cog(TraceCog(bot))
