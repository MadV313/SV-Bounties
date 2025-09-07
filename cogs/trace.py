# cogs/trace.py
import discord
from discord import app_commands
from discord.ext import commands
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

from utils.settings import load_settings
from utils.linking import resolve_from_any
from tracer.tracker import load_track
from tracer.map_renderer import render_track_png

# ----------------------- tiny logger -----------------------
def _now() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")


def _log(gid: int | None, msg: str, extra: Dict[str, Any] | None = None) -> None:
    base = f"[{_now()}] [trace] [guild {gid}] {msg}"
    if extra:
        try:
            import json
            print(base, json.dumps(extra, default=str, ensure_ascii=False))
            return
        except Exception:
            pass
    print(base)
# -----------------------------------------------------------


def _parse_dt(s: str) -> datetime | None:
    if not s:
        return None
    s = s.strip()
    fmts = ["%Y-%m-%d %H:%M", "%Y-%m-%d", "%m/%d/%Y %H:%M", "%m/%d/%Y"]
    for f in fmts:
        try:
            return datetime.strptime(s, f).replace(tzinfo=timezone.utc)
        except Exception:
            continue
    return None


# ---- iZurvive helper ---------------------------------------------------------
_MAP_SLUG = {
    "chernarus+": "chernarus",
    "chernarus": "chernarus",
    "livonia": "livonia",
    "namalsk": "namalsk",
}
def _active_map_name(guild_id: int | None) -> str:
    st = load_settings(guild_id) if guild_id else {}
    return (st.get("active_map") or "Livonia").strip()

def _izurvive_url(map_name: str, x: float, z: float) -> str:
    slug = _MAP_SLUG.get(map_name.lower(), "livonia")
    # iZurvive accepts decimals; semicolon delimiter jumps to the exact spot
    return f"https://www.izurvive.com/{slug}/#location={x:.2f};{z:.2f}"
# -----------------------------------------------------------------------------


class TraceCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @app_commands.command(name="trace", description="Render a player's movement path")
    @app_commands.describe(
        user="Discord user (optional if you provide gamertag)",
        gamertag="Gamertag (optional if you select a user)",
        start="Start datetime (e.g., 2025-09-04 13:30, UTC assumed)",
        end="End datetime (optional). Default: now",
        window_hours="Alternative to start/end. Default 24h."
    )
    async def trace(
        self,
        interaction: discord.Interaction,
        user: discord.User | None = None,
        gamertag: str | None = None,
        start: str | None = None,
        end: str | None = None,
        window_hours: int | None = 24
    ):
        await interaction.response.defer(thinking=True, ephemeral=True)

        guild_id = interaction.guild_id
        invoker_id = getattr(interaction.user, "id", None)
        _log(guild_id, "command invoked", {
            "invoker": invoker_id,
            "channel": interaction.channel_id,
            "args": {
                "user": getattr(user, "id", None),
                "gamertag": gamertag,
                "start": start,
                "end": end,
                "window_hours": window_hours,
            }
        })

        # ---------------- identity resolution ----------------
        # If a gamertag was explicitly provided, trust it (no link required).
        resolved_tag: str | None = None
        resolved_did: str | None = None

        def _clean_tag(s: str) -> str:
            # trim, normalize common weird whitespace/quotes
            return (s or "").strip().strip("“”\"'")

        if gamertag and _clean_tag(gamertag):
            resolved_tag = _clean_tag(gamertag)
            resolved_did = str(user.id) if user else None
            _log(guild_id, "using provided gamertag (bypassing link lookup)", {"gamertag": resolved_tag})
        else:
            did = str(user.id) if user else str(interaction.user.id)
            _log(guild_id, "resolving via link table", {"discord_id": did})
            try:
                resolved_did, resolved_tag = resolve_from_any(
                    guild_id, discord_id=did, gamertag=None
                )
            except Exception as e:
                _log(guild_id, "resolve_from_any raised", {"error": repr(e)})
                return await interaction.followup.send(
                    "❌ Internal error while resolving player identity. Check logs.", ephemeral=True
                )

        _log(guild_id, "identity result", {
            "resolved_discord_id": resolved_did,
            "resolved_gamertag": resolved_tag
        })

        if not resolved_tag:
            return await interaction.followup.send(
                "❌ Couldn’t resolve that player. "
                "Provide a **gamertag** in the command or `/link` your Discord first.",
                ephemeral=True
            )

        # ---------------- time window logic ------------------
        dt_start = _parse_dt(start) if start else None
        dt_end = _parse_dt(end) if end else datetime.now(timezone.utc)

        # If explicit start provided, compute a sane end if missing/invalid and ignore window_hours
        if dt_start:
            if not dt_end or dt_end <= dt_start:
                dt_end = dt_start + timedelta(hours=1)
            _log(guild_id, "explicit time range parsed", {
                "start": dt_start.isoformat(),
                "end": dt_end.isoformat()
            })
            window_hours = None
        else:
            if not window_hours:
                window_hours = 24
            _log(guild_id, "window mode", {"window_hours": window_hours})

        # ---------------- load track points ------------------
        try:
            pid, doc = load_track(resolved_tag, window_hours=window_hours, max_points=1000)
        except Exception as e:
            _log(guild_id, "load_track raised", {"gamertag": resolved_tag, "error": repr(e)})
            return await interaction.followup.send(
                f"❌ Failed to load track for `{resolved_tag}`. See logs.",
                ephemeral=True
            )

        points = (doc or {}).get("points") if doc else None
        count = len(points) if points else 0
        sample = points[:5] if points else []
        _log(guild_id, "track loaded", {
            "gamertag": resolved_tag,
            "player_id": pid,
            "doc_keys": list(doc.keys()) if isinstance(doc, dict) else None,
            "point_count": count,
            "sample": sample
        })

        if not doc or not points:
            return await interaction.followup.send(
                f"ℹ️ No track points found for `{resolved_tag}` in that window.",
                ephemeral=True
            )

        # --------------- filter by explicit range ------------
        if dt_start:
            pts: List[Dict[str, Any]] = []
            dropped = 0
            for p in points:
                try:
                    ts = datetime.fromisoformat(p["ts"].replace("Z", "+00:00"))
                except Exception:
                    dropped += 1
                    continue
                if dt_start <= ts <= dt_end:
                    pts.append(p)
            _log(guild_id, "post filter (explicit range)", {
                "kept": len(pts),
                "dropped": dropped,
                "range": {"start": dt_start.isoformat(), "end": dt_end.isoformat()},
            })
            if not pts:
                return await interaction.followup.send(
                    f"ℹ️ No points for `{resolved_tag}` in that time range.",
                    ephemeral=True
                )
            doc = {**doc, "points": pts}
            points = pts
            count = len(points)

        # ------------------- render image --------------------
        try:
            # Newer renderer may accept guild_id; call compatibly.
            try:
                img = render_track_png(doc, guild_id=guild_id)  # newer signature
            except TypeError:
                img = render_track_png(doc)  # older signature
        except Exception as e:
            _log(guild_id, "render_track_png failed", {"error": repr(e), "doc_keys": list(doc.keys())})
            return await interaction.followup.send(
                "❌ Failed to render map image. See logs for details.",
                ephemeral=True
            )

        _log(guild_id, "render complete", {"points_rendered": count})

        file = discord.File(img, filename=f"trace_{doc.get('gamertag','player')}.png")

        # Build caption with clickable iZurvive link for last point
        last = points[-1]
        try:
            lx, lz = float(last.get("x", 0.0)), float(last.get("z", 0.0))
        except Exception:
            lx, lz = 0.0, 0.0

        map_name = _active_map_name(guild_id)
        izu = _izurvive_url(map_name, lx, lz)

        when = ""
        try:
            ts_raw = last.get("ts")
            if ts_raw:
                when = datetime.fromisoformat(str(ts_raw).replace("Z", "+00:00")) \
                               .astimezone(timezone.utc).strftime("%H:%M:%S UTC")
        except Exception:
            pass

        caption = (
            f"**{doc.get('gamertag','?')}** — {count} points\n"
            f"Last: [({lx:.1f}, {lz:.1f})]({izu}) {when}"
        )
        if dt_start:
            caption += f"\nRange: `{dt_start.isoformat()}` to `{dt_end.isoformat()}`"
        else:
            caption += f"\nRange: last {window_hours}h"

        # ----------- post to admin channel if set ------------
        settings = load_settings(guild_id) or {}
        admin_ch_id = settings.get("admin_channel_id")
        _log(guild_id, "post target resolution", {"admin_channel_id": admin_ch_id})

        if admin_ch_id:
            ch = interaction.client.get_channel(int(admin_ch_id))
            if isinstance(ch, discord.TextChannel):
                try:
                    await ch.send(content=caption, file=file)
                    _log(guild_id, "posted to admin channel", {"channel": admin_ch_id})
                    return await interaction.followup.send(
                        f"📡 Posted trace in {ch.mention}.", ephemeral=True
                    )
                except Exception as e:
                    _log(guild_id, "failed posting to admin channel", {"error": repr(e)})

        # Fallback to replying in-channel
        await interaction.followup.send(caption, file=file)


async def setup(bot: commands.Bot):
    await bot.add_cog(TraceCog(bot))
