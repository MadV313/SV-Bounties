# cogs/trace.py
from __future__ import annotations

import io
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple

import discord
from discord import app_commands
from discord.ext import commands
from PIL import Image, ImageDraw, ImageFont  # Pillow

from utils.settings import load_settings
from utils.linking import resolve_from_any
from tracer.tracker import load_track

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


# ---- Map + iZurvive helpers -----------------------------------------------
WORLD_SIZE = {
    "chernarus+": 15360,
    "chernarus": 15360,
    "livonia": 12800,
    "namalsk": 20480,
}

MAP_SLUG = {
    "chernarus+": "chernarus",
    "chernarus": "chernarus",
    "livonia": "livonia",
    "namalsk": "namalsk",
}

MAP_PATHS = {
    "chernarus+": "assets/maps/chernarus_base.PNG",
    "chernarus": "assets/maps/chernarus_base.PNG",
    "livonia": "assets/maps/livonia_base.PNG",
    "namalsk": "assets/maps/namalsk_base.PNG",
}


def _resolve_asset(rel_path: str) -> Path | None:
    """Try several locations to find an asset on disk."""
    candidates: list[Path] = []
    rel = Path(rel_path)
    here = Path(__file__).resolve().parent

    candidates.append(Path.cwd() / rel)         # current working dir
    candidates.append(here / rel)               # alongside this file
    candidates.append(here.parent / rel)        # project root (parent of /cogs)
    candidates.append(Path("/app") / rel)       # Railway default root

    for p in candidates:
        try:
            if p.exists() and p.is_file():
                return p
        except Exception:
            continue
    return None


def _active_map_name(guild_id: int | None) -> str:
    s = load_settings(guild_id) if guild_id else {}
    # your settings store map in lowercase (admin_assign); normalize lookups
    val = (s.get("active_map") or "Livonia").strip()
    return val


def _izurvive_url(map_name: str, x: float, z: float) -> str:
    slug = MAP_SLUG.get(map_name.lower(), "livonia")
    # iZurvive accepts decimals with a semicolon to jump to the exact location
    return f"https://www.izurvive.com/{slug}/#location={x:.2f};{z:.2f}"


def _world_size_for(map_name: str) -> int:
    return WORLD_SIZE.get(map_name.lower(), 15360)


def _world_to_image(x: float, z: float, world_size: int, img_size: int) -> Tuple[int, int]:
    """
    DayZ world: (0,0) bottom-left. Image: (0,0) top-left. Flip Z axis.
    """
    try:
        px = max(0, min(img_size - 1, int(round((x / world_size) * img_size))))
        py = max(0, min(img_size - 1, int(round(((world_size - z) / world_size) * img_size))))
        return px, py
    except Exception:
        return 0, 0


def _load_map_image(gid: int | None, map_name: str, size_px: int = 1400) -> Image.Image:
    rel = MAP_PATHS.get(map_name.lower())
    if rel:
        abs_path = _resolve_asset(rel)
        if abs_path:
            try:
                img = Image.open(abs_path).convert("RGBA")
                if img.width != img.height:
                    side = max(img.width, img.height)
                    canvas = Image.new("RGBA", (side, side), (18, 18, 22, 255))
                    ox = (side - img.width) // 2
                    oy = (side - img.height) // 2
                    canvas.paste(img, (ox, oy))
                    img = canvas
                _log(gid, "map image loaded", {"map": map_name, "path": str(abs_path)})
                return img.resize((size_px, size_px), Image.BICUBIC)
            except Exception as e:
                _log(gid, "map open failed; using fallback", {"map": map_name, "path": str(abs_path), "error": repr(e)})
        else:
            _log(gid, "map file not found; using fallback", {"expected": rel})

    # Fallback grid
    side = size_px
    img = Image.new("RGBA", (side, side), (18, 18, 22, 255))
    drw = ImageDraw.Draw(img)
    step = side // 10
    for k in range(0, side + 1, step):
        drw.line([(k, 0), (k, side)], fill=(40, 40, 46, 255), width=1)
        drw.line([(0, k), (side, k)], fill=(40, 40, 46, 255), width=1)
    try:
        font = ImageFont.truetype("arial.ttf", 24)
    except Exception:
        font = ImageFont.load_default()
    drw.text((12, 12), f"{map_name} (fallback)", fill=(200, 200, 200, 255), font=font)
    return img


def _draw_pin(drw: ImageDraw.ImageDraw, p: Tuple[int, int], color: Tuple[int, int, int, int]):
    x, y = p
    r = 8
    drw.ellipse([x - r, y - r, x + r, y + r], fill=color, outline=(0, 0, 0, 255), width=2)
    drw.ellipse([x - 2, y - 2, x + 2, y + 2], fill=(0, 0, 0, 255))


def _render_trace_png(doc: Dict[str, Any], guild_id: int | None) -> io.BytesIO:
    """
    Minimal, self-contained renderer (avoids old map_renderer dependency).
    Draws the player's path on the active map background.
    """
    map_name = _active_map_name(guild_id)
    world_size = _world_size_for(map_name)
    base = _load_map_image(guild_id, map_name, size_px=1200)  # square RGBA
    drw = ImageDraw.Draw(base)

    try:
        font = ImageFont.truetype("arial.ttf", 22)
    except Exception:
        font = ImageFont.load_default()

    pts = doc.get("points", []) or []
    # convert to pixels, keep only sensible numbers
    pix: List[Tuple[int, int]] = []
    last_xz: Tuple[float, float] | None = None
    W, _ = base.size

    for p in pts:
        try:
            x, z = float(p.get("x")), float(p.get("z"))
        except Exception:
            continue
        if last_xz and (round(x, 1), round(z, 1)) == (round(last_xz[0], 1), round(last_xz[1], 1)):
            # skip tiny duplicates for a cleaner line
            continue
        pix.append(_world_to_image(x, z, world_size, W))
        last_xz = (x, z)

    # draw polyline
    if len(pix) >= 2:
        # main path
        drw.line(pix, fill=(255, 90, 90, 255), width=4)
        # a thin black outline for contrast
        try:
            drw.line(pix, fill=(0, 0, 0, 120), width=6, joint="curve")  # Pillow may ignore 'joint' on some builds
            drw.line(pix, fill=(255, 90, 90, 255), width=3)
        except Exception:
            pass

    # pins
    if pix:
        _draw_pin(drw, pix[0], (82, 200, 120, 255))   # start - green
        _draw_pin(drw, pix[-1], (255, 72, 72, 255))   # end   - red
        # label near end
        name = str(doc.get("gamertag") or "player")
        ex, ey = pix[-1]
        drw.text(
            (ex + 10, ey - 6),
            name,
            fill=(255, 255, 255, 255),
            font=font,
            stroke_width=2,
            stroke_fill=(0, 0, 0, 255),
        )

    # out to buffer
    buf = io.BytesIO()
    base.save(buf, format="PNG")
    buf.seek(0)
    return buf
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
            img_buf = _render_trace_png(doc, guild_id=guild_id)
        except Exception as e:
            _log(guild_id, "internal renderer failed", {"error": repr(e)})
            return await interaction.followup.send(
                "❌ Failed to render map image. See logs for details.",
                ephemeral=True
            )

        _log(guild_id, "render complete", {"points_rendered": count})

        file = discord.File(img_buf, filename=f"trace_{doc.get('gamertag','player')}.png")

        # Caption with clickable iZurvive link to last point
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

        # Fallback to replying in the invoking channel
        await interaction.followup.send(caption, file=file)


async def setup(bot: commands.Bot):
    await bot.add_cog(TraceCog(bot))
