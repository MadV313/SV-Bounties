from __future__ import annotations

import io
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Any, Tuple

import discord
from discord import app_commands
from discord.ext import commands

from utils.settings import load_settings
from PIL import Image, ImageDraw, ImageFont  # Pillow

# Optional import from tracker (safe fallback if not present during reloads)
try:
    from tracer.tracker import get_guild_snapshot  # type: ignore
except Exception:  # pragma: no cover
    def get_guild_snapshot(_gid: int) -> List[Dict[str, Any]]:
        return []


def admin_check():
    def pred(i: discord.Interaction):
        perms = getattr(i.user, "guild_permissions", None)
        return bool(perms and (perms.administrator or perms.manage_guild))
    return app_commands.check(lambda i: pred(i))


# ----------------------- tiny logger -----------------------
def _now():
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")


def _log(gid: int, msg: str, extra: Dict[str, Any] | None = None):
    base = f"[{_now()}] [showtracked] [guild {gid}] {msg}"
    if extra:
        try:
            import json
            print(base, json.dumps(extra, default=str, ensure_ascii=False))
            return
        except Exception:
            pass
    print(base)
# -----------------------------------------------------------


# ----------------------- map helpers -----------------------
# Canonical names for maps (so we can accept any case/variant)
CANON_MAP = {
    "chernarus+": "Chernarus+",
    "chernarus": "Chernarus",
    "livonia": "Livonia",
    "namalsk": "Namalsk",
}

# World sizes (meters) for common DayZ maps (x/z range).
WORLD_SIZE = {
    "Chernarus+": 15360,
    "Chernarus": 15360,
    "Livonia": 12800,
    "Namalsk": 20480,
}

MAP_SLUG = {  # for iZurvive URL building
    "Chernarus+": "chernarus",
    "Chernarus": "chernarus",
    "Livonia": "livonia",
    "Namalsk": "namalsk",
}

# Where to find background map art (relative to repo root)
MAP_PATHS = {
    "Chernarus+": "assets/maps/chernarus_base.PNG",
    "Chernarus": "assets/maps/chernarus_base.PNG",
    "Livonia": "assets/maps/livonia_base.PNG",
    "Namalsk": "assets/maps/namalsk_base.PNG",
}


def _canon_map_name(s: str | None) -> str:
    s = (s or "Livonia").strip()
    return CANON_MAP.get(s.casefold(), "Livonia")


# Strong path resolution (handles different working dirs / case sensitive FS)
def _resolve_asset(rel_path: str) -> Path | None:
    """
    Try several locations to find the asset on disk:
      - relative to current CWD
      - relative to this file's directory
      - relative to project root (parent of /cogs)
      - /app/... (Railway)
    Returns a Path if the file exists, else None.
    """
    candidates: list[Path] = []
    rel = Path(rel_path)

    # 1) current working dir
    candidates.append(Path.cwd() / rel)

    # 2) alongside this file
    here = Path(__file__).resolve().parent
    candidates.append(here / rel)

    # 3) project root (../)
    candidates.append(here.parent / rel)

    # 4) explicit /app (Railway containers usually run from /app)
    candidates.append(Path("/app") / rel)

    for p in candidates:
        try:
            if p.exists() and p.is_file():
                return p
        except Exception:
            continue
    return None


def _active_map_for_guild(gid: int) -> str:
    st = load_settings(gid) or {}
    active = _canon_map_name(st.get("active_map"))
    return active


def _world_size_for(map_name: str) -> int:
    return WORLD_SIZE.get(_canon_map_name(map_name), 15360)


def _load_map_image(gid: int, map_name: str, size_px: int = 1400) -> Image.Image:
    """
    Try loading a map background; fall back to blank grid if missing.
    Returns an RGBA image (square) so we can draw anti-aliased labels.
    """
    canon = _canon_map_name(map_name)
    rel = MAP_PATHS.get(canon)
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
                _log(gid, "map image loaded", {"map": canon, "path": str(abs_path)})
                return img.resize((size_px, size_px), Image.BICUBIC)
            except Exception as e:
                _log(gid, "map open failed; using fallback",
                     {"map": canon, "path": str(abs_path), "error": repr(e)})
        else:
            _log(gid, "map file not found; using fallback",
                 {"expected_dir": "/app/assets/maps", "rel": rel, "map": canon})

    # Fallback: plain dark background with grid
    side = size_px
    img = Image.new("RGBA", (side, side), (18, 18, 22, 255))
    drw = ImageDraw.Draw(img)
    # draw a simple 10x10 grid
    step = side // 10
    for k in range(0, side + 1, step):
        drw.line([(k, 0), (k, side)], fill=(40, 40, 46, 255), width=1)
        drw.line([(0, k), (side, k)], fill=(40, 40, 46, 255), width=1)
    title = f"{_canon_map_name(map_name)} (fallback)"
    try:
        font = ImageFont.truetype("arial.ttf", 24)
    except Exception:
        font = ImageFont.load_default()
    drw.text((12, 12), title, fill=(200, 200, 200, 255), font=font)
    return img


def _world_to_image(x: float, z: float, world_size: int, img_size: int) -> Tuple[int, int]:
    """
    Convert DayZ world coords (x, z) -> image pixels.
    (0,0) world is bottom-left; image (0,0) is top-left,
    so we flip the vertical axis.
    """
    try:
        px = max(0, min(img_size - 1, int(round((x / world_size) * img_size))))
        py = max(0, min(img_size - 1, int(round(((world_size - z) / world_size) * img_size))))
        return px, py
    except Exception:
        return 0, 0


def _draw_pin(drw: ImageDraw.ImageDraw, p: Tuple[int, int]):
    x, y = p
    r = 9
    drw.ellipse([x - r, y - r, x + r, y + r], fill=(255, 72, 72, 255), outline=(0, 0, 0, 255), width=2)
    drw.ellipse([x - 2, y - 2, x + 2, y + 2], fill=(0, 0, 0, 255))
# -----------------------------------------------------------


def _izurvive_url(map_name: str, x: float, z: float) -> str:
    canon = _canon_map_name(map_name)
    slug = MAP_SLUG.get(canon, "livonia")
    # iZurvive likes decimals with a semicolon delimiter
    return f"https://www.izurvive.com/{slug}/#location={x:.2f};{z:.2f}"


# ---------- pagination helpers (prevents 2,000-char crashes) ----------
EMBED_DESC_LIMIT = 4096  # Discord embed description max
CONTENT_LIMIT_SAFE = 1900  # if we ever use plain content

def _chunk_lines_for_embed(header: str, lines: List[str]) -> List[str]:
    """
    Split a list of bullet lines into multiple embed description strings,
    each <= EMBED_DESC_LIMIT (including header on first page).
    """
    pages: List[str] = []
    cur = header.strip()
    for ln in lines:
        add = ("\n" if cur else "") + ln
        if len(cur) + len(add) > EMBED_DESC_LIMIT:
            pages.append(cur)
            cur = ln  # start next page without header
        else:
            cur += add
    if cur:
        pages.append(cur)
    return pages
# ----------------------------------------------------------------------


class ShowTracked(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @app_commands.command(
        name="showtracked",
        description="Show last-known locations for all currently tracked players (list + map image).",
    )
    @admin_check()
    async def show_tracked(self, interaction: discord.Interaction):
        gid = interaction.guild_id or 0
        st = load_settings(gid) or {}
        admin_channel_id = st.get("admin_channel_id")

        _log(gid, "command invoked", {
            "user": getattr(interaction.user, "id", None),
            "channel": interaction.channel_id,
            "admin_channel_id": admin_channel_id,
        })

        # Require usage in the configured admin channel (if set)
        if admin_channel_id and interaction.channel_id != int(admin_channel_id):
            _log(gid, "wrong channel; refusing")
            return await interaction.response.send_message(
                "⚠️ Please run `/showtracked` in the configured admin channel.",
                ephemeral=True,
            )

        await interaction.response.defer(thinking=True, ephemeral=False)

        # Pull snapshot from tracker
        try:
            raw_rows = get_guild_snapshot(gid) or []
        except Exception as e:
            _log(gid, "get_guild_snapshot raised", {"error": repr(e)})
            return await interaction.followup.send(
                "❌ Failed to read tracker snapshot. See logs for details.",
                ephemeral=True,
            )

        active_map = _active_map_for_guild(gid)
        world_size = _world_size_for(active_map)

        pre_count = len(raw_rows)
        sample = [
            {
                "name": r.get("name"),
                "short_id": r.get("short_id"),
                "x": r.get("x"),
                "z": r.get("z"),
                "map": r.get("map"),
                "ts": r.get("ts"),
            }
            for r in raw_rows[:5]
        ]
        _log(gid, "snapshot loaded", {
            "active_map": active_map,
            "count": pre_count,
            "sample": sample,
        })

        # Filter to current map if items include map info
        def _same_map(row: Dict[str, Any]) -> bool:
            m = _canon_map_name((row.get("map") or active_map))
            return m == active_map

        rows = [r for r in raw_rows if _same_map(r)]
        post_count = len(rows)
        _log(gid, "after map filter", {"kept": post_count, "dropped": pre_count - post_count})

        relaxed_used = False
        if pre_count > 0 and post_count == 0:
            rows = raw_rows
            relaxed_used = True
            _log(gid, "relaxed map filter engaged (using all rows)")

        if not rows:
            msg = f"**No tracked players** for `{active_map}`."
            _log(gid, "no rows to display", {"active_map": active_map})
            return await interaction.followup.send(msg, ephemeral=False)

        # Sort by name for stable output
        rows.sort(key=lambda r: (str(r.get("name") or r.get("short_id")), r.get("short_id", "")))

        # Create image (with reliable asset loading)
        base = _load_map_image(gid, active_map, size_px=1400)  # RGBA
        drw = ImageDraw.Draw(base)
        W, _H = base.size

        # Font — larger + stroke for readability
        try:
            font = ImageFont.truetype("arial.ttf", 22)
        except Exception:
            font = ImageFont.load_default()

        # Draw each pin + label
        pins = []
        for r in rows:
            try:
                x = float(r.get("x") or 0.0)
                z = float(r.get("z") or 0.0)
            except Exception:
                x, z = 0.0, 0.0
            name = str(r.get("name") or r.get("short_id") or "?")
            px, py = _world_to_image(x, z, world_size, W)
            _draw_pin(drw, (px, py))
            # crisp text with stroke (outline) so it reads over the map
            drw.text(
                (px + 12, py - 6),
                name,
                fill=(255, 255, 255, 255),
                font=font,
                stroke_width=2,
                stroke_fill=(0, 0, 0, 255),
            )
            pins.append({"name": name, "x": x, "z": z, "px": px, "py": py})

        _log(gid, "pins drawn", {"count": len(pins), "pins_sample": pins[:5]})

        # Compose text list with clickable iZurvive links
        lines = []
        for r in rows:
            name = str(r.get("name") or r.get("short_id") or "?")
            short_id = str(r.get("short_id") or "")
            try:
                x = float(r.get("x") or 0.0)
                z = float(r.get("z") or 0.0)
            except Exception:
                x, z = 0.0, 0.0
            ts = r.get("ts")
            when = ""
            try:
                if ts and getattr(ts, "tzinfo", None):
                    when = ts.astimezone(timezone.utc).strftime("%H:%M:%S UTC")
            except Exception:
                pass
            url = _izurvive_url(active_map, x, z)
            lines.append(f"• **{name}** ({short_id}) — [({x:.1f}, {z:.1f})]({url}) {when}")

        header = f"**Tracked players — {active_map}**"
        if relaxed_used:
            header += "\n_(Note: map mismatch detected; showing all players returned by tracker.)_"

        # ---- paginate into embeds so we never exceed message limits ----
        pages = _chunk_lines_for_embed(header, lines)
        total_pages = len(pages)

        # Save image buffer once (used only on page 1)
        buf = io.BytesIO()
        base.save(buf, format="PNG")
        buf.seek(0)
        file = discord.File(buf, filename="tracked_map.png")

        # Send first page with the map image
        embed0 = discord.Embed(description=pages[0], color=0x2f3136)
        embed0.set_image(url="attachment://tracked_map.png")
        if total_pages > 1:
            embed0.set_footer(text=f"Page 1/{total_pages}")
        await interaction.followup.send(embed=embed0, file=file, ephemeral=False)

        # Send any remaining pages as additional embeds (no image to avoid reupload)
        for idx in range(1, total_pages):
            em = discord.Embed(description=pages[idx], color=0x2f3136)
            em.set_footer(text=f"Page {idx+1}/{total_pages}")
            await interaction.followup.send(embed=em, ephemeral=False)
        # ----------------------------------------------------------------


async def setup(bot: commands.Bot):
    await bot.add_cog(ShowTracked(bot))
