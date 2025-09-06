# cogs/show_tracked.py
from __future__ import annotations

import io
from datetime import datetime, timezone
from typing import List, Dict, Any, Tuple

import discord
from discord import app_commands
from discord.ext import commands

from utils.settings import load_settings
from PIL import Image, ImageDraw, ImageFont  # Pillow

# This function should be provided by your tracker module.
# Expected shape:
# get_guild_snapshot(guild_id) -> list of dicts with keys:
#   short_id, name, x, z, ts (datetime or ISO string), map (optional)
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
# World sizes (meters) for common DayZ maps (x/z range).
WORLD_SIZE = {
    "Chernarus+": 15360,
    "Chernarus": 15360,
    "Livonia": 12800,
    "Namalsk": 20480,
}

# Where to find background map art (optional)
MAP_PATHS = {
    "Chernarus+": "assets/maps/chernarus.png",
    "Chernarus": "assets/maps/chernarus.png",
    "Livonia": "assets/maps/livonia.png",
    "Namalsk": "assets/maps/namalsk.png",
}


def _active_map_for_guild(gid: int) -> str:
    st = load_settings(gid) or {}
    return (st.get("active_map") or "Livonia").strip()


def _world_size_for(map_name: str) -> int:
    # Default to 15360 if unknown.
    return WORLD_SIZE.get(map_name, 15360)


def _load_map_image(map_name: str, size_px: int = 1400) -> Image.Image:
    """
    Try loading a map background; fall back to blank grid if missing.
    Returns an RGB image (square).
    """
    path = MAP_PATHS.get(map_name)
    if path:
        try:
            img = Image.open(path).convert("RGB")
            # fit to square with letterbox/pad if needed
            if img.width != img.height:
                side = max(img.width, img.height)
                canvas = Image.new("RGB", (side, side), (18, 18, 22))
                ox = (side - img.width) // 2
                oy = (side - img.height) // 2
                canvas.paste(img, (ox, oy))
                img = canvas
            return img.resize((size_px, size_px), Image.BICUBIC)
        except Exception:
            pass

    # Fallback: plain dark background with grid
    side = size_px
    img = Image.new("RGB", (side, side), (18, 18, 22))
    drw = ImageDraw.Draw(img)
    # draw a simple 10x10 grid
    step = max(1, side // 10)
    for k in range(0, side + 1, step):
        drw.line([(k, 0), (k, side)], fill=(40, 40, 46), width=1)
        drw.line([(0, k), (side, k)], fill=(40, 40, 46), width=1)
    title = f"{map_name} (fallback)"
    drw.text((12, 12), title, fill=(200, 200, 200))
    return img


def _world_to_image(x: float, z: float, world_size: int, img_size: int) -> Tuple[int, int]:
    """
    Convert DayZ world coords (x, z) -> image pixels.
    (0,0) world is bottom-left; image (0,0) is top-left,
    so we flip the vertical axis.
    """
    try:
        # Clamp to [0, world_size]
        x = max(0.0, min(float(world_size), float(x)))
        z = max(0.0, min(float(world_size), float(z)))
        px = max(0, min(img_size - 1, int((x / world_size) * img_size)))
        py = max(0, min(img_size - 1, int(((world_size - z) / world_size) * img_size)))
        return px, py
    except Exception:
        return 0, 0


def _draw_pin(drw: ImageDraw.ImageDraw, p: Tuple[int, int], color=(255, 64, 64)):
    x, y = p
    r = 8
    drw.ellipse([x - r, y - r, x + r, y + r], fill=color, outline=(0, 0, 0))
    drw.ellipse([x - 2, y - 2, x + 2, y + 2], fill=(0, 0, 0))


def _load_font() -> ImageFont.ImageFont:
    # Try common fonts inside many Linux containers; fall back to Pillow default.
    for path in (
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/freefont/FreeSans.ttf",
        "arial.ttf",
    ):
        try:
            return ImageFont.truetype(path, 18)
        except Exception:
            continue
    return ImageFont.load_default()
# -----------------------------------------------------------


def _parse_ts(value: Any) -> datetime | None:
    """Accept datetime or ISO string; return aware UTC datetime or None."""
    try:
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        if isinstance(value, str):
            # tolerate trailing "Z"
            if value.endswith("Z"):
                value = value[:-1] + "+00:00"
            return datetime.fromisoformat(value).astimezone(timezone.utc)
    except Exception:
        pass
    return None


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
                ephemeral=False,
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

        # Filter to current map if items include map info (case-insensitive)
        def _same_map(row: Dict[str, Any]) -> bool:
            m = (row.get("map") or active_map).strip()
            return m.lower() == active_map.lower()

        rows = [r for r in raw_rows if _same_map(r)]
        post_count = len(rows)
        _log(gid, "after map filter", {"kept": post_count, "dropped": pre_count - post_count})

        # If we had data but lost it all only due to map mismatch, relax filter as a safety net.
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
        rows.sort(key=lambda r: (str(r.get("name") or r.get("short_id") or ""), r.get("short_id", "")))

        # Create image
        base = _load_map_image(active_map, size_px=1400)
        drw = ImageDraw.Draw(base)
        W, H = base.size
        font = _load_font()

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
            drw.text((px + 10, py - 4), f"{name}", fill=(235, 235, 235), font=font)
            pins.append({"name": name, "x": x, "z": z, "px": px, "py": py})

        _log(gid, "pins drawn", {"count": len(pins), "pins_sample": pins[:5]})

        # Compose text list with coords (rounded)
        lines = []
        for r in rows:
            name = str(r.get("name") or r.get("short_id") or "?")
            short_id = str(r.get("short_id") or "")
            try:
                x = float(r.get("x") or 0.0)
                z = float(r.get("z") or 0.0)
            except Exception:
                x, z = 0.0, 0.0
            ts = _parse_ts(r.get("ts"))
            when = f"{ts.astimezone(timezone.utc).strftime('%H:%M:%S UTC')}" if ts else ""
            lines.append(f"• **{name}** ({short_id}) — ({x:.1f}, {z:.1f}) {when}")

        header = f"**Tracked players — {active_map}**"
        if relaxed_used:
            header += "\n_(Note: map mismatch detected; showing all players returned by tracker.)_"
        header += "\n" + "\n".join(lines)

        # Save image to in-memory buffer
        buf = io.BytesIO()
        base.save(buf, format="PNG")
        buf.seek(0)
        file = discord.File(buf, filename="tracked_map.png")

        await interaction.followup.send(content=header, file=file, ephemeral=False)


async def setup(bot: commands.Bot):
    await bot.add_cog(ShowTracked(bot))
