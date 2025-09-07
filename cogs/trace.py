# cogs/trace.py
from __future__ import annotations

import io
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple, Optional

import discord
from discord import app_commands
from discord.ext import commands
from PIL import Image, ImageDraw, ImageFont  # Pillow

from utils.settings import load_settings
from utils.linking import resolve_from_any
from tracer.tracker import load_track

# Optional: actions loader (if your tracker exposes it)
try:
    from tracer.tracker import load_actions  # type: ignore
except Exception:  # pragma: no cover
    load_actions = None  # we'll just skip if unavailable


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
    return (s.get("active_map") or "Livonia").strip()


def _izurvive_url(map_name: str, x: float, z: float) -> str:
    slug = MAP_SLUG.get(map_name.lower(), "livonia")
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


def _draw_pin(drw: ImageDraw.ImageDraw, p: Tuple[int, int], color: Tuple[int, int, int, int], r: int = 8):
    x, y = p
    drw.ellipse([x - r, y - r, x + r, y + r], fill=color, outline=(0, 0, 0, 255), width=2)
    drw.ellipse([x - 2, y - 2, x + 2, y + 2], fill=(0, 0, 0, 255))


def _draw_diamond(drw: ImageDraw.ImageDraw, p: Tuple[int, int], color: Tuple[int, int, int, int], r: int = 7):
    """Small diamond marker for actions."""
    x, y = p
    poly = [(x, y - r), (x + r, y), (x, y + r), (x - r, y)]
    drw.polygon(poly, fill=color, outline=(0, 0, 0, 200))


def _action_color(kind: str) -> Tuple[int, int, int, int]:
    k = (kind or "").lower()
    if "kill" in k or "death" in k or "shot" in k:
        return (66, 135, 245, 255)      # blue
    if "flag" in k or "raid" in k or "door" in k or "lock" in k:
        return (186, 85, 211, 255)      # purple
    if "connect" in k or "disconnect" in k:
        return (160, 160, 160, 255)     # gray
    return (255, 165, 0, 255)           # orange fallback


def _render_trace_png(
    doc: Dict[str, Any],
    guild_id: int | None,
    actions: Optional[List[Dict[str, Any]]] = None
) -> io.BytesIO:
    """
    Self-contained renderer:
      - draws path (start=green, middle=yellow, end=red)
      - overlays action diamonds (colored by type) when actions supplied
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
    pix: List[Tuple[int, int]] = []
    last_xz: Tuple[float, float] | None = None
    W, _ = base.size

    for p in pts:
        try:
            x, z = float(p.get("x")), float(p.get("z"))
        except Exception:
            continue
        # Skip near-duplicates for a cleaner path
        if last_xz and (round(x, 1), round(z, 1)) == (round(last_xz[0], 1), round(last_xz[1], 1)):
            continue
        pix.append(_world_to_image(x, z, world_size, W))
        last_xz = (x, z)

    # polyline with subtle outline
    if len(pix) >= 2:
        try:
            drw.line(pix, fill=(0, 0, 0, 140), width=6)
        except Exception:
            pass
        drw.line(pix, fill=(255, 90, 90, 255), width=3)

    # pins
    if pix:
        _draw_pin(drw, pix[0], (82, 200, 120, 255), r=9)     # start
        for p in pix[1:-1]:
            _draw_pin(drw, p, (238, 210, 2, 255), r=7)      # middles
        if len(pix) > 1:
            _draw_pin(drw, pix[-1], (255, 72, 72, 255), r=9)  # end
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

    # actions overlay
    if actions:
        for a in actions:
            try:
                x, z = float(a.get("x")), float(a.get("z"))
            except Exception:
                continue
            px, py = _world_to_image(x, z, world_size, W)
            color = _action_color(str(a.get("type") or a.get("kind") or "event"))
            _draw_diamond(drw, (px, py), color, r=6)

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

        # -------------------- load actions --------------------
        actions: List[Dict[str, Any]] = []
        if load_actions is not None:
            try:
                # Your tracker can choose to respect start/end or window_hours.
                actions = load_actions(
                    resolved_tag,
                    start=dt_start,
                    end=dt_end if dt_start else None,
                    window_hours=window_hours if not dt_start else None,
                ) or []
            except Exception as e:
                _log(guild_id, "load_actions raised", {"error": repr(e)})

        # ------------------- render image --------------------
        try:
            img_buf = _render_trace_png(doc, guild_id=guild_id, actions=actions)
        except Exception as e:
            _log(guild_id, "internal renderer failed", {"error": repr(e)})
            return await interaction.followup.send(
                "❌ Failed to render map image. See logs for details.",
                ephemeral=True
            )

        _log(guild_id, "render complete", {"points_rendered": count, "actions": len(actions)})

        map_file = discord.File(img_buf, filename=f"trace_{doc.get('gamertag','player')}.png")

        # ------- Caption + per-point clickable links ----------
        last = points[-1]
        try:
            lx, lz = float(last.get("x", 0.0)), float(last.get("z", 0.0))
        except Exception:
            lx, lz = 0.0, 0.0

        map_name = _active_map_name(guild_id)
        izu_last = _izurvive_url(map_name, lx, lz)

        when_last = ""
        try:
            ts_raw = last.get("ts")
            if ts_raw:
                when_last = datetime.fromisoformat(str(ts_raw).replace("Z", "+00:00")) \
                               .astimezone(timezone.utc).strftime("%H:%M:%S UTC")
        except Exception:
            pass

        caption = (
            f"**{doc.get('gamertag','?')}** — {count} points\n"
            f"Last: [({lx:.1f}, {lz:.1f})]({izu_last}) {when_last}"
        )
        if dt_start:
            caption += f"\nRange: `{dt_start.isoformat()}` to `{dt_end.isoformat()}`"
        else:
            caption += f"\nRange: last {window_hours}h"

        # --------- Embeds: Points and (optional) Actions -------
        point_lines: List[str] = []
        n = len(points)
        for idx, p in enumerate(points, start=1):
            try:
                x, z = float(p.get("x", 0.0)), float(p.get("z", 0.0))
            except Exception:
                x, z = 0.0, 0.0
            tag = "🟢 start" if idx == 1 else ("🔴 end" if idx == n else "🟡")
            ts_s = ""
            try:
                ts_raw = p.get("ts")
                if ts_raw:
                    ts_s = datetime.fromisoformat(str(ts_raw).replace("Z", "+00:00")) \
                                   .astimezone(timezone.utc).strftime("%H:%M:%S UTC")
            except Exception:
                pass
            url = _izurvive_url(map_name, x, z)
            point_lines.append(f"{idx}. [{x:.1f}, {z:.1f}]({url}) — {tag} {ts_s}")

        points_embed = discord.Embed(title="Trace points (click to open in iZurvive)")
        # Split across multiple fields to fit Discord limits
        def _add_chunked(embed: discord.Embed, title_prefix: str, lines: List[str]):
            chunk: List[str] = []
            size = 0
            part = 1
            for ln in lines:
                if size + len(ln) + 1 > 1000 and chunk:
                    embed.add_field(name=f"{title_prefix} {part}", value="\n".join(chunk), inline=False)
                    chunk, size, part = [], 0, part + 1
                chunk.append(ln)
                size += len(ln) + 1
            if chunk:
                embed.add_field(name=f"{title_prefix} {part}", value="\n".join(chunk), inline=False)

        _add_chunked(points_embed, "Points", point_lines)

        embeds: List[discord.Embed] = [points_embed]

        # Build actions embed + snapshot text file (G Plot–style)
        snapshot_lines: List[str] = []
        if actions:
            action_lines: List[str] = []
            for a in actions:
                kind = str(a.get("type") or a.get("kind") or "event")
                desc = str(a.get("desc") or a.get("message") or a.get("detail") or "").strip()
                x = a.get("x")
                z = a.get("z")
                link = ""
                try:
                    if x is not None and z is not None:
                        link = f"[({float(x):.1f}, {float(z):.1f})]({_izurvive_url(map_name, float(x), float(z))}) "
                except Exception:
                    link = ""
                tss = ""
                try:
                    ts_raw = a.get("ts")
                    if ts_raw:
                        tss = datetime.fromisoformat(str(ts_raw).replace("Z", "+00:00")) \
                                      .astimezone(timezone.utc).strftime("%H:%M:%S UTC")
                except Exception:
                    pass

                pretty = kind.capitalize()
                text = f"• {pretty}: {link}{desc} {tss}".strip()
                action_lines.append(text)

                # Snapshot (monospace-ish) line
                hhmmss = tss.split(" ")[0] if tss else "--:--:--"
                coord_txt = ""
                try:
                    if x is not None and z is not None:
                        coord_txt = f" ({float(x):.1f},{float(z):.1f})"
                except Exception:
                    coord_txt = ""
                snapshot_lines.append(f"{hhmmss} | {pretty:<12} | {desc or '-'}{coord_txt}")

            actions_embed = discord.Embed(title=f"Actions snapshot ({len(actions)})")
            _add_chunked(actions_embed, "Actions", action_lines)
            embeds.append(actions_embed)

        # Fallback snapshot if no actions available: list positions as POS
        if not snapshot_lines:
            for p in points:
                tss = ""
                try:
                    ts_raw = p.get("ts")
                    if ts_raw:
                        tss = datetime.fromisoformat(str(ts_raw).replace("Z", "+00:00")) \
                                      .astimezone(timezone.utc).strftime("%H:%M:%S UTC")
                except Exception:
                    pass
                try:
                    x, z = float(p.get("x", 0.0)), float(p.get("z", 0.0))
                    coord_txt = f" ({x:.1f},{z:.1f})"
                except Exception:
                    coord_txt = ""
                hhmmss = tss.split(" ")[0] if tss else "--:--:--"
                snapshot_lines.append(f"{hhmmss} | {'POS':<12} | position update{coord_txt}")

        # Build the txt attachment content
        now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        hdr = f"***** ADM Snapshot — {now_utc} *****\n"
        if dt_start:
            hdr += f"Range: {dt_start.isoformat()} .. {dt_end.isoformat()}\n"
        else:
            hdr += f"Range: last {window_hours}h\n"
        hdr += f"Player: {doc.get('gamertag','player')}\n"
        hdr += "----------------------------------------------\n"
        snapshot_text = hdr + "\n".join(snapshot_lines) + "\n"

        snapshot_buf = io.BytesIO(snapshot_text.encode("utf-8"))
        snapshot_file = discord.File(snapshot_buf, filename="adm_snapshot.txt")

        # ----------- post to admin channel if set ------------
        settings = load_settings(guild_id) or {}
        admin_ch_id = settings.get("admin_channel_id")
        _log(guild_id, "post target resolution", {"admin_channel_id": admin_ch_id})

        if admin_ch_id:
            ch = interaction.client.get_channel(int(admin_ch_id))
            if isinstance(ch, discord.TextChannel):
                try:
                    await ch.send(content=caption, files=[map_file, snapshot_file], embeds=embeds)
                    _log(guild_id, "posted to admin channel", {"channel": admin_ch_id})
                    return await interaction.followup.send(
                        f"📡 Posted trace in {ch.mention}.", ephemeral=True
                    )
                except Exception as e:
                    _log(guild_id, "failed posting to admin channel", {"error": repr(e)})

        # Fallback to replying in the invoking channel
        await interaction.followup.send(caption, files=[map_file, snapshot_file], embeds=embeds)


async def setup(bot: commands.Bot):
    await bot.add_cog(TraceCog(bot))
