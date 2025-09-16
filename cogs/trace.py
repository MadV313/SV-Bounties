# cogs/trace.py
from __future__ import annotations

import io
import re
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
    load_actions = None

# Optional storage client (supports local/remote reads like S3/FS)
try:
    from utils.storageClient import load_file  # type: ignore
except Exception:
    load_file = None


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


def _load_map_image(gid: int | None, map_name: str, size_px: int = 1200) -> Image.Image:
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
                RESAMPLE = getattr(getattr(Image, "Resampling", Image), "BICUBIC")
                return img.resize((size_px, size_px), RESAMPLE)
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


# ------------------- Fallback ADM scanner -------------------
ADM_CANDIDATES = [
    "data/latest_adm.log",
    "latest_adm.log",
    "logs/latest_adm.log",
]

# Time like "15:07:49 | ..."
_TIME_RE = re.compile(r"^\s*(?:\d+\s+)?(\d{2}:\d{2}:\d{2})\s*\|\s*", re.I)
_POS_RE = re.compile(
    r'pos\s*=\s*<\s*(?P<x>-?\d+(?:\.\d+)?)[,\s]+(?P<z>-?\d+(?:\.\d+)?)[,\s]+(?P<y>-?\d+(?:\.\d+)?)\s*>',
    re.I,
)

def _classify(line: str) -> str:
    l = line.lower()
    if "is connected" in l:
        return "connect"
    if "has been disconnected" in l:
        return "disconnect"
    if "placed" in l:
        return "placed"
    if "teleported" in l:
        return "teleport"
    if "hit by" in l or "is unconscious" in l or "regained consciousness" in l or "was killed by" in l:
        return "combat"
    if "performed" in l or "emote" in l:
        return "emote"
    return "event"

def _extract_time(utc_date: datetime, line: str) -> Optional[datetime]:
    m = _TIME_RE.search(line)
    if not m:
        return None
    hh, mm, ss = m.group(1).split(":")
    return utc_date.replace(hour=int(hh), minute=int(mm), second=int(ss), microsecond=0)

def _extract_coords(line: str) -> Tuple[Optional[float], Optional[float]]:
    m = _POS_RE.search(line)
    if not m:
        return None, None
    try:
        return float(m.group("x")), float(m.group("z"))
    except Exception:
        return None, None

def _read_text_candidates(gid: int | None, guild_settings: dict) -> str:
    """Read ADM mirror. Prefer per-guild mirror, then global."""
    paths: List[str] = []
    if gid:  # **per-guild mirror written by the fetcher**
        paths.append(f"data/latest_adm_{gid}.log")

    # optional override from settings
    custom = (guild_settings or {}).get("adm_latest_path")
    if custom:
        paths.append(custom)

    # global mirrors (fallbacks)
    for p in ADM_CANDIDATES:
        if p not in paths:
            paths.append(p)

    # Try storageClient first, then local disk
    for p in paths:
        # storageClient
        if load_file is not None:
            try:
                blob = load_file(p)
                if blob:
                    text = blob.decode("utf-8", "ignore") if isinstance(blob, (bytes, bytearray)) else str(blob)
                    _log(gid, "ADM source chosen (storageClient)", {"path": p, "bytes": len(text)})
                    return text
            except Exception as e:
                _log(gid, "storageClient load failed", {"path": p, "error": repr(e)})

        # local disk
        try:
            fp = Path(p)
            if fp.exists() and fp.is_file():
                text = fp.read_text(encoding="utf-8", errors="ignore")
                _log(gid, "ADM source chosen (local)", {"path": p, "bytes": len(text)})
                return text
        except Exception as e:
            _log(gid, "local read failed", {"path": p, "error": repr(e)})

    _log(gid, "no ADM text available for fallback scan", None)
    return ""

def _fallback_load_actions(
    gid: int | None,
    gamertag: str,
    start: Optional[datetime],
    end: Optional[datetime],
    window_hours: Optional[int],
    guild_settings: dict,
    max_lines: int = 25000
) -> List[Dict[str, Any]]:
    """
    Read latest ADM and extract *verbatim* action lines for a single player.

    This variant is intentionally tolerant:
    - matches quoted or unquoted names
    - doesn't require the literal 'Player ' prefix
    - case-insensitive
    """
    txt = _read_text_candidates(gid, guild_settings)
    if not txt:
        _log(gid, "no ADM text available for fallback scan")
        return []

    esc = re.escape(gamertag)
    # Accept:
    #   Player "Name" ...,  Player Name ...,  "Name" ...,  Name ...
    # We only require the *end* to be a word boundary so we don't match longer suffixes.
    name_pat = re.compile(rf'(?i)(?:player\s+)?(?:"{esc}"|{esc})\b')

    now = datetime.now(timezone.utc)
    if start and end:
        win_start, win_end = start, end
    else:
        hrs = window_hours or 24
        win_end = now
        win_start = now - timedelta(hours=hrs)

    date_for_ts = now.astimezone(timezone.utc)

    actions: List[Dict[str, Any]] = []
    lines = txt.splitlines()
    if len(lines) > max_lines:
        lines = lines[-max_lines:]

    for ln in lines:
        # Fast prefilter by name (tolerant)
        if not name_pat.search(ln):
            continue

        ts = _extract_time(date_for_ts, ln)
        if ts and not (win_start <= ts <= win_end):
            continue

        kind = _classify(ln)
        x, z = _extract_coords(ln)

        actions.append({
            "ts": ts.isoformat() if ts else None,
            "type": kind,
            "desc": ln.split("|", 1)[-1].strip(),
            "x": x,
            "z": z,
            "raw": ln.strip(),   # preserve the full original line for the snapshot
        })

    _log(gid, "fallback actions parsed", {"count": len(actions)})
    return actions
# -----------------------------------------------------------


# -------------------- Embed helpers (hard caps) --------------------
# Discord hard limit for a single embed is 6000 chars total (title+fields).
# We'll keep a safety margin.
_EMBED_TOTAL_BUDGET = 5500
_EMBED_FIELD_BUDGET = 950
_EMBED_MAX_FIELDS = 20
_POINTS_MAX_LINES = 200            # cap points lines to avoid overflow

def _add_lines_with_budget(embed: discord.Embed, title_prefix: str, lines: List[str],
                           total_budget: int = _EMBED_TOTAL_BUDGET,
                           field_budget: int = _EMBED_FIELD_BUDGET,
                           max_fields: int = _EMBED_MAX_FIELDS) -> None:
    """
    Add lines into fields without exceeding Discord's 6k embed limit.
    """
    used = len(embed.title or "") + len(embed.description or "")
    fields_added = 0

    chunk: List[str] = []
    chunk_len = 0
    part = 1

    def flush():
        nonlocal used, fields_added, chunk, chunk_len, part
        if not chunk:
            return
        if fields_added >= max_fields:
            return
        value = "\n".join(chunk)
        name = f"{title_prefix} {part}"
        # rough accounting
        addition = len(name) + len(value) + 8
        if used + addition > total_budget:
            # add a small truncated note if possible
            if used + len("(truncated)") + 25 <= total_budget and fields_added < max_fields:
                embed.add_field(name="(truncated)", value="…see adm_snapshot.txt", inline=False)
            chunk = []
            chunk_len = 0
            return
        embed.add_field(name=name, value=value, inline=False)
        used += addition
        fields_added += 1
        part += 1
        chunk = []
        chunk_len = 0

    for ln in lines:
        if len(ln) > field_budget:
            ln = ln[:field_budget - 1] + "…"
        if chunk_len + len(ln) + 1 > field_budget:
            flush()
            if fields_added >= max_fields:
                break
        if fields_added >= max_fields:
            break
        chunk.append(ln)
        chunk_len += len(ln) + 1
    if fields_added < max_fields:
        flush()
# -------------------------------------------------------------------


def _render_trace_png(
    doc: Dict[str, Any],
    guild_id: int | None,
    actions: Optional[List[Dict[str, Any]]] = None
) -> io.BytesIO:
    """
    Draws path and overlays action diamonds.
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
        if last_xz and (round(x, 1), round(z, 1)) == (round(last_xz[0], 1), round(last_xz[1], 1)):
            continue
        pix.append(_world_to_image(x, z, world_size, W))
        last_xz = (x, z)

    if len(pix) >= 2:
        try:
            drw.line(pix, fill=(0, 0, 0, 140), width=6)
        except Exception:
            pass
        drw.line(pix, fill=(255, 90, 90, 255), width=3)

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

    if actions:
        for a in actions:
            try:
                x, z = float(a.get("x")), float(a.get("z"))
            except Exception:
                continue
            if x is None or z is None:
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
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.guild_only()
    @app_commands.command(name="trace", description="Render a player's movement path")
    @app_commands.describe(
        user="Discord user (optional if you provide gamertag)",
        gamertag="Gamertag (optional if you select a user)",
        window_hours="How far back to look (hours). Default 24."
    )
    async def trace(
        self,
        interaction: discord.Interaction,
        user: discord.User | None = None,
        gamertag: str | None = None,
        window_hours: int | None = 24
    ):
        gid = interaction.guild_id
        st = load_settings(gid) if gid else {}
        admin_ch_id = int(st.get("admin_channel_id") or 0)
        if admin_ch_id and interaction.channel_id != admin_ch_id:
            return await interaction.response.send_message(
                f"⚠️ Please run this in <#{admin_ch_id}>.", ephemeral=True
            )
        await interaction.response.defer(thinking=True, ephemeral=True)

        _log(gid, "command invoked", {
            "channel": interaction.channel_id,
            "args": {
                "user": getattr(user, "id", None),
                "gamertag": gamertag,
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
            _log(gid, "using provided gamertag", {"gamertag": resolved_tag})
        else:
            did = str(user.id) if user else str(interaction.user.id)
            _log(gid, "resolving via link table", {"discord_id": did})
            try:
                resolved_did, resolved_tag = resolve_from_any(gid, discord_id=did, gamertag=None)
            except Exception as e:
                _log(gid, "resolve_from_any raised", {"error": repr(e)})
                return await interaction.followup.send(
                    "❌ Internal error while resolving player identity. Check logs.",
                    ephemeral=True
                )

        if not resolved_tag:
            return await interaction.followup.send(
                "❌ Couldn’t resolve that player. Provide a **gamertag** or `/link` first.",
                ephemeral=True
            )

        # ---------------- window mode only -------------------
        if not window_hours:
            window_hours = 24
        _log(gid, "window mode", {"window_hours": window_hours})

        # ---------------- load track points ------------------
        try:
            pid, doc = load_track(resolved_tag, window_hours=window_hours, max_points=1000)
        except Exception as e:
            _log(gid, "load_track raised", {"gamertag": resolved_tag, "error": repr(e)})
            return await interaction.followup.send(
                f"❌ Failed to load track for `{resolved_tag}`. See logs.",
                ephemeral=True
            )

        # Ensure gamertag present for caption and drawing
        player_name = resolved_tag
        if isinstance(doc, dict):
            if not doc.get("gamertag"):
                doc["gamertag"] = player_name

        points = (doc or {}).get("points") if doc else None
        if not doc or not points:
            return await interaction.followup.send(
                f"ℹ️ No track points found for `{player_name}` in that window.",
                ephemeral=True
            )

        # -------------------- load actions --------------------
        actions: List[Dict[str, Any]] = []
        if load_actions is not None:
            try:
                actions = load_actions(
                    player_name,
                    start=None,
                    end=None,
                    window_hours=window_hours,
                ) or []
            except Exception as e:
                _log(gid, "load_actions raised", {"error": repr(e)})
                actions = []

        # Fallback to direct ADM scan if none found
        if not actions:
            actions = _fallback_load_actions(
                gid=gid,
                gamertag=player_name,
                start=None,
                end=None,
                window_hours=window_hours,
                guild_settings=st or {},
            )

        # ------------------- render image --------------------
        try:
            img_buf = _render_trace_png(doc, guild_id=gid, actions=actions)
        except Exception as e:
            _log(gid, "internal renderer failed", {"error": repr(e)})
            return await interaction.followup.send(
                "❌ Failed to render map image. See logs for details.",
                ephemeral=True
            )

        map_file = discord.File(img_buf, filename=f"trace_{player_name}.png")

        # ------- caption and link to last point ---------------
        last = points[-1]
        try:
            lx, lz = float(last.get("x", 0.0)), float(last.get("z", 0.0))
        except Exception:
            lx, lz = 0.0, 0.0

        map_name = _active_map_name(gid)
        izu_last = _izurvive_url(map_name, lx, lz)

        when_last = ""
        try:
            ts_raw = last.get("ts")
            if ts_raw:
                when_last = datetime.fromisoformat(str(ts_raw).replace("Z", "+00:00")) \
                    .astimezone(timezone.utc).strftime("%H:%M:%S UTC")
        except Exception:
            pass

        count = len(points)
        caption = (
            f"**{player_name}** — {count} points\n"
            f"Last: [({lx:.1f}, {lz:.1f})]({izu_last}) {when_last}\n"
            f"Range: last {window_hours}h"
        )

        # --------- Embeds: Points only (no actions embed) ----
        embeds: List[discord.Embed] = []

        points_embed = discord.Embed(title="Trace points (click to open in iZurvive)")
        point_lines: List[str] = []
        n = len(points)
        show_points = min(n, _POINTS_MAX_LINES)
        start_index = n - show_points + 1  # last N points
        for idx, p in enumerate(points[-show_points:], start=start_index):
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
        if show_points < n:
            point_lines.append(f"…and {n - show_points} older points.")
        _add_lines_with_budget(points_embed, "Points", point_lines)
        embeds.append(points_embed)

        # -------- Build the .txt attachment content ----------
        snapshot_lines: List[str] = []
        if actions:
            # Prefer verbatim ADM lines
            for a in actions:
                tss = ""
                try:
                    ts_raw = a.get("ts")
                    if ts_raw:
                        tss = datetime.fromisoformat(str(ts_raw).replace("Z", "+00:00")) \
                              .astimezone(timezone.utc).strftime("%H:%M:%S UTC")
                except Exception:
                    pass
                hhmmss = tss.split(" ")[0] if tss else "--:--:--"
                raw = str(a.get("raw") or "").strip()
                if raw:
                    snapshot_lines.append(f"{hhmmss} | {raw}")
                else:
                    kind = str(a.get("type") or a.get("kind") or "event").capitalize()
                    x = a.get("x"); z = a.get("z")
                    coord_txt = ""
                    try:
                        if x is not None and z is not None:
                            coord_txt = f" ({float(x):.1f},{float(z):.1f})"
                    except Exception:
                        coord_txt = ""
                    desc = str(a.get("desc") or a.get("message") or a.get("detail") or "").strip()
                    snapshot_lines.append(f"{hhmmss} | {kind:<12} | {desc or '-'}{coord_txt}")

        # If still nothing actionable, fall back to POS list
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

        now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        hdr = f"***** ADM Snapshot — {now_utc} *****\n"
        hdr += f"Range: last {window_hours}h\n"
        hdr += f"Player: {player_name}\n"
        hdr += "----------------------------------------------\n"
        snapshot_text = hdr + "\n".join(snapshot_lines) + "\n"

        snapshot_buf = io.BytesIO(snapshot_text.encode("utf-8"))
        snapshot_file = discord.File(snapshot_buf, filename="adm_snapshot.txt")

        # ----------- post to admin channel if set ------------
        admin_ch_id = (st or {}).get("admin_channel_id")
        _log(gid, "post target resolution", {"admin_channel_id": admin_ch_id})

        if admin_ch_id:
            ch = interaction.client.get_channel(int(admin_ch_id))
            if isinstance(ch, discord.TextChannel):
                try:
                    await ch.send(content=caption, files=[map_file, snapshot_file], embeds=embeds)
                    _log(gid, "posted to admin channel", {"channel": admin_ch_id})
                    return await interaction.followup.send(
                        f"📡 Posted trace in {ch.mention}.", ephemeral=True
                    )
                except Exception as e:
                    _log(gid, "failed posting to admin channel", {"error": repr(e)})

        # Fallback to replying where invoked
        await interaction.followup.send(caption, files=[map_file, snapshot_file], embeds=embeds, ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(TraceCog(bot))
