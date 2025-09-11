# cogs/bounty.py — /svbounty end-to-end (set + auto-updater + award on kill)
from __future__ import annotations

import io
import re
import json
import asyncio
import hashlib
from urllib.request import Request, urlopen
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Optional, Tuple, List
from pathlib import Path  # ← needed by ADM parsing
import discord
from discord import app_commands
from discord.ext import commands, tasks
from PIL import Image, ImageDraw  # Pillow

from utils.settings import load_settings
from utils.storageClient import load_file, save_file
from utils.linking import resolve_from_any
from utils import live_pulse  # optional live updates elsewhere
from utils.bounties import remove_bounty_by_gamertag, remove_bounty_by_discord_id

from tracer.config import MAPS, INDEX_PATH
from tracer.tracker import load_track

# ---------------------------- Persistence paths ------------------------------
BOUNTIES_DB = "data/bounties.json"        # list of open/closed bounties
LOCAL_WALLET_PATHS = ["data/wallet.json", "wallet.json"]
LINKS_DB = "data/linked_players.json"
ADM_LATEST_PATH = "data/latest_adm.log"

# When a target is absent from PlayerList this many consecutive snapshots,
# infer they're offline and announce once.
PL_ABSENCE_THRESHOLD = 2  # ~2 snapshots ≈ a few minutes
# Force a post at least every N seconds while target is online & present
FORCE_POST_EVERY_SEC = 5 * 60

# ----------------------------- Helper dataclasses ----------------------------
@dataclass
class BountyMsgRef:
    channel_id: int
    message_id: int

@dataclass
class ActiveBounty:
    guild_id: int
    set_by_discord_id: str
    target_discord_id: Optional[str]
    target_gamertag: str
    tickets: int
    created_at: str  # ISO
    reason: Optional[str] = None
    message: Optional[BountyMsgRef] = None

# ----------------------------- Small logger ---------------------------------
def _log(msg: str, **kv):
    try:
        extra = (" " + json.dumps(kv, ensure_ascii=False, default=str)) if kv else ""
    except Exception:
        extra = f" {kv!r}" if kv else ""
    print(f"[{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}] [bounty]{extra} {msg}")

# ----------------------------- Utilities ------------------------------------
def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _guild_settings(guild_id: int) -> dict:
    return load_settings(guild_id)

def _is_linked_discord(guild_id: int, discord_id: str) -> Tuple[bool, Optional[str]]:
    did, gt = resolve_from_any(guild_id, discord_id=discord_id)
    return (did is not None and gt is not None), gt

def _is_player_seen(gamertag: str) -> bool:
    idx = load_file(INDEX_PATH) or {}
    g = gamertag or ""
    return any(k.lower() == g.lower() for k in idx.keys())

def _norm(name: str) -> str:
    """Normalize names for ADM matching."""
    return (name or "").strip().casefold()

# -------- Wallet helpers (use per-guild settings, then local fallbacks) -------
def _wallet_candidate_paths_for_guild(gid: int) -> List[str]:
    st = _guild_settings(gid) or {}
    base = (st.get("external_data_base") or "").strip().rstrip("/")
    explicit = (st.get("external_wallet_path") or "").strip()
    candidates: List[str] = []
    if explicit:
        candidates.append(explicit)
    if base:
        candidates.append(f"{base}/wallet.json")
    candidates += LOCAL_WALLET_PATHS
    return candidates

def _http_post_json(url: str, obj: dict) -> bool:
    try:
        payload = json.dumps(obj, ensure_ascii=False, indent=2).encode("utf-8")
        req = Request(
            url,
            data=payload,
            headers={"Content-Type": "application/json; charset=utf-8", "User-Agent": "SV-Bounties/wallet-write"},
            method="POST",
        )
        with urlopen(req, timeout=8.0) as resp:
            return 200 <= getattr(resp, "status", 200) < 300
    except Exception as e:
        _log("HTTP write failed", url=url, err=repr(e))
        return False

def _write_json_to_any(path: str, obj: dict) -> bool:
    if path.lower().startswith(("http://", "https://")):
        return _http_post_json(path, obj)
    try:
        return bool(save_file(path, obj))
    except Exception as e:
        _log("Local write failed", path=path, err=repr(e))
        return False

def _load_json_from_any(path: str) -> Optional[dict]:
    if path.lower().startswith(("http://", "https://")):
        try:
            req = Request(path, headers={"User-Agent": "SV-Bounties/wallet-fetch"})
            with urlopen(req, timeout=8.0) as resp:  # nosec
                charset = resp.headers.get_content_charset() or "utf-8"
                raw = resp.read().decode(charset, errors="replace")
            doc = json.loads(raw)
            return doc if isinstance(doc, dict) else None
        except Exception as e:
            _log("HTTP load failed", path=path, err=repr(e))
            return None

    try:
        data = load_file(path)
    except Exception:
        data = None
    if isinstance(data, dict):
        return data
    if isinstance(data, str):
        try:
            return json.loads(data)
        except Exception:
            return None

    try:
        p = Path(path)
        if p.is_file():
            return json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:
        _log("Local load failed", path=path, err=repr(e))
    return None

def _load_wallet_doc_and_path(gid: int) -> Tuple[Optional[dict], Optional[str]]:
    """
    Try the configured external wallet path, then base/wallet.json, then local fallbacks.
    Returns (wallet_doc, source_path-or-url). If nothing is readable, (None, None).
    If the file exists but is empty, returns ({}, path).
    """
    empty_path: Optional[str] = None
    tried: List[str] = []
    for p in _wallet_candidate_paths_for_guild(gid):
        tried.append(p)
        doc = _load_json_from_any(p)
        if isinstance(doc, dict):
            if doc:
                _log("Using wallet file (non-empty)", path=p)
                return doc, p
            # empty dict is acceptable; remember path so we can write back
            empty_path = empty_path or p
    if empty_path is not None:
        _log("Using wallet file (empty)", path=empty_path)
        return {}, empty_path
    _log("No wallet file found", tried=", ".join(tried))
    return None, None
    
def _adm_candidate_paths_for_guild(gid: int) -> List[str]:
    st = _guild_settings(gid) or {}
    base = (st.get("external_data_base") or "").strip().rstrip("/")
    # allow an explicit override if you ever add it to settings later
    explicit = (st.get("external_adm_path") or "").strip()
    candidates: List[str] = []
    if explicit:
        candidates.append(explicit)  # could be http(s)://.../latest_adm.log
    if base:
        candidates.append(f"{base}/latest_adm.log")
    # local fallback inside this repo/container
    candidates.append(ADM_LATEST_PATH)
    return candidates

def _load_text_from_any(path: str) -> Optional[str]:
    # HTTP/HTTPS
    if path.lower().startswith(("http://", "https://")):
        try:
            req = Request(path, headers={"User-Agent": "SV-Bounties/adm-fetch"})
            with urlopen(req, timeout=8.0) as resp:  # nosec
                charset = resp.headers.get_content_charset() or "utf-8"
                return resp.read().decode(charset, errors="replace")
        except Exception as e:
            _log("ADM HTTP fetch failed", path=path, err=repr(e))
            return None
    # Local
    try:
        p = Path(path)
        if p.is_file():
            return p.read_text(encoding="utf-8", errors="ignore")
    except Exception as e:
        _log("ADM local read failed", path=path, err=repr(e))
    return None

def _coerce_int(val) -> int:
    try:
        return int(val)
    except Exception:
        try:
            return int(float(val))
        except Exception:
            return 0

def _get_user_balance(gid: int, discord_id: str) -> Tuple[int, Optional[dict], Optional[str]]:
    """
    Support wallet schemas:
      A) {"<id>": {"sv_tickets": 5, ...}}
      B) {"<id>": 5}
      C) {"<id>": "5"}
    """
    wallets, path = _load_wallet_doc_and_path(gid)
    if wallets is None:
        return 0, None, None
    entry = wallets.get(discord_id, None)

    if isinstance(entry, dict):
        bal = entry.get("sv_tickets", entry.get("tickets", 0))
    else:
        bal = entry if entry is not None else 0

    bal = _coerce_int(bal)
    return bal, wallets, path

def _adjust_tickets(gid: int, discord_id: str, delta: int) -> Tuple[bool, int]:
    cur, wallets, path = _get_user_balance(gid, discord_id)
    if wallets is None or path is None:
        _log("_adjust_tickets: wallet missing", gid=gid, discord_id=discord_id)
        return False, 0
    if discord_id not in wallets:
        _log("_adjust_tickets: user not in wallet map", gid=gid, discord_id=discord_id, path=path)
        return False, cur
    if delta < 0 and cur < (-delta):
        return False, cur

    new_bal = cur + delta
    # Preserve schema on writeback
    if isinstance(wallets[discord_id], dict):
        wallets[discord_id]["sv_tickets"] = new_bal
    else:
        wallets[discord_id] = new_bal

    if not _write_json_to_any(path, wallets):
        _log("wallet write failed", path=path)
        return False, cur
    return True, new_bal

def _canon_map_and_cfg(map_name: Optional[str]) -> Tuple[str, dict]:
    key = (map_name or "livonia").lower()
    cfg = MAPS.get(key) or MAPS["livonia"]
    return key, cfg

def _world_to_px(cfg: dict, x: float, z: float, size: int) -> Tuple[int, int]:
    wminx, wmaxx = cfg["world_min_x"], cfg["world_max_x"]
    wminz, wmaxz = cfg["world_min_z"], cfg["world_max_z"]
    try:
        px = int(round(((x - wminx) / (wmaxx - wminx)) * (size - 1)))
        py = int(round(((wmaxz - z) / (wmaxz - wminz)) * (size - 1)))
        px = max(0, min(size - 1, px))
        py = max(0, min(size - 1, py))
        return px, py
    except Exception:
        return 0, 0

def _load_map_image(map_key: str, size: int = 1400) -> Image.Image:
    rel = MAPS.get(map_key, MAPS["livonia"])["image"]
    try:
        img = Image.open(rel).convert("RGBA")
    except Exception:
        img = Image.new("RGBA", (size, size), (22, 24, 27, 255))
        dr = ImageDraw.Draw(img)
        for i in range(0, size, 50):
            dr.line([(i, 0), (i, size)], fill=(60, 60, 60, 255), width=1)
            dr.line([(0, i), (size, i)], fill=(60, 60, 60, 255), width=1)
    if img.width != img.height:
        side = max(img.width, img.height)
        canvas = Image.new("RGBA", (side, side), (0, 0, 0, 255))
        canvas.paste(img, (0, 0))
        img = canvas
    return img

# iZurvive deep link — jump to exact location.
def _izurvive_url(map_key: str, x: float, z: float) -> str:
    slug = {"livonia": "livonia", "chernarus": "chernarus"}.get(map_key, "livonia")
    return f"https://www.izurvive.com/{slug}/#location={x:.2f};{z:.2f}"

def _safe_png_name(basename: str) -> str:
    """Sanitize filenames used for Discord attachments."""
    s = re.sub(r"[^A-Za-z0-9_.-]+", "_", basename).strip("._")
    if not s:
        s = "map"
    s = s[:60]
    if not s.lower().endswith(".png"):
        s += ".png"
    return s

def _coords_link_text(map_key: str, x: float, z: float) -> str:
    """Return Markdown where the coords themselves are the clickable link."""
    url = _izurvive_url(map_key, x, z)
    return f"[**{int(x)} {int(z)}**]({url})"

# ----------------------------- DB helpers ------------------------------------
def _db() -> dict:
    return load_file(BOUNTIES_DB) or {"open": [], "closed": []}

def _save_db(doc: dict):
    save_file(BOUNTIES_DB, doc)

def _set_online_state(guild_id: int, target: str, online: bool) -> bool:
    doc = _db()
    changed = False
    for b in doc["open"]:
        if int(b.get("guild_id", 0)) == guild_id and _norm(b.get("target_gamertag", "")) == _norm(target):
            if b.get("online", True) != online:
                b["online"] = online
                changed = True
    if changed:
        _save_db(doc)
    return changed

# ----------------------------- ADM parsing -----------------------------------
TS_PREFIX_OPT = r'(?:\d{4}-\d{2}-\d{2}\s+|[A-Za-z]{3}\s+\d{1,2}\s+\d{4}\s+)?'

# Kills (both common and Nitrado variants)
KILL_RE = re.compile(
    rf'^{TS_PREFIX_OPT}(?P<ts>\d\d:\d\d:\d\d).*?(?P<victim>.+?) was killed by (?P<killer>.+?)\b',
    re.I,
)
KILL_RE_NIT = re.compile(
    r'Player\s+"(?P<victim>[^"]+)"(?:\s*\(DEAD\))?.*?killed by Player\s+"(?P<killer>[^"]+)"',
    re.I,
)

# Connect / Disconnect — no look-behinds
CONNECT_RE = re.compile(
    rf'^{TS_PREFIX_OPT}(?P<ts>\d\d:\d\d:\d\d)\s+\|\s+Player\s+"(?P<name>[^"]+)"[^\n]*?(?<![A-Za-z])connected(?![A-Za-z])',
    re.I,
)
DISCONNECT_RE = re.compile(
    rf'^{TS_PREFIX_OPT}(?P<ts>\d\d:\d\d:\d\d)\s+\|\s+Player\s+"(?P<name>[^"]+)"[^\n]*?\bdisconnected\b',
    re.I,
)

# PlayerList (tolerant)
PL_HEADER_RE = re.compile(
    rf'^{TS_PREFIX_OPT}(?P<ts>\d\d:\d\d:\d\d)\s+\|\s*#####\s*Player\s*List\s*log[^:]*:\s*(?P<count>\d+)\s+players?',
    re.I,
)
PL_PLAYER_RE = re.compile(
    rf'^{TS_PREFIX_OPT}\d\d:\d\d:\d\d\s+\|\s+Player\s+"(?P<name>[^"]+)"\s*\('
    r'(?:id=[^)]*?)?\s*'
    r'pos\s*=\s*<\s*'
    r'(?P<x>-?\d+(?:\.\d+)?)\s*,\s*'
    r'(?P<z>-?\d+(?:\.\d+)?)\s*,\s*'
    r'[-\d.]+\s*'
    r'>\)',
    re.I,
)
PL_FOOTER_RE = re.compile(rf'^{TS_PREFIX_OPT}\d\d:\d\d:\d\d\s+\|\s*#####\s*$', re.I)

def _last_pos_for(lines: List[str], name_norm: str) -> Optional[Tuple[float, float]]:
    """Search the tail of ADM for the most recent single-line position for a player."""
    for ln in reversed(lines[-2000:]):
        pm = PL_PLAYER_RE.search(ln)
        if pm and _norm(pm.group("name")) == name_norm:
            try:
                return float(pm.group("x")), float(pm.group("z"))
            except Exception:
                pass
    return None
    
def _read_adm_lines(limit: int = 5000, gid_hint: Optional[int] = None) -> List[str]:
    """
    Read all viable ADM candidates, then pick the *best* one:
      - prefers files with PlayerList/connect/disconnect/kill lines
      - prefers the most recent in-file HH:MM:SS clock time
      - falls back to longest file if times are tied
    This avoids getting stuck on a placeholder file.
    """
    paths = _adm_candidate_paths_for_guild(int(gid_hint or 0)) if gid_hint else [ADM_LATEST_PATH]
    _log("adm_candidates", gid=gid_hint, candidates=paths)
    best_lines: List[str] = []
    best_score: int = -1
    chosen_path: Optional[str] = None
    TIME_RE = re.compile(r'(\d{2}):(\d{2}):(\d{2})')

    def _clock_score(ls: List[str]) -> int:
        # Extract the max HH:MM:SS seen anywhere to approximate "freshness"
        hhmmss = 0
        for ln in ls[-2000:]:
            m = TIME_RE.search(ln)
            if m:
                h, mi, s = (int(m.group(1)), int(m.group(2)), int(m.group(3)))
                hhmmss = max(hhmmss, h * 3600 + mi * 60 + s)
        return hhmmss

    for p in paths:
        txt = _load_text_from_any(p)
        if not txt:
            continue
        # Strip trailing empties and ignore trivial placeholders
        lines = [l for l in txt.splitlines() if l.strip()]
        if not lines:
            continue
        has_signal = any(
            PL_HEADER_RE.search(l) or CONNECT_RE.search(l) or DISCONNECT_RE.search(l) or
            KILL_RE.search(l) or KILL_RE_NIT.search(l)
            for l in lines[-500:]
        )
        if not has_signal and len(lines) <= 5:
            # Looks like a tiny placeholder; skip
            continue

        score = _clock_score(lines) * 100000 + min(len(lines), 100000)
        if score > best_score:
            best_score = score
            best_lines = lines
            chosen_path = p

    if chosen_path:
        _log("adm_source_chosen", path=chosen_path, lines=len(best_lines), score=best_score)
    else:
        _log("adm_no_viable_lines", candidates=paths)
    return best_lines[-limit:] if best_lines else []

def _latest_playerlist(lines: List[str]) -> Tuple[Optional[str], Dict[str, Tuple[float, float]]]:
    """
    Parse the most recent PlayerList block.
    Returns (pl_sig, {normalized_name: (x,z)}), where pl_sig is a stable content signature
    that changes whenever the PlayerList block's contents change.
    """
    pl_sig: Optional[str] = None
    players: Dict[str, Tuple[float, float]] = {}
    i = len(lines) - 1
    while i >= 0:
        mhead = PL_HEADER_RE.search(lines[i])
        if mhead:
            ts = mhead.group("ts")  # kept for logging only
            block_lines = [lines[i]]
            j = i + 1
            tmp_players: Dict[str, Tuple[float, float]] = {}
            while j < len(lines) and not PL_FOOTER_RE.search(lines[j]):
                block_lines.append(lines[j])
                pm = PL_PLAYER_RE.search(lines[j])
                if pm:
                    nm = _norm(pm.group("name"))
                    try:
                        x = float(pm.group("x")); z = float(pm.group("z"))
                        tmp_players[nm] = (x, z)
                    except Exception:
                        pass
                j += 1
            # content signature: header index + player count + small hash of block text
            h = hashlib.blake2b("\n".join(block_lines).encode("utf-8", "ignore"), digest_size=6).hexdigest()
            pl_sig = f"{i}|{len(tmp_players)}|{h}"
            players = tmp_players
            break
        i -= 1
    return pl_sig, players

# ----------------------- Renderer with PlayerList gating ----------------------
class BountyUpdater:
    """Posts a seed map, then posts further maps when target appears in PlayerList.
       Also infers offline/online from PlayerList presence as fallback."""
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._locks: Dict[int, asyncio.Lock] = {}

    def _lock_for(self, gid: int) -> asyncio.Lock:
        self._locks.setdefault(gid, asyncio.Lock())
        return self._locks[gid]

    async def _send_map(self, ch: discord.abc.Messageable, map_key: str, tgt: str, x: float, z: float, reason: Optional[str]):
        img = _load_map_image(map_key)
        draw = ImageDraw.Draw(img)
        cfg = MAPS.get(map_key, MAPS["livonia"])
        px, py = _world_to_px(cfg, x, z, img.width)
        r = 9
        draw.ellipse([px - r, py - r, px + r, py + r], outline=(255, 0, 0, 255), width=4)
        draw.ellipse([px - 2, py - 2, px + 2, py + 2], fill=(255, 0, 0, 255))
        try:
            draw.text((px + 12, py - 12), f"{tgt} • {int(x)},{int(z)}", fill=(255, 255, 255, 255))
        except Exception:
            pass

        buf = io.BytesIO()
        img.save(buf, format="PNG")
        buf.seek(0)

        coords_md = _coords_link_text(map_key, x, z)
        desc = f"Last known location: {coords_md}"
        if reason:
            desc += f"\n**Reason:** {reason[:300]}"

        fname = _safe_png_name(f"{tgt}_bounty")
        embed = discord.Embed(
            title=f"<:wanted:1414383833494847540> Bounty: {tgt}",
            description=desc,
            color=discord.Color.red(),
            timestamp=datetime.now(timezone.utc)
        )
        embed.set_image(url=f"attachment://{fname}")

        file = discord.File(fp=buf, filename=fname)
        msg = await ch.send(embed=embed, file=file)
        return msg.id

    async def update_guild(self, gid: int):
        async with self._lock_for(gid):
            settings = _guild_settings(gid)
            channel_id = settings.get("bounty_channel_id")
            if not channel_id:
                _log("update_guild: no bounty_channel_id; skip", gid=gid)
                return
            ch = self.bot.get_channel(int(channel_id))
            if not isinstance(ch, (discord.TextChannel, discord.Thread)):
                _log("update_guild: channel not found/visible", gid=gid, channel_id=channel_id)
                return

            doc = _db()
            targets = [b for b in doc.get("open", []) if int(b.get("guild_id", 0)) == gid]
            if not targets:
                return

            # Read latest ADM once per tick
            lines = _read_adm_lines(gid_hint=gid)
            pl_sig, pl_players = _latest_playerlist(lines)
            _log("playerlist parsed", gid=gid, pl_sig=pl_sig, count=len(pl_players))

            # Helper for tolerant name matching (ignores spaces/punct)
            def _key(s: str) -> str:
                return re.sub(r'[^a-z0-9]+', '', (s or '').casefold())

            by_key = { _key(nm): xy for nm, xy in pl_players.items() } if pl_players else {}

            for b in targets:
                tgt = (b.get("target_gamertag") or "").strip()
                if not tgt:
                    continue
                norm_tgt = _norm(tgt)
                map_key, _cfg = _canon_map_and_cfg(settings.get("active_map"))
                reason = (b.get("reason") or "").strip() or None

                # Ensure fields exist for stable logic
                b.setdefault("pl_absent", 0)
                b.setdefault("last_pl_seen_ts", None)
                b.setdefault("online", True)
                b.setdefault("last_state_announce", "online")

                # 1) Seed initial post (once), regardless of PlayerList.
                if not b.get("has_initial_posted"):
                    _, track = load_track(tgt, window_hours=48, max_points=1)
                    if track and track.get("points"):
                        pt = track["points"][-1]
                        try:
                            x, z = float(pt["x"]), float(pt["z"])
                            await self._send_map(ch, map_key, tgt, x, z, reason)
                            b["has_initial_posted"] = True
                            b["last_coords"] = {"x": x, "z": z}
                            _save_db(doc)
                            _log("initial map posted", gid=gid, target=tgt)
                        except Exception as e:
                            _log("initial map failed", gid=gid, target=tgt, err=repr(e))
                    continue  # wait for PlayerList-gated loop next tick

                # ---- PlayerList presence inference (fallback for conn/disc lines) ----
                # Consider presence & coords with tolerant key fallback
                coords = pl_players.get(norm_tgt)
                if coords is None and by_key:
                    coords = by_key.get(_key(tgt))

                present = bool(pl_sig and coords)
                snapshot_changed = bool(pl_sig and b.get("last_pl_seen_ts") != pl_sig)
                # Only mutate counters when the snapshot actually changed
                if snapshot_changed:
                    b["last_pl_seen_ts"] = pl_sig
                    if present:
                        b["pl_absent"] = 0
                        # If we had flipped offline earlier, announce coming back
                        if not b.get("online", True):
                            try:
                                await _announce_online(self.bot, gid, tgt)
                            except Exception:
                                pass
                            b["online"] = True
                            b["last_state_announce"] = "online"
                            _save_db(doc)
                            _log("inferred ONLINE from PlayerList", gid=gid, target=tgt, pl_sig=pl_sig)
                    else:
                        b["pl_absent"] = int(b.get("pl_absent", 0)) + 1
                        _log("PL absence tick", gid=gid, target=tgt, misses=b["pl_absent"])
                        # Only announce offline once, after threshold
                        try:
                            thr = PL_ABSENCE_THRESHOLD  # expect global
                        except NameError:
                            thr = 1  # sensible default
                        if b.get("online", True) and b["pl_absent"] >= thr and b.get("last_state_announce") != "offline":
                            lc = b.get("last_coords") or {}
                            lx = lc.get("x"); lz = lc.get("z")
                            try:
                                await _announce_offline(self.bot, gid, tgt, lx, lz)
                            except Exception:
                                pass
                            b["online"] = False
                            b["last_state_announce"] = "offline"
                            _save_db(doc)
                            _log("inferred OFFLINE from PlayerList", gid=gid, target=tgt, pl_sig=pl_sig)
                # ----------------------------------------------------------------------

                # 2) First, if not present in PlayerList, try the fallback even if marked offline.
                if not present:
                    # helpful debug the first few times
                    if pl_players and snapshot_changed:
                        _log("target not in latest playerlist",
                             gid=gid, target=tgt, pl_sig=pl_sig,
                             sample=list(pl_players.keys())[:5])

                    fallback = _last_pos_for(lines, norm_tgt)
                    if fallback:
                        x, z = fallback
                        b.setdefault("last_post_ts", 0)
                        last = b.get("last_coords") or {}
                        last_x = float(last.get("x", 0) or 0)
                        last_z = float(last.get("z", 0) or 0)
                        moved = (abs(last_x - x) > 0.1) or (abs(last_z - z) > 0.1)
                        now_ts = datetime.now(timezone.utc).timestamp()
                        should_post = moved or (now_ts - float(b.get("last_post_ts") or 0) >= FORCE_POST_EVERY_SEC)
                        if should_post:
                            try:
                                await self._send_map(ch, map_key, tgt, x, z, reason)
                                b["last_coords"] = {"x": x, "z": z}
                                b["last_state_announce"] = "online"
                                b["last_post_ts"] = now_ts
                                b["online"] = True  # revive if we had inferred offline
                                _save_db(doc)
                                _log("fallback map posted (no PlayerList block)", gid=gid, target=tgt, moved=moved)
                            except Exception as e:
                                _log("fallback send failed", gid=gid, target=tgt, err=repr(e))
                    else:
                        if not pl_sig:
                            _log("no PlayerList block and no single-line pos", gid=gid, target=tgt)
                    continue  # handled the 'not present' case

                # Only now gate on 'online' for PlayerList-driven posts
                if not b.get("online", True):
                    continue

                # Post only if this is a new snapshot OR movement happened since last post,
                # OR our cadence timer demands it.
                x, z = coords
                same_snapshot = (b.get("last_pl_ts") == pl_sig)
                moved = True
                if same_snapshot:
                    last = b.get("last_coords") or {}
                    last_x = float(last.get("x", 0) or 0)
                    last_z = float(last.get("z", 0) or 0)
                    moved = (abs(last_x - x) > 0.1) or (abs(last_z - z) > 0.1)
                
                # ensure default (for older records)
                b.setdefault("last_post_ts", 0)
                now_ts = datetime.now(timezone.utc).timestamp()
                should_post = snapshot_changed or moved
                if now_ts - float(b.get("last_post_ts") or 0) >= FORCE_POST_EVERY_SEC:
                    should_post = True
                
                if should_post:
                    try:
                        await self._send_map(ch, map_key, tgt, x, z, reason)
                        b["last_pl_ts"] = pl_sig
                        b["last_coords"] = {"x": x, "z": z}
                        b["last_state_announce"] = "online"
                        b["last_post_ts"] = now_ts
                        b["online"] = True
                        _save_db(doc)
                        _log("playerlist-gated map posted",
                             gid=gid, target=tgt, pl_sig=pl_sig,
                             moved=moved, same_snapshot=same_snapshot)
                    except Exception as e:
                        _log("send failed", gid=gid, target=tgt, err=repr(e))

# ---------------------------- Announce helpers --------------------------------
async def _announce_online(bot: commands.Bot, gid: int, name: str):
    ch_id = _guild_settings(gid).get("bounty_channel_id")
    ch = bot.get_channel(int(ch_id)) if ch_id else None
    if not isinstance(ch, (discord.TextChannel, discord.Thread)):
        return
    try:
        await ch.send(
            f"🟢 **{name}** is back **online** — tracking resumed. "
            f"Next update will appear on the next PlayerList refresh."
        )
    except Exception:
        pass

async def _announce_offline(bot: commands.Bot, gid: int, name: str, x: Optional[float], z: Optional[float]):
    ch_id = _guild_settings(gid).get("bounty_channel_id")
    ch = bot.get_channel(int(ch_id)) if ch_id else None
    if not isinstance(ch, (discord.TextChannel, discord.Thread)):
        return

    map_key, _ = _canon_map_and_cfg(_guild_settings(gid).get("active_map"))
    coords_md = "unknown"
    if x is not None and z is not None:
        coords_md = _coords_link_text(map_key, x, z)

    try:
        if x is not None and z is not None:
            img = _load_map_image(map_key)
            draw = ImageDraw.Draw(img)
            cfg = MAPS.get(map_key, MAPS["livonia"])
            px, py = _world_to_px(cfg, x, z, img.width)
            r = 9
            draw.ellipse([px - r, py - r, px + r, py + r], outline=(255, 0, 0, 255), width=4)
            draw.ellipse([px - 2, py - 2, px + 2, py + 2], fill=(255, 0, 0, 255))
            try:
                draw.text((px + 12, py - 12), f"{name} • {int(x)},{int(z)}", fill=(255, 255, 255, 255))
            except Exception:
                pass
            buf = io.BytesIO()
            img.save(buf, format="PNG")
            buf.seek(0)
            fname = _safe_png_name(f"{name}_offline_last_known")
            embed = discord.Embed(
                title=f"🔴 {name} disconnected",
                description=f"Last known location: {coords_md}",
                color=discord.Color.red(),
                timestamp=datetime.now(timezone.utc),
            )
            embed.set_image(url=f"attachment://{fname}")
            await ch.send(file=discord.File(fp=buf, filename=fname), embed=embed)
        else:
            await ch.send(f"🔴 **{name}** has **disconnected**. Last known location: {coords_md}.")
    except Exception:
        pass

# ---------------------------- Kill + status watcher --------------------------
async def check_kills_and_status(bot: commands.Bot, guild_id: int):
    """
    - Close bounties on kill, award killer (both common and Nitrado lines).
    - Flip online/offline on connect/disconnect with announcements.
    - Map updates are handled by the updater (gated by PlayerList).
    """
    lines = _read_adm_lines(gid_hint=guild_id)
    if not lines:
        return

    doc = _db()
    open_bounties = [b for b in doc.get("open", []) if int(b.get("guild_id", 0)) == guild_id]
    if not open_bounties:
        return

    # 1) Kills
    kills: List[Tuple[str, str]] = []
    for ln in lines[-2000:]:
        m = KILL_RE.search(ln) or KILL_RE_NIT.search(ln)
        if m:
            kills.append((m.group("victim").strip(), m.group("killer").strip()))
    if kills:
        changed = False
        for victim, killer in kills:
            for b in list(open_bounties):
                if _norm(b.get("target_gamertag", "")) != _norm(victim):
                    continue
                tickets = int(b.get("tickets", 0))
                did, _ = resolve_from_any(guild_id, gamertag=killer)
                if not did:
                    did, _ = resolve_from_any(guild_id, discord_id=killer)
                if did:
                    _adjust_tickets(guild_id, str(did), +tickets)

                try:
                    doc["open"].remove(b)
                except Exception:
                    pass
                changed = True

                ch_id = _guild_settings(guild_id).get("bounty_channel_id")
                ch = bot.get_channel(int(ch_id)) if ch_id else None
                if isinstance(ch, (discord.TextChannel, discord.Thread)):
                    try:
                        await ch.send(
                            "📢 **Attention survivors!**\n"
                            f"The bounty for **{victim}** has been claimed by **{killer}** and they have been "
                            f"duly awarded **{tickets} SV tickets**.\n"
                            "Keep your eyes peeled for more bounties!"
                        )
                    except Exception:
                        pass
        if changed:
            _save_db(doc)
            open_bounties = [b for b in doc.get("open", []) if int(b.get("guild_id", 0)) == guild_id]

    # 2) Connection status + announcements (regex-based)
    # Updated: 2025-09-10
    if not open_bounties:
        return
    
    last_status: Dict[str, str] = {}        # norm(name) -> "connected"/"disconnected"
    last_status_ts: Dict[str, str] = {}     # norm(name) -> "HH:MM:SS"
    
    for ln in lines:  # scan in file order; keep the latest clock time per player
        m = CONNECT_RE.search(ln)
        if m:
            nm = _norm(m.group("name"))
            ts = m.group("ts")
            if ts >= last_status_ts.get(nm, "00:00:00"):
                last_status[nm] = "connected"
                last_status_ts[nm] = ts
            continue
    
        m = DISCONNECT_RE.search(ln)
        if m:
            nm = _norm(m.group("name"))
            ts = m.group("ts")
            if ts >= last_status_ts.get(nm, "00:00:00"):
                last_status[nm] = "disconnected"
                last_status_ts[nm] = ts

    changed = False
    for b in open_bounties:
        tgt = b.get("target_gamertag", "")
        if not tgt:
            continue
        prev_online = bool(b.get("online", True))
        last_flag = b.get("last_state_announce")
    
        status = last_status.get(_norm(tgt))
        _dec_ts = last_status_ts.get(_norm(tgt))
        _log("status decision", gid=guild_id, target=tgt, status=status, at=_dec_ts, prev_online=prev_online)

        now_online = prev_online
        if status == "connected":
            now_online = True
        elif status == "disconnected":
            now_online = False

        if now_online != prev_online:
            b["online"] = now_online
            changed = True
            _log("online flip", target=tgt, online=now_online, gid=guild_id)

        if status == "connected" and last_flag != "online":
            try:
                await _announce_online(bot, guild_id, tgt)
            except Exception:
                pass
            b["last_state_announce"] = "online"
            # reset PL absence if we got an explicit connect
            b["pl_absent"] = 0
            changed = True

        if status == "disconnected" and last_flag != "offline":
            lx = lz = None
            lc = b.get("last_coords") or {}
            if "x" in lc and "z" in lc:
                try:
                    lx = float(lc["x"]); lz = float(lc["z"])
                except Exception:
                    lx = lz = None
            if lx is None or lz is None:
                _, track = load_track(tgt, window_hours=48, max_points=1)
                if track and track.get("points"):
                    pt = track["points"][-1]
                    try:
                        lx, lz = float(pt["x"]), float(pt["z"])
                    except Exception:
                        lx = lz = None
            try:
                await _announce_offline(bot, guild_id, tgt, lx, lz)
            except Exception:
                pass
            b["last_state_announce"] = "offline"
            changed = True

    if changed:
        _save_db(doc)

# ------------------------------ Cog ------------------------------------------
class BountyCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        live_pulse.init(bot)  # ok if unused elsewhere
        self.updater = BountyUpdater(bot)

        self.bounty_updater.start()
        self.kill_watcher.start()
        self.idle_announcer.start()

    def cog_unload(self):
        for loop_task in (self.bounty_updater, self.kill_watcher, self.idle_announcer):
            try:
                loop_task.cancel()
            except Exception:
                pass

    @app_commands.command(name="svbounty", description="Set a bounty on a player (2–10 SV tickets).")
    @app_commands.describe(
        user="Discord user (if linked)",
        gamertag="Exact in-game gamertag (include digits immediately after the name, no space)",
        tickets="Tickets to set (2–10)",
        reason="Why are you placing this bounty? (optional)"
    )
    async def svbounty(
        self,
        interaction: discord.Interaction,
        user: Optional[discord.Member] = None,
        gamertag: Optional[str] = None,
        tickets: int = 2,
        reason: Optional[str] = None
    ):
        _log("svbounty invoked",
             guild_id=interaction.guild_id,
             channel_id=interaction.channel_id,
             user_id=getattr(interaction.user, "id", None),
             target_user_id=getattr(user, "id", None),
             gamertag=gamertag, tickets=tickets)

        await interaction.response.defer(ephemeral=True)

        gid = interaction.guild_id
        if not gid:
            return await interaction.followup.send("❌ Guild-only command.", ephemeral=True)

        settings = _guild_settings(gid)
        bounty_channel_id = settings.get("bounty_channel_id")

        if not bounty_channel_id:
            return await interaction.followup.send(
                "⚠️ No bounty channel is set yet. Please run `/setchannels` to configure `bounty_channel`.",
                ephemeral=True
            )
        if interaction.channel_id != int(bounty_channel_id):
            ch = self.bot.get_channel(int(bounty_channel_id))
            where = f"<#{bounty_channel_id}>" if ch else "`the configured bounty channel`"
            return await interaction.followup.send(
                f"⚠️ This command can only be used in {where}.",
                ephemeral=True
            )

        inv_id = str(interaction.user.id)
        is_linked, _ = _is_linked_discord(gid, inv_id)
        if not is_linked:
            return await interaction.followup.send(
                "❌ You are not linked yet. Please use the Rewards Bot `/link` command first.",
                ephemeral=True
            )

        if tickets < 2 or tickets > 10:
            return await interaction.followup.send(
                "❌ Ticket amount must be between **2** and **10**.",
                ephemeral=True
            )

        # Identify/validate target
        target_discord_id: Optional[str] = None
        target_gt: Optional[str] = None

        if user is not None:
            did, gt = resolve_from_any(gid, discord_id=str(user.id))
            if not did or not gt:
                return await interaction.followup.send(
                    "❌ That Discord user is not linked to a gamertag.",
                    ephemeral=True
                )
            target_discord_id = str(did)
            target_gt = gt
        elif gamertag:
            did, gt = resolve_from_any(gid, gamertag=gamertag)
            if gt:
                target_discord_id = str(did) if did else None
                target_gt = gt
            else:
                if _is_player_seen(gamertag):
                    target_gt = gamertag
                else:
                    return await interaction.followup.send(
                        "❌ That gamertag wasn’t found as linked **or** in recent ADM scans.\n"
                        "➡️ Use the exact in-game spelling; digits immediately after the name (no space).",
                        ephemeral=True
                    )
        else:
            return await interaction.followup.send("❌ Provide either a `user` or a `gamertag`.", ephemeral=True)

        if not _is_player_seen(target_gt):
            did_check, _gt_check = resolve_from_any(gid, gamertag=target_gt)
            if not did_check:
                return await interaction.followup.send(
                    f"❌ `{target_gt}` hasn’t been seen in ADM yet. "
                    "If you’re using the gamertag path, make sure digits come right after the name (no space).",
                    ephemeral=True
                )

        ok, bal_after = _adjust_tickets(gid, inv_id, -tickets)
        if not ok:
            cur, wallets, path = _get_user_balance(gid, inv_id)
            if wallets is None:
                hint = " Wallet file not found."
            elif inv_id not in wallets:
                hint = " Your wallet entry was not found."
            else:
                hint = ""
            return await interaction.followup.send(
                f"❌ Not enough SV tickets. You need **{tickets}**, but your balance is **{cur}**.{hint}",
                ephemeral=True
            )

        # Create/open bounty record
        rec = {
            "guild_id": gid,
            "set_by_discord_id": inv_id,
            "target_discord_id": target_discord_id,
            "target_gamertag": target_gt,
            "tickets": tickets,
            "created_at": _now_iso(),
            "reason": (reason or "").strip() or None,
            "message": None,
            # Tracking fields for gating + announcements
            "online": True,                  # default true until watcher flips
            "has_initial_posted": False,     # first seed image regardless of PlayerList
            "last_pl_ts": None,              # last PlayerList timestamp we posted for
            "last_state_announce": "online", # dedupe “online/offline” notices
            "last_coords": None,             # {"x": float, "z": float}
            "pl_absent": 0,                  # consecutive PlayerList misses
            "last_pl_seen_ts": None,
            "last_post_ts": 0,               # cadence guard
        }
        bdoc = _db()
        for b in bdoc["open"]:
            if int(b.get("guild_id", 0)) == gid and _norm(b.get("target_gamertag","")) == _norm(target_gt or ""):
                _adjust_tickets(gid, inv_id, +tickets)
                return await interaction.followup.send("❌ A bounty for that player is already active.", ephemeral=True)
        bdoc["open"].append(rec)
        _save_db(bdoc)
        _log("bounty recorded", guild_id=gid, target=target_gt, tickets=tickets)

        extra = ""
        if not target_discord_id:
            extra = ("\nℹ️ Target isn’t linked; tracking will rely on ADM updates only. "
                     "Make sure the gamertag formatting matches in-game (digits right after the name, no space).")
        await interaction.followup.send(
            f"✅ Bounty set on **{target_gt}** for **{tickets} SV tickets**.{extra} "
            f"Your new balance: **{bal_after}**.",
            ephemeral=True
        )

        ch = self.bot.get_channel(int(bounty_channel_id))
        if isinstance(ch, (discord.TextChannel, discord.Thread)):
            try:
                pretty_reason = rec["reason"] or "_no reason provided_"
                await ch.send(
                    "📢 **Attention survivors!**\n"
                    f"A new bounty has been set for **{target_gt}** by <@{inv_id}> for **{tickets} SV tickets**.\n"
                    f"**Reason:** {pretty_reason}\n"
                    "Live updates will appear below. First post shows last known location; "
                    "subsequent updates will appear when the PlayerList refreshes.\n"
                    "**Stay Frosty!**"
                )
            except Exception as e:
                _log("announcement send failed", guild_id=gid, channel_id=ch.id, err=repr(e))

        try:
            await self.updater.update_guild(gid)
        except Exception as e:
            _log("immediate update failed", guild_id=gid, err=repr(e))

    @app_commands.command(name="svbounty_remove", description="Remove an active bounty by user or gamertag.")
    async def svbounty_remove(
        self,
        interaction: discord.Interaction,
        user: Optional[discord.Member] = None,
        gamertag: Optional[str] = None
    ):
        await interaction.response.defer(ephemeral=True)
        if user:
            n = remove_bounty_by_discord_id(str(user.id))
            return await interaction.followup.send(f"Removed **{n}** bounty(ies) for {user.mention}.", ephemeral=True)
        if gamertag:
            n = remove_bounty_by_gamertag(gamertag)
            return await interaction.followup.send(f"Removed **{n}** bounty(ies) for `{gamertag}`.", ephemeral=True)
        await interaction.followup.send("Provide `user` or `gamertag`.", ephemeral=True)

    # ------------------ Background loops owned by the Cog ------------------
    @tasks.loop(minutes=5.0)
    async def bounty_updater(self):
        doc = _db()
        gids = {int(b["guild_id"]) for b in doc.get("open", []) if b.get("guild_id")}
        _log("bounty_updater tick", guilds=list(gids))
        for gid in gids:
            try:
                await self.updater.update_guild(gid)
            except Exception as e:
                _log("update failed", gid=gid, err=repr(e))

    @bounty_updater.before_loop
    async def _before_bounty_updater(self):
        await self.bot.wait_until_ready()
        await asyncio.sleep(5)

    @tasks.loop(minutes=2.0)
    async def kill_watcher(self):
        doc = _db()
        gids = {int(b["guild_id"]) for b in doc.get("open", []) if b.get("guild_id")}
        for gid in gids:
            try:
                await check_kills_and_status(self.bot, gid)
            except Exception as e:
                _log("kill/status watcher error", gid=gid, err=repr(e))

    @kill_watcher.before_loop
    async def _before_kw(self):
        await self.bot.wait_until_ready()

    # Every 15 minutes, if no bounties online, say so (once per tick)
    @tasks.loop(minutes=15.0)
    async def idle_announcer(self):
        doc = _db()
        by_guild: Dict[int, List[dict]] = {}
        for b in doc.get("open", []):
            by_guild.setdefault(int(b.get("guild_id", 0)), []).append(b)
        for gid, rows in by_guild.items():
            if any(r.get("online", True) for r in rows):
                continue
            ch_id = _guild_settings(gid).get("bounty_channel_id")
            ch = self.bot.get_channel(int(ch_id)) if ch_id else None
            if isinstance(ch, (discord.TextChannel, discord.Thread)):
                try:
                    await ch.send("⏸️ There are **no current bounties actively online**.")
                except Exception:
                    pass

    @idle_announcer.before_loop
    async def _before_idle(self):
        await self.bot.wait_until_ready()


async def setup(bot: commands.Bot):
    await bot.add_cog(BountyCog(bot))
