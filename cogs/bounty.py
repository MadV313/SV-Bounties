# cogs/bounty.py — /svbounty end-to-end (set + auto-updater + award on kill)
from __future__ import annotations

import io
import re
import json
import asyncio
from urllib.request import Request, urlopen
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Optional, Tuple, List
from pathlib import Path  # ← needed by kill scanner

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
    empty_path: Optional[str] = None
    tried: List[str] = []
    for p in _wallet_candidate_paths_for_guild(gid):
        tried.append(p)
        doc = _load_json_from_any(p)
        if isinstance(doc, dict):
            if doc:
                _log("Using wallet file (non-empty)", path=p)
                return doc, p
            empty_path = empty_path or p
    if empty_path is not None:
        _log("Using wallet file (empty)", path=empty_path)
        return {}, empty_path
    _log("No wallet file found", tried=", ".join(tried))
    return None, None

def _get_user_balance(gid: int, discord_id: str) -> Tuple[int, Optional[dict], Optional[str]]:
    wallets, path = _load_wallet_doc_and_path(gid)
    if wallets is None:
        return 0, None, None
    entry = wallets.get(discord_id, {})
    bal = entry.get("sv_tickets", 0)
    try:
        bal = int(bal)
    except Exception:
        try:
            bal = int(float(bal))
        except Exception:
            bal = 0
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
    wallets[discord_id]["sv_tickets"] = new_bal
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

# iZurvive deep link — this format jumps to the exact location.
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
        if int(b.get("guild_id", 0)) == guild_id and str(b.get("target_gamertag", "")).lower() == target.lower():
            if b.get("online", True) != online:
                b["online"] = online
                changed = True
    if changed:
        _save_db(doc)
    return changed

# ----------------------- Aggregated updates helper ---------------------------
class BountyUpdater:
    """Renderer that posts bounty maps only for ONLINE bounties."""
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._locks: Dict[int, asyncio.Lock] = {}

    def _lock_for(self, gid: int) -> asyncio.Lock:
        self._locks.setdefault(gid, asyncio.Lock())
        return self._locks[gid]

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
            targets = [b for b in doc.get("open", []) if int(b.get("guild_id", 0)) == gid and b.get("online", True)]
            _log("update_guild: start", gid=gid, online_targets=len(targets))
            if not targets:
                return

            for b in targets:
                tgt = (b.get("target_gamertag") or "").strip()
                if not tgt:
                    continue
                map_key, cfg = _canon_map_and_cfg(settings.get("active_map"))

                # latest point for this target
                _, track = load_track(tgt, window_hours=48, max_points=1)
                if not track or not track.get("points"):
                    _log("no points for target; skip", gid=gid, target=tgt)
                    continue
                pt = track["points"][-1]
                try:
                    x, z = float(pt["x"]), float(pt["z"])
                except Exception:
                    _log("bad point data; skip", gid=gid, target=tgt, pt=str(pt))
                    continue

                img = _load_map_image(map_key)
                draw = ImageDraw.Draw(img)
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

                reason = (b.get("reason") or "").strip() or None
                coords_md = _coords_link_text(map_key, x, z)
                desc = f"Last known location: {coords_md}"
                if reason:
                    desc += f"\n**Reason:** {reason[:300]}"

                fname = _safe_png_name(f"{tgt}_bounty")
                embed = discord.Embed(
                    title=f"🎯 Bounty: {tgt}",
                    description=desc,
                    color=discord.Color.red(),
                    timestamp=datetime.now(timezone.utc)
                )
                embed.set_image(url=f"attachment://{fname}")

                try:
                    file = discord.File(fp=buf, filename=fname)
                    msg = await ch.send(embed=embed, file=file)
                    _log("sent bounty map", gid=gid, target=tgt, channel_id=ch.id, message_id=msg.id)
                except Exception as e:
                    _log("send failed", gid=gid, target=tgt, err=repr(e))
                    continue

# ---------------------------- ADM parsing ------------------------------------
# Old/community style:
KILL_RE = re.compile(r"^(?P<ts>\d\d:\d\d:\d\d).*?(?P<victim>.+?) was killed by (?P<killer>.+?)\b", re.I)
# Nitrado line you pasted:
KILL_RE_NIT = re.compile(
    r'Player\s+"(?P<victim>[^"]+)"(?:\s*\(DEAD\))?.*?killed by Player\s+"(?P<killer>[^"]+)"',
    re.I,
)
CONNECT_RE = re.compile(r'Player\s+"(?P<name>[^"]+)"\s*\(id=.*?\)\s+is connected', re.I)
DISCONNECT_RE = re.compile(r'Player\s+"(?P<name>[^"]+)"\s*\(id=.*?\)\s+has been disconnected', re.I)

def _read_adm_lines(limit: int = 5000) -> List[str]:
    try:
        txt = Path(ADM_LATEST_PATH).read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return []
    lines = txt.splitlines()
    return lines[-limit:]

async def _announce_offline(bot: commands.Bot, gid: int, name: str, x: Optional[float], z: Optional[float]):
    ch_id = _guild_settings(gid).get("bounty_channel_id")
    ch = bot.get_channel(int(ch_id)) if ch_id else None
    if not isinstance(ch, (discord.TextChannel, discord.Thread)):
        return
    map_key, _ = _canon_map_and_cfg(_guild_settings(gid).get("active_map"))
    coords = "unknown"
    if x is not None and z is not None:
        coords = _coords_link_text(map_key, x, z)
    try:
        await ch.send(
            f"📡 **Bounty target offline:** **{name}** has logged out. "
            f"Last known location: {coords}."
        )
    except Exception:
        pass

# ---------------------------- Kill + status watcher --------------------------
async def check_kills_and_status(bot: commands.Bot, guild_id: int):
    """Close bounties on kill, award killer; flip online/offline on connect/disconnect."""
    lines = _read_adm_lines()
    if not lines:
        return

    doc = _db()
    open_bounties = [b for b in doc.get("open", []) if int(b.get("guild_id", 0)) == guild_id]
    if not open_bounties:
        return

    # 1) Kills (support both formats)
    kills: List[Tuple[str, str]] = []
    for ln in lines[-2000:]:
        m = KILL_RE.search(ln) or KILL_RE_NIT.search(ln)
        if m:
            kills.append((m.group("victim").strip(), m.group("killer").strip()))
    if kills:
        changed = False
        for victim, killer in kills:
            for b in list(open_bounties):
                if b.get("target_gamertag", "").lower() != victim.lower():
                    continue
                tickets = int(b.get("tickets", 0))
                # Resolve killer → discord id if linked
                did, _ = resolve_from_any(guild_id, gamertag=killer)
                if not did:
                    did, _ = resolve_from_any(guild_id, discord_id=killer)
                if did:
                    _adjust_tickets(guild_id, str(did), +tickets)

                # Remove bounty from DB
                try:
                    doc["open"].remove(b)
                except Exception:
                    pass
                changed = True

                # Announce
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
            # prune local copy
            open_bounties = [b for b in doc.get("open", []) if int(b.get("guild_id", 0)) == guild_id]

    # 2) Connection status (look backwards, first mention wins)
    if not open_bounties:
        return

    last_status: Dict[str, str] = {}  # name -> "connected"/"disconnected"
    for ln in reversed(lines):
        mc = CONNECT_RE.search(ln)
        if mc:
            nm = mc.group("name").strip()
            if nm not in last_status:
                last_status[nm] = "connected"
        md = DISCONNECT_RE.search(ln)
        if md:
            nm = md.group("name").strip()
            if nm not in last_status:
                last_status[nm] = "disconnected"
        if len(last_status) > 1000:
            break

    changed = False
    for b in open_bounties:
        tgt = b.get("target_gamertag", "")
        if not tgt:
            continue
        prev = b.get("online", True)
        now = prev
        status = last_status.get(tgt)
        if status == "connected":
            now = True
        elif status == "disconnected":
            now = False

        if now != prev:
            b["online"] = now
            changed = True
            _log("online flip", target=tgt, online=now, gid=guild_id)
            if now is False:
                # Announce offline with last coords
                _, track = load_track(tgt, window_hours=48, max_points=1)
                x = z = None
                if track and track.get("points"):
                    pt = track["points"][-1]
                    try:
                        x, z = float(pt["x"]), float(pt["z"])
                    except Exception:
                        x = z = None
                await _announce_offline(bot, guild_id, tgt, x, z)

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

        # Create/open bounty record (track online status; default True until watcher flips it)
        rec = {
            "guild_id": gid,
            "set_by_discord_id": inv_id,
            "target_discord_id": target_discord_id,
            "target_gamertag": target_gt,
            "tickets": tickets,
            "created_at": _now_iso(),
            "reason": (reason or "").strip() or None,
            "message": None,
            "online": True,
        }
        bdoc = _db()
        for b in bdoc["open"]:
            if int(b.get("guild_id", 0)) == gid and str(b.get("target_gamertag","")).lower() == target_gt.lower():
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
                    "Live updates will appear below with their most recent last known location.\n"
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

    # Every 30 minutes, if no bounties online, say so (once per tick)
    @tasks.loop(minutes=30.0)
    async def idle_announcer(self):
        doc = _db()
        by_guild: Dict[int, List[dict]] = {}
        for b in doc.get("open", []):
            by_guild.setdefault(int(b.get("guild_id", 0)), []).append(b)
        for gid, rows in by_guild.items():
            # if at least one online → skip
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
