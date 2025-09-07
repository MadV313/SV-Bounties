# cogs/bounty.py — /svbounty end-to-end (set + auto-updater + award on kill)
from __future__ import annotations

import io
import re
import json
import asyncio
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
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
from utils.bounties import create_bounty, list_open, remove_bounty_by_gamertag, remove_bounty_by_discord_id

from tracer.config import MAPS, INDEX_PATH, TRACKS_DIR
from tracer.tracker import load_track

# ---------------------------- Persistence paths ------------------------------
BOUNTIES_DB = "data/bounties.json"        # list of open/closed bounties (utils.bounties also writes here)
# Local fallbacks if no external base/path is configured
LOCAL_WALLET_PATHS = ["data/wallet.json", "wallet.json"]
LINKS_DB     = "data/linked_players.json" # external links file (via /set external)

# Optional: ADM latest path for kill detection (can be overridden via settings later)
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

# ----------------------------- Utilities ------------------------------------
def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _guild_settings(guild_id: int) -> dict:
    return load_settings(guild_id)

def _is_linked_discord(guild_id: int, discord_id: str) -> Tuple[bool, Optional[str]]:
    # resolve_from_any returns (discord_id or None, gamertag or None)
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

def _load_json_from_any(path: str) -> Optional[dict]:
    """
    Load a JSON object from either a local path (via storageClient / FS) or an HTTP(S) URL.
    Returns a dict on success, otherwise None.
    """
    if path.lower().startswith(("http://", "https://")):
        try:
            req = Request(path, headers={"User-Agent": "SV-Bounties/wallet-fetch"})
            with urlopen(req, timeout=8.0) as resp:  # nosec - admin-provided URL
                charset = resp.headers.get_content_charset() or "utf-8"
                raw = resp.read().decode(charset, errors="replace")
            doc = json.loads(raw)
            if isinstance(doc, dict):
                return doc
            return None
        except Exception as e:
            print(f"[bounty] HTTP load failed for {path}: {type(e).__name__}: {e}")
            return None

    # Local: try storageClient first
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

    # Final local FS fallback
    try:
        p = Path(path)
        if p.is_file():
            return json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"[bounty] Local load failed for {path}: {type(e).__name__}: {e}")
    return None

def _load_wallet_doc_and_path(gid: int) -> Tuple[Optional[dict], Optional[str]]:
    """
    Try the configured external paths first, then locals.
    Return (doc, path) for the first JSON object found.
    If none found, return (None, None).
    """
    empty_path: Optional[str] = None
    tried: List[str] = []
    for p in _wallet_candidate_paths_for_guild(gid):
        tried.append(p)
        doc = _load_json_from_any(p)
        if isinstance(doc, dict):
            if doc:
                print(f"[bounty] Using wallet file: {p} (non-empty)")
                return doc, p
            empty_path = empty_path or p
    if empty_path is not None:
        print(f"[bounty] Using wallet file: {empty_path} (empty)")
        return {}, empty_path
    print(f"[bounty] No wallet file found. Tried: {', '.join(tried)}")
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
    """
    Adjust tickets only if:
      - wallet file is present
      - user already has a wallet entry
    We DO NOT auto-create a new user entry (to avoid writing to a wrong mount).
    """
    cur, wallets, path = _get_user_balance(gid, discord_id)
    if wallets is None or path is None:
        print("[bounty] _adjust_tickets: wallet file missing; refusing to write")
        return False, 0

    if discord_id not in wallets:
        print(f"[bounty] _adjust_tickets: user {discord_id} not in wallet map for {path}")
        return False, cur

    if delta < 0 and cur < (-delta):
        return False, cur

    new_bal = cur + delta
    wallets[discord_id]["sv_tickets"] = new_bal
    save_file(path, wallets)
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

def _izurvive_url(map_key: str, x: float, z: float) -> str:
    slug = {"livonia":"livonia","chernarus":"chernarus"}.get(map_key, "livonia")
    return f"https://www.izurvive.com/{slug}/#loc:{int(x)},{int(z)}"

# ----------------------- Aggregated updates helper ---------------------------
class BountyUpdater:
    """Light wrapper that can render/update one guild on demand."""
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._locks: Dict[int, asyncio.Lock] = {}  # per guild

    def _lock_for(self, gid: int) -> asyncio.Lock:
        self._locks.setdefault(gid, asyncio.Lock())
        return self._locks[gid]

    async def update_guild(self, gid: int):
        async with self._lock_for(gid):
            settings = _guild_settings(gid)
            channel_id = settings.get("bounty_channel_id")
            if not channel_id:
                return
            ch = self.bot.get_channel(int(channel_id))
            if not isinstance(ch, (discord.TextChannel, discord.Thread)):
                return

            open_bounties = (load_file(BOUNTIES_DB) or {}).get("open", [])
            targets = [b for b in open_bounties if int(b.get("guild_id", 0)) == gid]
            if not targets:
                return

            for b in targets:
                tgt = b.get("target_gamertag") or ""
                map_key, cfg = _canon_map_and_cfg(settings.get("active_map"))

                # latest point
                _, doc = load_track(tgt, window_hours=48, max_points=1)
                if not doc or not doc.get("points"):
                    continue
                pt = doc["points"][-1]
                x, z = float(pt["x"]), float(pt["z"])

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

                url = _izurvive_url(map_key, x, z)
                reason = b.get("reason")
                desc = f"Last known location: **{int(x)} {int(z)}**  •  [iZurvive link]({url})"
                if reason:
                    desc += f"\n**Reason:** {reason[:300]}"

                embed = discord.Embed(
                    title=f"🎯 Bounty: {tgt}",
                    description=desc,
                    color=discord.Color.red(),
                    timestamp=datetime.now(timezone.utc)
                )

                # Edit or create the bounty message per target
                msg_id = b.get("message", {}).get("message_id")
                if msg_id:
                    try:
                        msg = await ch.fetch_message(int(msg_id))
                        await msg.edit(embed=embed, attachments=[discord.File(buf, filename=f"{tgt}_bounty.png")])
                        continue
                    except Exception:
                        pass  # will send new message

                file = discord.File(buf, filename=f"{tgt}_bounty.png")
                msg = await ch.send(embed=embed, file=file)
                # Persist message reference
                b["message"] = {"channel_id": ch.id, "message_id": msg.id}
                db = load_file(BOUNTIES_DB) or {"open": [], "closed": []}
                for i, ob in enumerate(db["open"]):
                    if ob.get("target_gamertag","").lower() == tgt.lower() and int(ob.get("guild_id",0)) == gid:
                        db["open"][i] = b
                        break
                save_file(BOUNTIES_DB, db)

# ---------------------------- Kill detection ---------------------------------
KILL_RE = re.compile(
    r"^(?P<ts>\d\d:\d\d:\d\d).*?(?P<victim>.+?) was killed by (?P<killer>.+?)\b",
    re.IGNORECASE
)

async def check_kills_and_award(bot: commands.Bot, guild_id: int):
    """Close bounties whose target was killed and award killer."""
    try:
        txt = Path(ADM_LATEST_PATH).read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return

    bdoc = load_file(BOUNTIES_DB) or {}
    open_bounties = bdoc.get("open", [])
    if not open_bounties:
        return

    kills: List[Tuple[str,str]] = []
    for line in txt.splitlines()[-2000:]:
        m = KILL_RE.search(line)
        if m:
            kills.append((m.group("victim").strip(), m.group("killer").strip()))

    if not kills:
        return

    changed = False
    for victim, killer in kills:
        for b in list(open_bounties):
            if int(b.get("guild_id", 0)) != guild_id:
                continue
            if b.get("target_gamertag","").lower() != victim.lower():
                continue

            tickets = int(b.get("tickets", 0))
            # Resolve killer → discord id if linked
            did, _ = resolve_from_any(guild_id, gamertag=killer)
            if not did:
                did, _ = resolve_from_any(guild_id, discord_id=killer)  # fallback

            if did:
                _adjust_tickets(guild_id, str(did), +tickets)

            open_bounties.remove(b)
            changed = True

            # Announce in bounty channel (snazzy)
            ch_id = _guild_settings(guild_id).get("bounty_channel_id")
            ch = bot.get_channel(int(ch_id)) if ch_id else None
            if isinstance(ch, (discord.TextChannel, discord.Thread)):
                try:
                    await ch.send(
                        "📢 **Attention survivors!**\n"
                        f"The bounty for **{victim}** has been claimed by **{killer}** and they have been "
                        f"duly awarded **{tickets} SV tickets** for bringing down the culprit!\n"
                        "Be on the look out for more bounties as they update here!"
                    )
                except Exception:
                    pass

    if changed:
        bdoc["open"] = open_bounties
        save_file(BOUNTIES_DB, bdoc)

# ------------------------------ Cog ------------------------------------------
class BountyCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        live_pulse.init(bot)  # ok if unused elsewhere
        self.updater = BountyUpdater(bot)

        # Start both loops owned by the Cog
        self.bounty_updater.start()
        self.kill_watcher.start()

    def cog_unload(self):
        for loop_task in (self.bounty_updater, self.kill_watcher):
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
        await interaction.response.defer(ephemeral=True)

        gid = interaction.guild_id
        if not gid:
            return await interaction.followup.send("❌ Guild-only command.", ephemeral=True)

        settings = _guild_settings(gid)
        bounty_channel_id = settings.get("bounty_channel_id")

        # Require command to be used in the configured bounty channel
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

        # Invoker must be linked
        inv_id = str(interaction.user.id)
        is_linked, _ = _is_linked_discord(gid, inv_id)
        if not is_linked:
            return await interaction.followup.send(
                "❌ You are not linked yet. Please use the Rewards Bot `/link` command first.",
                ephemeral=True
            )

        # Validate tickets
        if tickets < 2 or tickets > 10:
            return await interaction.followup.send(
                "❌ Ticket amount must be between **2** and **10**.",
                ephemeral=True
            )

        # Identify/validate target
        target_discord_id: Optional[str] = None
        target_gt: Optional[str] = None

        if user is not None:
            # If a Discord user is provided, they must be linked to a gamertag
            did, gt = resolve_from_any(gid, discord_id=str(user.id))
            if not did or not gt:
                return await interaction.followup.send(
                    "❌ That Discord user is not linked to a gamertag.",
                    ephemeral=True
                )
            target_discord_id = str(did)
            target_gt = gt
        elif gamertag:
            # Target does NOT need to be linked; accept if linked OR seen in ADM
            did, gt = resolve_from_any(gid, gamertag=gamertag)
            if gt:
                target_discord_id = str(did) if did else None
                target_gt = gt
            else:
                # Not linked — check ADM index
                if _is_player_seen(gamertag):
                    target_gt = gamertag
                else:
                    return await interaction.followup.send(
                        "❌ That gamertag wasn’t found as linked **or** in recent ADM scans.\n"
                        "➡️ Please use the **exact in-game spelling**, and include digits **immediately** after the name "
                        "(no space) so it matches our scanner.",
                        ephemeral=True
                    )
        else:
            return await interaction.followup.send(
                "❌ Provide either a `user` or a `gamertag`.",
                ephemeral=True
            )

        # Double-check we’ve seen them in ADM at least once if they’re not linked
        if not _is_player_seen(target_gt):
            # Still allow if they are linked (resolve_from_any found them),
            # otherwise block (likely a misspelling).
            did_check, _gt_check = resolve_from_any(gid, gamertag=target_gt)
            if not did_check:
                return await interaction.followup.send(
                    f"❌ `{target_gt}` hasn’t been seen in ADM yet. "
                    "If you’re using the gamertag path, be sure it’s exactly as in game and digits come right after the name (no space).",
                    ephemeral=True
                )

        # Check invoker has tickets (do NOT create wallet entries on the fly)
        ok, bal_after = _adjust_tickets(gid, inv_id, -tickets)
        if not ok:
            # Give a clear hint if the wallet file wasn't present or user not found
            cur, wallets, path = _get_user_balance(gid, inv_id)
            if wallets is None:
                hint = " Wallet file not found on this bot host or remote URL."
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
        }
        bdoc = load_file(BOUNTIES_DB) or {"open": [], "closed": []}
        # Prevent duplicates (same target in this guild)
        for b in bdoc["open"]:
            if int(b.get("guild_id", 0)) == gid and str(b.get("target_gamertag","")).lower() == target_gt.lower():
                # refund since we already deducted
                _adjust_tickets(gid, inv_id, +tickets)
                return await interaction.followup.send(
                    "❌ A bounty for that player is already active.",
                    ephemeral=True
                )
        bdoc["open"].append(rec)
        save_file(BOUNTIES_DB, bdoc)

        # Ephemeral confirmation to invoker
        extra = ""
        if not target_discord_id:
            extra = (
                "\nℹ️ Target isn’t linked; tracking will rely on ADM updates only. "
                "Make sure the gamertag formatting matches in-game (digits right after the name, no space)."
            )
        await interaction.followup.send(
            f"✅ Bounty set on **{target_gt}** for **{tickets} SV tickets**.{extra} "
            f"Your new balance: **{bal_after}**.",
            ephemeral=True
        )

        # Public announcement in the bounty channel (snazzy)
        ch = self.bot.get_channel(int(bounty_channel_id))
        if isinstance(ch, (discord.TextChannel, discord.Thread)):
            try:
                pretty_reason = rec["reason"] or "_no reason provided_"
                await ch.send(
                    "📢 **Attention survivors!**\n"
                    f"A new bounty has been set for **{target_gt}** by <@{inv_id}> for **{tickets} SV tickets**.\n"
                    f"**Reason:** {pretty_reason}\n"
                    "Be on the look out for their live updates here in this channel — below is their most recent last known location!\n"
                    "**Stay Frosty!**"
                )
            except Exception:
                pass

        # Trigger an immediate update for this guild (posts the live map under the announcement)
        try:
            await self.updater.update_guild(gid)
        except Exception:
            pass

    # Optional admin cleanups
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
            return await interaction.followup.send(
                f"Removed **{n}** bounty(ies) for {user.mention}.",
                ephemeral=True
            )
        if gamertag:
            n = remove_bounty_by_gamertag(gamertag)
            return await interaction.followup.send(
                f"Removed **{n}** bounty(ies) for `{gamertag}`.",
                ephemeral=True
            )
        await interaction.followup.send(
            "Provide `user` or `gamertag`.",
            ephemeral=True
        )

    # ------------------ Background loops owned by the Cog ------------------
    @tasks.loop(minutes=5.0)
    async def bounty_updater(self):
        # Update all guilds that currently have open bounties
        doc = load_file(BOUNTIES_DB) or {}
        gids = {int(b["guild_id"]) for b in doc.get("open", []) if b.get("guild_id")}
        for gid in gids:
            try:
                await self.updater.update_guild(gid)
            except Exception as e:
                print(f"[BountyCog] update failed for guild {gid}: {e}")

    @bounty_updater.before_loop
    async def _before_bounty_updater(self):
        await self.bot.wait_until_ready()
        await asyncio.sleep(5)

    @tasks.loop(minutes=2.0)
    async def kill_watcher(self):
        # Periodically watch for kill events and award
        doc = load_file(BOUNTIES_DB) or {}
        gids = {int(b["guild_id"]) for b in doc.get("open", []) if b.get("guild_id")}
        for gid in gids:
            try:
                await check_kills_and_award(self.bot, gid)
            except Exception:
                pass

    @kill_watcher.before_loop
    async def _before_kw(self):
        await self.bot.wait_until_ready()


async def setup(bot: commands.Bot):
    await bot.add_cog(BountyCog(bot))
