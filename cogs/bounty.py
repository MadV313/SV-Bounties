# cogs/bounty.py — /svbounty end-to-end (set + auto-updater + award on kill)
from __future__ import annotations

import io
import re
import asyncio
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Dict, Optional, Tuple, List

import discord
from discord import app_commands
from discord.ext import commands, tasks
from PIL import Image, ImageDraw, ImageFont  # Pillow

from utils.settings import load_settings
from utils.storageClient import load_file, save_file
from utils.linking import resolve_from_any
from utils import live_pulse  # optional live updates elsewhere
from utils.bounties import create_bounty, list_open, remove_bounty_by_gamertag, remove_bounty_by_discord_id

from tracer.config import MAPS, INDEX_PATH, TRACKS_DIR
from tracer.tracker import load_track

# ---------------------------- Persistence paths ------------------------------
BOUNTIES_DB = "data/bounties.json"       # list of open/closed bounties (utils.bounties also writes here)
WALLETS_DB  = "data/wallet.json"         # persistent repo (external) via /set external previously
LINKS_DB    = "data/linked_players.json" # external links file (via /set external)

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
    g = gamertag
    return any(k.lower() == g.lower() for k in idx.keys())

def _ensure_wallet(doc: dict, discord_id: str) -> dict:
    if discord_id not in doc:
        doc[discord_id] = {"sv_tickets": 0}
    if "sv_tickets" not in doc[discord_id]:
        doc[discord_id]["sv_tickets"] = 0
    return doc

def _adjust_tickets(discord_id: str, delta: int) -> Tuple[bool, int]:
    wallets = load_file(WALLETS_DB) or {}
    wallets = _ensure_wallet(wallets, discord_id)
    cur = int(wallets[discord_id]["sv_tickets"])
    if delta < 0 and cur < (-delta):
        return False, cur
    wallets[discord_id]["sv_tickets"] = cur + delta
    save_file(WALLETS_DB, wallets)
    return True, wallets[discord_id]["sv_tickets"]

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

# ----------------------- Aggregated updates every 5 min ----------------------
class BountyUpdater:
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

            # Build a map per target (single-pin) and edit its message, or create one.
            for b in targets:
                tgt = b.get("target_gamertag") or ""
                map_key, cfg = _canon_map_and_cfg(settings.get("active_map"))
                # Load latest point for that player
                pid, doc = load_track(tgt, window_hours=48, max_points=1)
                if not doc or not doc.get("points"):
                    # nothing new; skip
                    continue
                pt = doc["points"][-1]
                x, z = float(pt["x"]), float(pt["z"])
                img = _load_map_image(map_key)
                draw = ImageDraw.Draw(img)
                px, py = _world_to_px(cfg, x, z, img.width)
                r = 9
                draw.ellipse([px - r, py - r, px + r, py + r], outline=(255, 0, 0, 255), width=4)
                draw.ellipse([px - 2, py - 2, px + 2, py + 2], fill=(255, 0, 0, 255))
                # Label
                label = f"{tgt} • {int(x)},{int(z)}"
                try:
                    draw.text((px + 12, py - 12), label, fill=(255, 255, 255, 255))
                except Exception:
                    pass

                buf = io.BytesIO()
                img.save(buf, format="PNG")
                buf.seek(0)

                url = _izurvive_url(map_key, x, z)
                embed = discord.Embed(
                    title=f"🎯 Bounty: {tgt}",
                    description=f"Last known location: **{int(x)} {int(z)}**  •  [iZurvive link]({url})",
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
                        pass  # fall through to send new

                file = discord.File(buf, filename=f"{tgt}_bounty.png")
                msg = await ch.send(embed=embed, file=file)
                # Persist message reference back to DB
                b["message"] = {"channel_id": ch.id, "message_id": msg.id}
                doc_all = load_file(BOUNTIES_DB) or {"open": [], "closed": []}
                # update record in place
                for i, ob in enumerate(doc_all["open"]):
                    if ob.get("target_gamertag","").lower() == tgt.lower() and int(ob.get("guild_id",0)) == gid:
                        doc_all["open"][i] = b
                        break
                save_file(BOUNTIES_DB, doc_all)

    @tasks.loop(minutes=5.0)
    async def loop(self):
        await asyncio.sleep(5)  # slight delay on startup
        # Update all guilds seen in bounties DB
        doc = load_file(BOUNTIES_DB) or {}
        gids = {int(b["guild_id"]) for b in doc.get("open", []) if b.get("guild_id")}
        for gid in gids:
            try:
                await self.update_guild(gid)
            except Exception as e:
                print(f"[BountyUpdater] update failed for guild {gid}: {e}")

# ---------------------------- Kill detection ---------------------------------
KILL_RE = re.compile(r"^(?P<ts>\d\d:\d\d:\d\d).*?(?P<victim>.+?) was killed by (?P<killer>.+?)\b", re.IGNORECASE)

async def check_kills_and_award(guild_id: int):
    # Try open latest ADM text (if present). If unavailable, silently skip.
    try:
        txt = Path(ADM_LATEST_PATH).read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return
    bdoc = load_file(BOUNTIES_DB) or {}
    open_bounties = bdoc.get("open", [])
    if not open_bounties:
        return

    kills: List[Tuple[str,str]] = []  # (victim, killer)
    for line in txt.splitlines()[-2000:]:  # scan last chunk
        m = KILL_RE.search(line)
        if not m:
            continue
        kills.append((m.group("victim").strip(), m.group("killer").strip()))

    changed = False
    for victim, killer in kills:
        # If victim has an open bounty in this guild, close and award killer if linked
        for b in list(open_bounties):
            if int(b.get("guild_id", 0)) != guild_id:
                continue
            if b.get("target_gamertag","").lower() == victim.lower():
                tickets = int(b.get("tickets", 0))
                # Resolve killer's discord id (if linked), else try by gamertag
                did, k_gt = resolve_from_any(guild_id, gamertag=killer)
                if not did:
                    did, k_gt = resolve_from_any(guild_id, discord_id=killer)  # unlikely, fallback
                if did:
                    ok, newbal = _adjust_tickets(str(did), +tickets)
                # remove bounty
                open_bounties.remove(b)
                changed = True
                # Announce
                settings = _guild_settings(guild_id)
                ch_id = settings.get("bounty_channel_id")
                ch = None
                if ch_id:
                    ch = bot.get_channel(int(ch_id))  # type: ignore[name-defined]
                msg = f"✅ **Bounty completed!** `{victim}` was taken out by `{killer}`. Award: **{tickets} SV tickets**."
                if ch:
                    try:
                        await ch.send(msg)
                    except Exception:
                        pass
    if changed:
        bdoc["open"] = open_bounties
        save_file(BOUNTIES_DB, bdoc)

# ------------------------------ Cog ------------------------------------------
class BountyCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        live_pulse.init(bot)  # available if used elsewhere
        self.updater = BountyUpdater(bot)
        self.updater.loop.start()

    def cog_unload(self):
        try:
            self.updater.loop.cancel()
        except Exception:
            pass

    @app_commands.command(name="svbounty", description="Set a bounty on a linked player (2–10 SV tickets).")
    @app_commands.describe(user="Discord user (if linked)", gamertag="In-game gamertag", tickets="Tickets to set (2–10)")
    async def svbounty(self, interaction: discord.Interaction, user: Optional[discord.Member] = None, gamertag: Optional[str] = None, tickets: int = 2):
        await interaction.response.defer(ephemeral=True)

        gid = interaction.guild_id
        if not gid:
            return await interaction.followup.send("❌ Guild-only command.", ephemeral=True)

        # Invoker must be linked
        inv_id = str(interaction.user.id)
        is_linked, inv_gt = _is_linked_discord(gid, inv_id)
        if not is_linked:
            return await interaction.followup.send(
                "❌ You are not linked yet. Please run the Rewards Bot `/link` command first.", ephemeral=True
            )

        # Validate tickets
        if tickets < 2 or tickets > 10:
            return await interaction.followup.send("❌ Ticket amount must be between **2** and **10**.", ephemeral=True)

        # Identify target
        target_discord_id: Optional[str] = None
        target_gt: Optional[str] = None

        if user is not None:
            # Resolve linked gamertag for this discord user
            did, gt = resolve_from_any(gid, discord_id=str(user.id))
            if not did or not gt:
                return await interaction.followup.send("❌ That Discord user is not linked to a gamertag.", ephemeral=True)
            target_discord_id = str(did)
            target_gt = gt
        elif gamertag:
            # Resolve to ensure it's a known/linked gamertag (any source)
            did, gt = resolve_from_any(gid, gamertag=gamertag)
            if not gt:
                return await interaction.followup.send("❌ That gamertag is not linked.", ephemeral=True)
            target_discord_id = str(did) if did else None
            target_gt = gt
        else:
            return await interaction.followup.send("❌ Provide either a `user` or a `gamertag`.", ephemeral=True)

        # Safeguard: must have been seen in ADM (players_index)
        if not _is_player_seen(target_gt):
            return await interaction.followup.send(
                f"❌ `{target_gt}` hasn't been seen in ADM yet — bounty can't be set.", ephemeral=True
            )

        # Check invoker has tickets
        ok, bal_after = _adjust_tickets(inv_id, -tickets)
        if not ok:
            return await interaction.followup.send(
                f"❌ Not enough SV tickets. You need **{tickets}**, but your balance is **{bal_after}**.", ephemeral=True
            )

        # Create/open bounty record
        rec = {
            "guild_id": gid,
            "set_by_discord_id": inv_id,
            "target_discord_id": target_discord_id,
            "target_gamertag": target_gt,
            "tickets": tickets,
            "created_at": _now_iso(),
            "message": None,
        }
        bdoc = load_file(BOUNTIES_DB) or {"open": [], "closed": []}
        # Prevent duplicates (same target)
        for b in bdoc["open"]:
            if int(b.get("guild_id", 0)) == gid and str(b.get("target_gamertag","")).lower() == target_gt.lower():
                return await interaction.followup.send("❌ A bounty for that player is already active.", ephemeral=True)
        bdoc["open"].append(rec)
        save_file(BOUNTIES_DB, bdoc)

        await interaction.followup.send(f"✅ Bounty set on **{target_gt}** for **{tickets} SV tickets**. Your new balance: **{bal_after}**.", ephemeral=True)

        # Trigger an immediate update for this guild
        try:
            await self.updater.update_guild(gid)
        except Exception:
            pass

    # Optional admin cleanups
    @app_commands.command(name="svbounty_remove", description="Remove an active bounty by user or gamertag.")
    async def svbounty_remove(self, interaction: discord.Interaction, user: Optional[discord.Member] = None, gamertag: Optional[str] = None):
        await interaction.response.defer(ephemeral=True)
        gid = interaction.guild_id or 0
        if user:
            n = remove_bounty_by_discord_id(str(user.id))
            return await interaction.followup.send(f"Removed **{n}** bounty(ies) for {user.mention}.", ephemeral=True)
        if gamertag:
            n = remove_bounty_by_gamertag(gamertag)
            return await interaction.followup.send(f"Removed **{n}** bounty(ies) for `{gamertag}`.", ephemeral=True)
        await interaction.followup.send("Provide `user` or `gamertag`.", ephemeral=True)

    @tasks.loop(minutes=2.0)
    async def kill_watcher(self):
        # Periodically watch for kill events and award
        doc = load_file(BOUNTIES_DB) or {}
        gids = {int(b["guild_id"]) for b in doc.get("open", []) if b.get("guild_id")}
        for gid in gids:
            try:
                await check_kills_and_award(gid)
            except Exception:
                pass

    @kill_watcher.before_loop
    async def _before_kw(self):
        await self.bot.wait_until_ready()

    @loop.before_loop  # type: ignore[name-defined]
    async def _before_updater(self):
        await self.bot.wait_until_ready()


async def setup(bot: commands.Bot):
    await bot.add_cog(BountyCog(bot))
