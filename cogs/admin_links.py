# cogs/admin_links.py
from __future__ import annotations

import json
import os
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

import discord
from discord import app_commands
from discord.ext import commands

from utils.settings import load_settings, save_settings
from utils.storageClient import load_file  # used to peek local json if needed


def admin_check():
    def pred(i: discord.Interaction):
        perms = getattr(i.user, "guild_permissions", None)
        return bool(perms and (perms.administrator or perms.manage_guild))
    return app_commands.check(lambda i: pred(i))


# ---- helpers ----------------------------------------------------------------
def _read_http_json(url: str, timeout: float = 8.0) -> dict:
    """Fetch JSON from HTTP(S) with a small timeout; raises on error."""
    req = Request(url, headers={"User-Agent": "SV-Bounties/links-check"})
    with urlopen(req, timeout=timeout) as resp:  # nosec - admin-provided URL
        charset = resp.headers.get_content_charset() or "utf-8"
        raw = resp.read()
    return json.loads(raw.decode(charset, errors="replace"))


def _try_local_json(path: str) -> tuple[bool, str, dict | None]:
    """
    Attempt to read a local JSON using storageClient first, then filesystem.
    Returns (ok, detail, data_or_none)
    """
    try:
        data = load_file(path)
        if data is None:
            if os.path.isfile(path):
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
        if isinstance(data, dict):
            return True, "ok", data
        return False, "file found but not a JSON object", None
    except Exception as e:
        return False, f"{type(e).__name__}: {e}", None


def _size_hint(doc: dict) -> int:
    """Crude count of entries; looks for common containers first."""
    for k in ("links", "players", "mapping", "map", "by_id", "by_name"):
        v = doc.get(k)
        if isinstance(v, (list, dict)):
            return len(v)
    return len(doc)
# -----------------------------------------------------------------------------


class AdminLinks(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @app_commands.command(
        name="setexternallinks",
        description="Set path/URL to external linked_players.json (per guild, e.g., SV13 persistent repo)"
    )
    @admin_check()
    @app_commands.describe(path="Local path or URL to linked_players.json (leave blank to disable)")
    async def setexternallinks(self, interaction: discord.Interaction, path: str | None = None):
        gid = interaction.guild_id
        save_settings(gid, {"external_links_path": (path or None)})
        await interaction.response.send_message(
            f"✅ External links source {'set' if path else 'cleared'}."
            + (f"\n`{path}`" if path else ""),
            ephemeral=True
        )

    @app_commands.command(
        name="preferexternallinks",
        description="Prefer external linked_players over local (this guild)"
    )
    @admin_check()
    @app_commands.describe(enabled="true/false")
    async def preferexternallinks(self, interaction: discord.Interaction, enabled: bool):
        gid = interaction.guild_id
        save_settings(gid, {"prefer_external_links": bool(enabled)})
        await interaction.response.send_message(
            f"✅ `prefer_external_links` set to **{enabled}**.",
            ephemeral=True
        )

    @app_commands.command(
        name="disablelocallink",
        description="Disable this bot's /link in this guild (use Rewards bot instead)"
    )
    @admin_check()
    @app_commands.describe(enabled="true/false")
    async def disablelocallink(self, interaction: discord.Interaction, enabled: bool):
        gid = interaction.guild_id
        save_settings(gid, {"disable_local_link": bool(enabled)})
        await interaction.response.send_message(
            f"✅ `disable_local_link` set to **{enabled}**.",
            ephemeral=True
        )

    # --- NEW: quick status/diagnostics ---------------------------------------
    @app_commands.command(
        name="showlinks",
        description="Show which source will be used for linked_players and verify it loads."
    )
    @admin_check()
    async def showlinks(self, interaction: discord.Interaction):
        gid = interaction.guild_id
        st = load_settings(gid) or {}
        prefer_external = bool(st.get("prefer_external_links", True))
        disable_local = bool(st.get("disable_local_link", False))
        external_path = (st.get("external_links_path") or "").strip()

        external_is_url = external_path.lower().startswith(("http://", "https://"))
        external_present = bool(external_path)

        # Decide which source would be chosen by the bot
        use_external_first = prefer_external or disable_local
        chosen = "external" if (use_external_first and external_present) else ("local" if not disable_local else "none")

        # Test load
        src_used = "none"
        load_ok = False
        size_hint = 0
        top_keys = "—"
        detail = ""

        # 1) Try external if it's the chosen or if local is disabled
        data = None
        if chosen == "external" and external_present:
            src_used = f"external:{external_path}"
            try:
                data = _read_http_json(external_path) if external_is_url else _try_local_json(external_path)[2]
                if not isinstance(data, dict):
                    raise ValueError("top-level JSON is not an object")
                load_ok = True
                size_hint = _size_hint(data)
                top_keys = ", ".join(list(data.keys())[:10]) or "—"
                detail = "ok"
            except (HTTPError, URLError, TimeoutError, ValueError) as e:
                detail = f"external load failed: {e}"

        # 2) Fallback to local if allowed and external wasn’t chosen/failed
        if not load_ok and not disable_local:
            # common local paths: project usually stores in settings/linked_players.json
            local_candidates = []
            if external_present and not external_is_url:
                local_candidates.append(external_path)  # allow explicit local override
            local_candidates.extend([
                "settings/linked_players.json",
                "data/linked_players.json",
            ])
            for path in local_candidates:
                ok, det, doc = _try_local_json(path)
                if ok and isinstance(doc, dict):
                    src_used = f"local:{path}"
                    load_ok = True
                    data = doc
                    detail = det
                    size_hint = _size_hint(doc)
                    top_keys = ", ".join(list(doc.keys())[:10]) or "—"
                    break
            if not load_ok and not detail:
                detail = "no usable local file found"

        embed = discord.Embed(
            title="linked_players status",
            color=0x3BA55C if load_ok else 0xED4245,
            description=f"**Chosen source**: `{chosen}`",
        )
        embed.add_field(name="prefer_external_links", value=str(prefer_external))
        embed.add_field(name="disable_local_link", value=str(disable_local))
        embed.add_field(name="external_links_path", value=external_path or "—", inline=False)
        embed.add_field(name="Resolved source used", value=src_used, inline=False)
        embed.add_field(name="Load result", value=("✅ ok" if load_ok else f"❌ {detail}"), inline=False)
        if load_ok:
            embed.add_field(name="Top-level keys", value=top_keys, inline=False)
            embed.set_footer(text=f"size_hint={size_hint} • type={type(data).__name__}")

        await interaction.response.send_message(embed=embed, ephemeral=True)
    # -------------------------------------------------------------------------


async def setup(bot: commands.Bot):
    await bot.add_cog(AdminLinks(bot))
