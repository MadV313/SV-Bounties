# cogs/admin_links.py
from __future__ import annotations

import json
import os
from hashlib import blake2b
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

import discord
from discord import app_commands
from discord.ext import commands

from utils.settings import load_settings, save_settings
from utils.storageClient import load_file  # used for local JSON if present


def admin_check():
    def pred(i: discord.Interaction):
        perms = getattr(i.user, "guild_permissions", None)
        return bool(perms and (perms.administrator or perms.manage_guild))
    return app_commands.check(lambda i: pred(i))


# ----------------------------- helpers ---------------------------------------
def _read_http_json_and_text(url: str, timeout: float = 8.0) -> tuple[dict, str]:
    """Fetch JSON from HTTP(S). Returns (parsed_dict, raw_text)."""
    req = Request(url, headers={"User-Agent": "SV-Bounties/links-check"})
    with urlopen(req, timeout=timeout) as resp:  # nosec - admin-provided URL
        charset = resp.headers.get_content_charset() or "utf-8"
        raw = resp.read().decode(charset, errors="replace")
    return json.loads(raw), raw


def _try_local_json_and_text(path: str) -> tuple[bool, str, dict | None, str | None]:
    """
    Try reading a local JSON via storageClient first, then direct FS.
    Returns (ok, detail, data_or_none, raw_text_or_none).
    """
    try:
        data = load_file(path)
    except Exception:
        data = None

    if isinstance(data, dict):
        try:
            raw = json.dumps(data, ensure_ascii=False, indent=2)
        except Exception:
            raw = None
        return True, "ok", data, raw

    # Fallback to filesystem
    try:
        if os.path.isfile(path):
            with open(path, "r", encoding="utf-8") as f:
                raw = f.read()
            doc = json.loads(raw)
            if isinstance(doc, dict):
                return True, "ok", doc, raw
            return False, "file found but not a JSON object", None, None
        return False, "file not found", None, None
    except Exception as e:
        return False, f"{type(e).__name__}: {e}", None, None


def _size_hint(doc: dict) -> int:
    """Rough size based on common containers."""
    for k in ("links", "players", "mapping", "map", "by_id", "by_name"):
        v = doc.get(k)
        if isinstance(v, (list, dict)):
            return len(v)
    return len(doc)


def _content_hash(raw_text: str | None) -> str:
    if not raw_text:
        return "n/a"
    h = blake2b(raw_text.encode("utf-8", "ignore"), digest_size=8).hexdigest()
    return f"#{h}"


def _preview_json(doc: dict, raw_text: str | None, max_chars: int = 900) -> str:
    """Short snippet suitable for an embed field."""
    try:
        text = raw_text if raw_text else json.dumps(doc, ensure_ascii=False, indent=2)
    except Exception:
        text = json.dumps(doc, ensure_ascii=False)

    if len(text) > max_chars:
        return text[: max_chars - 1] + "…"
    return text
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

    # ------------------- diagnostics: show current source & snapshot ----------
    @app_commands.command(
        name="showlinks",
        description="Show which linked_players source is used, verify it loads, and include a snapshot."
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
        use_external_first = prefer_external or disable_local
        chosen = "external" if (use_external_first and external_present) else ("local" if not disable_local else "none")

        src_used = "none"
        load_ok = False
        detail = ""
        size_hint = 0
        top_keys = "—"
        content_hash = "n/a"
        snapshot = None

        data = None
        raw_text = None

        # Try external if chosen
        if chosen == "external" and external_present:
            src_used = f"external:{external_path}"
            try:
                if external_is_url:
                    data, raw_text = _read_http_json_and_text(external_path)
                else:
                    ok, det, doc, raw = _try_local_json_and_text(external_path)
                    if not ok or not isinstance(doc, dict):
                        raise ValueError(det or "failed to read local external path")
                    data, raw_text = doc, raw or json.dumps(doc, ensure_ascii=False)

                if not isinstance(data, dict):
                    raise ValueError("top-level JSON is not an object")

                load_ok = True
                size_hint = _size_hint(data)
                top_keys = ", ".join(list(data.keys())[:10]) or "—"
                content_hash = _content_hash(raw_text)
                snapshot = _preview_json(data, raw_text)
                detail = "ok"
            except (HTTPError, URLError, TimeoutError, ValueError, json.JSONDecodeError) as e:
                detail = f"external load failed: {e}"

        # Fallback to local if allowed or if external failed
        if not load_ok and not disable_local:
            candidates = []
            if external_present and not external_is_url:
                candidates.append(external_path)
            candidates.extend([
                "settings/linked_players.json",
                "data/linked_players.json",
            ])
            for path in candidates:
                ok, det, doc, raw = _try_local_json_and_text(path)
                if ok and isinstance(doc, dict):
                    src_used = f"local:{path}"
                    load_ok = True
                    data = doc
                    raw_text = raw or json.dumps(doc, ensure_ascii=False)
                    detail = det
                    size_hint = _size_hint(doc)
                    top_keys = ", ".join(list(doc.keys())[:10]) or "—"
                    content_hash = _content_hash(raw_text)
                    snapshot = _preview_json(doc, raw_text)
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
            embed.add_field(name="Content hash", value=content_hash)
            embed.add_field(
                name="Snapshot (first ~900 chars)",
                value=f"```json\n{snapshot}\n```",
                inline=False
            )
            embed.set_footer(text=f"size_hint={size_hint} • type={type(data).__name__}")

        await interaction.response.send_message(embed=embed, ephemeral=True)
    # -------------------------------------------------------------------------


async def setup(bot: commands.Bot):
    await bot.add_cog(AdminLinks(bot))
