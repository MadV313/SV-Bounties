# cogs/admin_links.py
from __future__ import annotations

import base64
import json
import os
from hashlib import blake2b
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from typing import Any, Tuple

import discord
from discord import app_commands
from discord.ext import commands

from utils.settings import load_settings, save_settings
from utils.storageClient import load_file, save_file  # used for JSON (local or remote)


def admin_check():
    def pred(i: discord.Interaction):
        perms = getattr(i.user, "guild_permissions", None)
        return bool(perms and (perms.administrator or perms.manage_guild))
    return app_commands.check(lambda i: pred(i))


# ============================= guardrail helpers =============================

def _looks_base64(s: str) -> bool:
    s = s.strip()
    if not s:
        return False
    for ch in s:
        if ch not in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=\n\r\t ":
            return False
    return True


def unwrap_links_json(obj: Any) -> Tuple[Any, bool, str]:
    """
    Robustly unwrap a links file that may be wrapped as:
      {"data": "<base64-json>"}  or  {"data": "<raw json string>"}  or  {"data": {...}}
    Returns (plain_obj, changed, reason).
    Re-applies until fully unwrapped (handles accidental double-wraps).
    """
    changed_any = False
    reason = "no wrapper"
    seen = 0

    while isinstance(obj, dict) and "data" in obj and len(obj) == 1 and seen < 3:
        seen += 1
        d = obj["data"]
        # Already proper dict/list
        if isinstance(d, (dict, list)):
            obj = d
            changed_any = True
            reason = "unwrapped nested dict/list"
            continue
        # Try base64 → JSON
        if isinstance(d, str) and _looks_base64(d):
            try:
                decoded = base64.b64decode(d, validate=True).decode("utf-8", "ignore")
                obj = json.loads(decoded)
                changed_any = True
                reason = "unwrapped base64→JSON"
                continue
            except Exception:
                # fall through to try raw JSON string
                pass
        # Try raw JSON string
        if isinstance(d, str):
            try:
                obj = json.loads(d)
                changed_any = True
                reason = "unwrapped raw JSON string"
                continue
            except Exception:
                # not decodable; stop
                break
        break

    return obj, changed_any, reason


def _maybe_decode_wrapped_base64(doc: dict) -> tuple[dict, str | None, str | None]:
    """
    If doc is {"data": "<base64-json>"} return (decoded_obj, 'data', decoded_text).
    Otherwise return (doc, None, None).
    (Kept for backwards-compat with the existing show embed logic.)
    """
    if isinstance(doc, dict) and "data" in doc and isinstance(doc["data"], str):
        blob = doc["data"]
        if _looks_base64(blob):
            try:
                raw = base64.b64decode(blob)
                txt = raw.decode("utf-8", "ignore")
                decoded = json.loads(txt)
                if isinstance(decoded, dict):
                    return decoded, "data", txt
            except Exception:
                pass
    return doc, None, None


# ----------------------------- I/O helpers -----------------------------------

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

    if isinstance(data, str):
        try:
            doc = json.loads(data)
            return True, "ok", doc, data
        except Exception:
            pass

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


def _preview_text(text: str, max_chars: int = 900) -> str:
    return (text[: max_chars - 1] + "…") if len(text) > max_chars else text


def _preview_json(doc: dict, raw_text: str | None, max_chars: int = 900) -> str:
    """Short snippet suitable for an embed field."""
    try:
        text = raw_text if raw_text else json.dumps(doc, ensure_ascii=False, indent=2)
    except Exception:
        text = json.dumps(doc, ensure_ascii=False)
    return _preview_text(text, max_chars)


# ============================================================================

class AdminLinks(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    # ---- Settings controls ---------------------------------------------------

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

    # ---- Diagnostics: show current source & snapshot -------------------------

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
        content_hash_raw = "n/a"
        content_hash_decoded = None
        snapshot_text = None
        decoded_from = None

        data = None
        raw_text = None
        decoded_text = None

        # Try external if chosen
        if chosen == "external" and external_present:
            src_used = f"external:{external_path}"
            try:
                # Prefer our storageClient loader; it may return dict or JSON string
                data = load_file(external_path)
                if isinstance(data, dict):
                    raw_text = json.dumps(data, ensure_ascii=False, indent=2)
                elif isinstance(data, str):
                    data = json.loads(data)
                    raw_text = json.dumps(data, ensure_ascii=False, indent=2)
                else:
                    if external_is_url:
                        data, raw_text = _read_http_json_and_text(external_path)
                    else:
                        ok, det, doc, raw = _try_local_json_and_text(external_path)
                        if not ok or not isinstance(doc, dict):
                            raise ValueError(det or "failed to read local external path")
                        data, raw_text = doc, raw or json.dumps(doc, ensure_ascii=False)

                if not isinstance(data, dict):
                    raise ValueError("top-level JSON is not an object")

                # record raw stats
                content_hash_raw = _content_hash(raw_text)
                top_keys = ", ".join(list(data.keys())[:10]) or "—"

                # prefer fully unwrapped view for size/snapshot
                unwrapped, changed, _ = unwrap_links_json(data)
                if changed:
                    decoded_from = "data"
                    decoded_text = json.dumps(unwrapped, ensure_ascii=False, indent=2)
                    content_hash_decoded = _content_hash(decoded_text)
                    data = unwrapped

                load_ok = True
                size_hint = _size_hint(data)
                snapshot_text = _preview_json(data, decoded_text or raw_text)
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
                    # raw view
                    content_hash_raw = _content_hash(raw or json.dumps(doc, ensure_ascii=False))
                    top_keys = ", ".join(list(doc.keys())[:10]) or "—"
                    # unwrapped view
                    unwrapped, changed, _ = unwrap_links_json(doc)
                    if changed:
                        decoded_from = "data"
                        decoded_text = json.dumps(unwrapped, ensure_ascii=False, indent=2)
                        content_hash_decoded = _content_hash(decoded_text)
                        doc = unwrapped
                    data = doc
                    load_ok = True
                    detail = det
                    size_hint = _size_hint(doc)
                    snapshot_text = _preview_json(doc, decoded_text or raw)
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

        if load_ok and isinstance(data, dict):
            embed.add_field(name="Top-level keys (raw)", value=top_keys or "—", inline=False)
            embed.add_field(name="Content hash (raw)", value=content_hash_raw or "n/a", inline=True)
            if content_hash_decoded:
                embed.add_field(
                    name=f"Content hash (decoded from '{decoded_from}')",
                    value=content_hash_decoded,
                    inline=True
                )
            embed.add_field(
                name="Snapshot (first ~900 chars)",
                value=f"```json\n{snapshot_text}\n```",
                inline=False
            )
            embed.set_footer(text=f"size_hint={size_hint} • type={type(data).__name__}")

        await interaction.response.send_message(embed=embed, ephemeral=True)

    # ---- Normalizer: fix wrapped files in-place ------------------------------

    links = app_commands.Group(name="links", description="Manage linked players file")

    async def _normalize_impl(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)

        gid = interaction.guild_id
        st = load_settings(gid) or {}
        prefer_external = bool(st.get("prefer_external_links", True))
        disable_local = bool(st.get("disable_local_link", False))
        external_path = (st.get("external_links_path") or "").strip()

        # Choose path roughly the same way showlinks selects a source
        external_present = bool(external_path)
        use_external_first = prefer_external or disable_local

        chosen_path = None
        if use_external_first and external_present:
            chosen_path = external_path
        elif not disable_local:
            # fallbacks
            for p in ("settings/linked_players.json", "data/linked_players.json"):
                if os.path.isfile(p):
                    chosen_path = p
                    break
            if not chosen_path:
                # Still normalize whatever path might exist locally even if not a real file yet
                chosen_path = "data/linked_players.json"

        if not chosen_path:
            await interaction.followup.send("❌ Could not resolve a path to normalize.", ephemeral=True)
            return

        # Load current content (storageClient first; may return dict or JSON string)
        doc = None
        raw_text = None
        try:
            raw_obj = load_file(chosen_path)
        except Exception:
            raw_obj = None

        if isinstance(raw_obj, dict):
            doc = raw_obj
            raw_text = json.dumps(doc, ensure_ascii=False, indent=2)
        elif isinstance(raw_obj, str):
            try:
                doc = json.loads(raw_obj)
                raw_text = raw_obj
            except Exception:
                doc = None

        # try local/http helpers if needed
        if not isinstance(doc, dict):
            if chosen_path.lower().startswith(("http://", "https://")):
                doc, raw_text = _read_http_json_and_text(chosen_path)
            else:
                ok, det, doc0, raw0 = _try_local_json_and_text(chosen_path)
                if not ok or not isinstance(doc0, dict):
                    await interaction.followup.send(f"❌ Load failed: {det}", ephemeral=True)
                    return
                doc, raw_text = doc0, raw0 or json.dumps(doc0, ensure_ascii=False)

        # Unwrap
        fixed, changed, reason = unwrap_links_json(doc)

        # Stats
        def _count(v: Any) -> int:
            if isinstance(v, dict):
                return len(v)
            if isinstance(v, list):
                return len(v)
            return 0

        before_n = _count(doc)
        after_n = _count(fixed)

        if changed:
            # Save back as plain JSON (no wrapper)
            save_file(chosen_path, fixed, indent=2)
            title = "✅ Normalized linked_players.json"
            color = 0x2ecc71
        else:
            title = "ℹ️ Nothing to change"
            color = 0x3498db

        # Preview keys
        keys_preview = ""
        if isinstance(fixed, dict):
            keys = list(fixed.keys())[:5]
            keys_preview = ", ".join(keys) + ("…" if len(fixed) > 5 else "")
        elif isinstance(fixed, list):
            keys_preview = f"{min(5, len(fixed))} items previewed"

        emb = discord.Embed(title=title, color=color)
        emb.add_field(name="Resolved path", value=f"```{chosen_path}```", inline=False)
        emb.add_field(name="Result", value=reason, inline=False)
        emb.add_field(name="Counts", value=f"Before: **{before_n}** · After: **{after_n}**", inline=False)
        if keys_preview:
            emb.add_field(name="Preview", value=f"```{keys_preview}```", inline=False)

        await interaction.followup.send(embed=emb, ephemeral=True)

    @links.command(name="normalize", description="Normalize linked_players.json to plain JSON")
    @admin_check()
    async def links_normalize(self, interaction: discord.Interaction):
        await self._normalize_impl(interaction)

    # Alias for folks who prefer a top-level command
    @app_commands.command(name="linksnormalize", description="(Alias) Normalize linked_players.json to plain JSON")
    @admin_check()
    async def linksnormalize(self, interaction: discord.Interaction):
        await self._normalize_impl(interaction)


async def setup(bot: commands.Bot):
    await bot.add_cog(AdminLinks(bot))
