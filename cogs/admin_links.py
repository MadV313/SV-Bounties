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
    Returns (plain_obj, changed, reason). Handles accidental double-wraps.
    """
    changed_any = False
    reason = "no wrapper"
    seen = 0

    while isinstance(obj, dict) and "data" in obj and len(obj) == 1 and seen < 3:
        seen += 1
        d = obj["data"]
        if isinstance(d, (dict, list)):
            obj = d
            changed_any = True
            reason = "unwrapped nested dict/list"
            continue
        if isinstance(d, str) and _looks_base64(d):
            try:
                decoded = base64.b64decode(d, validate=True).decode("utf-8", "ignore")
                obj = json.loads(decoded)
                changed_any = True
                reason = "unwrapped base64→JSON"
                continue
            except Exception:
                pass
        if isinstance(d, str):
            try:
                obj = json.loads(d)
                changed_any = True
                reason = "unwrapped raw JSON string"
                continue
            except Exception:
                break
        break

    return obj, changed_any, reason


def _maybe_decode_wrapped_base64(doc: dict) -> tuple[dict, str | None, str | None]:
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
    req = Request(url, headers={"User-Agent": "SV-Bounties/links-check"})
    with urlopen(req, timeout=timeout) as resp:  # nosec
        charset = resp.headers.get_content_charset() or "utf-8"
        raw = resp.read().decode(charset, errors="replace")
    return json.loads(raw), raw


def _http_post_text(url: str, text: str, content_type: str = "application/json; charset=utf-8", timeout: float = 8.0) -> tuple[bool, str]:
    """Best-effort direct POST used when writing to http(s) JSON targets."""
    try:
        req = Request(url, data=text.encode("utf-8"), headers={"Content-Type": content_type}, method="POST")
        with urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8", "ignore")
            return (200 <= resp.status < 300, f"HTTP {resp.status}: {body[:200]}")
    except Exception as e:
        return (False, f"{type(e).__name__}: {e}")


def _try_local_json_and_text(path: str) -> tuple[bool, str, dict | None, str | None]:
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
        name="setexternallinkswriter",
        description="Set a WRITABLE path for linked_players.json (used by /links normalize to write back)"
    )
    @admin_check()
    @app_commands.describe(path="Writable path (e.g. /app/data/linked_players.json or sftp://user:pass@host/path/linked_players.json)")
    async def setexternallinkswriter(self, interaction: discord.Interaction, path: str):
        gid = interaction.guild_id
        save_settings(gid, {"external_links_write_path": path})
        await interaction.response.send_message(
            f"✅ External links *writer* path set.\n`{path}`",
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

    # ---- Diagnostics ---------------------------------------------------------

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
        external_write = (st.get("external_links_write_path") or "").strip()

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

        if chosen == "external" and external_present:
            src_used = f"external:{external_path}"
            try:
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

                content_hash_raw = _content_hash(raw_text)
                top_keys = ", ".join(list(data.keys())[:10]) or "—"

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

        if not load_ok and not disable_local:
            candidates = []
            if external_present and not external_is_url:
                candidates.append(external_path)
            candidates.extend(["settings/linked_players.json", "data/linked_players.json"])
            for path in candidates:
                ok, det, doc, raw = _try_local_json_and_text(path)
                if ok and isinstance(doc, dict):
                    src_used = f"local:{path}"
                    content_hash_raw = _content_hash(raw or json.dumps(doc, ensure_ascii=False))
                    top_keys = ", ".join(list(doc.keys())[:10]) or "—"
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
        if external_write:
            embed.add_field(name="external_links_write_path", value=external_write, inline=False)
        embed.add_field(name="Resolved source used", value=src_used, inline=False)
        embed.add_field(name="Load result", value=("✅ ok" if load_ok else f"❌ {detail}"), inline=False)

        if load_ok and isinstance(data, dict):
            embed.add_field(name="Top-level keys (raw)", value=top_keys or "—", inline=False)
            embed.add_field(name="Content hash (raw)", value=content_hash_raw or "n/a", inline=True)
            if content_hash_decoded:
                embed.add_field(name=f"Content hash (decoded from '{decoded_from}')", value=content_hash_decoded, inline=True)
            embed.add_field(name="Snapshot (first ~900 chars)", value=f"```json\n{snapshot_text}\n```", inline=False)
            embed.set_footer(text=f"size_hint={size_hint} • type={type(data).__name__}")

        await interaction.response.send_message(embed=embed, ephemeral=True)

    # ---- Normalizer ----------------------------------------------------------

    links = app_commands.Group(name="links", description="Manage linked players file")

    async def _normalize_impl(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)

        gid = interaction.guild_id
        st = load_settings(gid) or {}
        prefer_external = bool(st.get("prefer_external_links", True))
        disable_local = bool(st.get("disable_local_link", False))
        read_path = (st.get("external_links_path") or "").strip()
        write_path = (st.get("external_links_write_path") or "").strip()

        external_present = bool(read_path)
        use_external_first = prefer_external or disable_local

        chosen_read = None
        if use_external_first and external_present:
            chosen_read = read_path
        elif not disable_local:
            for p in ("settings/linked_players.json", "data/linked_players.json"):
                if os.path.isfile(p):
                    chosen_read = p
                    break
            if not chosen_read:
                chosen_read = "data/linked_players.json"

        if not chosen_read:
            await interaction.followup.send("❌ Could not resolve a path to normalize.", ephemeral=True)
            return

        is_read_url = chosen_read.lower().startswith(("http://", "https://"))
        if not write_path:
            if is_read_url:
                await interaction.followup.send(
                    "❌ The read path is an HTTP URL (likely read-only). "
                    "Set a writable path with **/setexternallinkswriter**.",
                    ephemeral=True
                )
                return
            write_path = chosen_read  # local path is writable

        # Load from READ path
        def _load_dict_from(path: str):
            if path.lower().startswith(("http://", "https://")):
                doc, raw = _read_http_json_and_text(path)
                return doc, raw
            ok, det, doc, raw = _try_local_json_and_text(path)
            if not ok or not isinstance(doc, dict):
                raise ValueError(det or "failed to read JSON object")
            return doc, raw or json.dumps(doc, ensure_ascii=False)

        try:
            doc, raw_text = _load_dict_from(chosen_read)
        except Exception as e:
            await interaction.followup.send(f"❌ Load failed from read path: {e}", ephemeral=True)
            return

        fixed, changed, reason = unwrap_links_json(doc)

        def _count(v: Any) -> int:
            if isinstance(v, dict):
                return len(v)
            if isinstance(v, list):
                return len(v)
            return 0

        before_n = _count(doc)
        after_n = _count(fixed)

        # ---- WRITE: if writing to HTTP(S), send a JSON STRING (not dict) ----
        wrote = False
        write_err = None
        wrote_raw_plain = False  # did we manage to make the remote RAW become plain JSON?
        if changed:
            try:
                if write_path.lower().startswith(("http://", "https://")):
                    payload = json.dumps(fixed, ensure_ascii=False, indent=2)
                    # First, try via storageClient (some impls accept raw string and don't wrap)
                    wrote = bool(save_file(write_path, payload))
                    # If raw is still wrapped after this, fall back to direct HTTP POST
                    if wrote:
                        try:
                            # quick raw check
                            _, raw_after = _read_http_json_and_text(chosen_read)
                            if raw_after.strip().startswith('{"data"'):
                                ok2, note2 = _http_post_text(write_path, payload)
                                wrote_raw_plain = ok2
                                if not ok2:
                                    write_err = f"storageClient wrote (wrapped). HTTP fallback: {note2}"
                            else:
                                wrote_raw_plain = True
                        except Exception:
                            pass
                else:
                    wrote = bool(save_file(write_path, fixed))
                    wrote_raw_plain = True  # local writes produce plain files
            except Exception as e:
                write_err = f"{type(e).__name__}: {e}"

        # Verify RAW + decoded
        raw_match_plain = False
        decoded_match = False
        verify_note = ""
        if changed and wrote:
            try:
                # cache-buster if URL
                verify_path = chosen_read
                if is_read_url:
                    sep = '&' if '?' in verify_path else '?'
                    verify_path = f"{verify_path}{sep}t={int(discord.utils.utcnow().timestamp())}"
                # raw read
                req = Request(verify_path, headers={"User-Agent": "SV-Bounties/links-verify"})
                with urlopen(req, timeout=8.0) as resp:
                    raw_verify = resp.read().decode(resp.headers.get_content_charset() or "utf-8", "replace")
                raw_match_plain = raw_verify.strip().startswith("{") and not raw_verify.strip().startswith('{"data"')
                # decoded compare
                ver_doc, _ = _read_http_json_and_text(verify_path)
                ver_plain, _, _ = unwrap_links_json(ver_doc)
                decoded_match = (ver_plain == fixed)
                if not raw_match_plain:
                    verify_note = "remote RAW still wrapped (content decodes correctly)"
            except Exception as e:
                verify_note = f"verify failed: {type(e).__name__}: {e}"

        # Preview keys
        keys_preview = ""
        if isinstance(fixed, dict):
            keys = list(fixed.keys())[:5]
            keys_preview = ", ".join(keys) + ("…" if len(fixed) > 5 else "")
        elif isinstance(fixed, list):
            keys_preview = f"{min(5, len(fixed))} items previewed"

        if changed and wrote and (wrote_raw_plain or raw_match_plain):
            title = "✅ Normalized & wrote linked_players.json"
            color = 0x2ecc71
        elif changed and wrote:
            title = "⚠️ Normalized & wrote (but RAW still wrapped)"
            color = 0xf1c40f
        elif changed and not wrote:
            title = "❌ Normalized but failed to write"
            color = 0xe74c3c
        else:
            title = "ℹ️ Nothing to change"
            color = 0x3498db

        emb = discord.Embed(title=title, color=color)
        emb.add_field(name="Read path", value=f"```{chosen_read}```", inline=False)
        emb.add_field(name="Write path", value=f"```{write_path}```", inline=False)
        emb.add_field(name="Result", value=reason, inline=False)
        emb.add_field(name="Counts", value=f"Before: **{before_n}** · After: **{after_n}**", inline=False)
        if changed:
            emb.add_field(name="Write", value=("ok" if wrote else f"error: {write_err}"), inline=False)
        if keys_preview:
            emb.add_field(name="Preview", value=f"```{keys_preview}```", inline=False)
        if changed and wrote:
            emb.add_field(
                name="Verify",
                value=("✅ RAW plain & decoded match" if raw_match_plain and decoded_match
                       else "⚠️ Decoded matches, RAW still wrapped" if decoded_match
                       else f"⚠️ {verify_note or 'verify mismatch'}"),
                inline=False
            )

        await interaction.followup.send(embed=emb, ephemeral=True)

    @links.command(name="normalize", description="Normalize linked_players.json to plain JSON")
    @admin_check()
    async def links_normalize(self, interaction: discord.Interaction):
        await self._normalize_impl(interaction)

    # -------- Optional debug helpers --------

    @links.command(name="raw", description="Show first ~900 chars of RAW linked_players.json (no decoding)")
    @admin_check()
    async def links_raw(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        st = load_settings(interaction.guild_id) or {}
        path = (st.get("external_links_path") or "").strip()
        if not path:
            await interaction.followup.send("No external_links_path is set.", ephemeral=True)
            return
        if path.lower().startswith(("http://", "https://")):
            # Fetch RAW over HTTP
            req = Request(path, headers={"User-Agent": "SV-Bounties/links-raw"})
            with urlopen(req, timeout=8.0) as resp:
                raw_text = resp.read().decode(resp.headers.get_content_charset() or "utf-8", "replace")
        else:
            ok, det, doc, raw = _try_local_json_and_text(path)
            if not ok:
                await interaction.followup.send(f"Load failed: {det}", ephemeral=True)
                return
            raw_text = raw or json.dumps(doc, ensure_ascii=False)
        snippet = raw_text[:900] + ("…" if len(raw_text) > 900 else "")
        h = _content_hash(raw_text)
        await interaction.followup.send(f"**Raw content hash** {h}\n```json\n{snippet}\n```", ephemeral=True)

    @links.command(name="pretty", description="Pretty-print the decoded links (unwraps if needed)")
    @admin_check()
    async def links_pretty(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        st = load_settings(interaction.guild_id) or {}
        path = (st.get("external_links_path") or "").strip()
        if not path:
            await interaction.followup.send("No external_links_path is set.", ephemeral=True)
            return
        if path.lower().startswith(("http://", "https://")):
            doc, _ = _read_http_json_and_text(path)
        else:
            ok, det, doc, _ = _try_local_json_and_text(path)
            if not ok or not isinstance(doc, dict):
                await interaction.followup.send(f"Load failed: {det}", ephemeral=True)
                return
        plain, _, _ = unwrap_links_json(doc)
        pretty = json.dumps(plain, ensure_ascii=False, indent=2)
        snippet = pretty[:1900] + ("…" if len(pretty) > 1900 else "")
        await interaction.followup.send(f"```json\n{snippet}\n```", ephemeral=True)

    # Alias (optional)
    @app_commands.command(name="linksnormalize", description="(Alias) Normalize linked_players.json to plain JSON")
    @admin_check()
    async def linksnormalize(self, interaction: discord.Interaction):
        await self._normalize_impl(interaction)


async def setup(bot: commands.Bot):
    await bot.add_cog(AdminLinks(bot))
