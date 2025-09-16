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
        # already proper dict/list
        if isinstance(d, (dict, list)):
            obj = d
            changed_any = True
            reason = "unwrapped nested dict/list"
            continue
        # base64 → JSON
        if isinstance(d, str) and _looks_base64(d):
            try:
                decoded = base64.b64decode(d, validate=True).decode("utf-8", "ignore")
                obj = json.loads(decoded)
                changed_any = True
                reason = "unwrapped base64→JSON"
                continue
            except Exception:
                pass
        # raw JSON string
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


def _read_http_json_and_text(url: str, timeout: float = 8.0) -> tuple[dict, str]:
    """Fetch JSON from HTTP(S). Returns (parsed_dict, raw_text)."""
    req = Request(url, headers={"User-Agent": "SV-Bounties/links-check"})
    with urlopen(req, timeout=timeout) as resp:  # nosec - admin-provided URL
        charset = resp.headers.get_content_charset() or "utf-8"
        raw = resp.read().decode(charset, errors="replace")
    return json.loads(raw), raw


def _try_local_json_and_text(path: str) -> tuple[bool, str, dict | None, str | None]:
    """
    Try reading JSON via storageClient first, then direct FS.
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

    # ---- NEW: combined SET command for externals -----------------------------

    set = app_commands.Group(name="set", description="Configure bot settings")

    @set.command(
        name="externals",
        description="Set base/links/wallet/writer paths. Leave fields empty to keep current; use '-' to clear."
    )
    @admin_check()
    @app_commands.describe(
        base="Base folder (e.g. https://.../data) that contains wallet.json & linked_players.json",
        links="Explicit path/URL to linked_players.json (overrides base)",
        wallet="Explicit path/URL to wallet.json (overrides base)",
        writer="Writable path/URL where normalized linked_players.json will be saved"
    )
    async def set_externals(
        self,
        interaction: discord.Interaction,
        base: str | None = None,
        links: str | None = None,
        wallet: str | None = None,
        writer: str | None = None,
    ):
        gid = interaction.guild_id

        def _norm(v: str | None, *, strip_trailing_slash: bool = False) -> tuple[bool, str | None]:
            """
            Returns (should_update, value_or_None). If v is None → no change.
            If v is one of {'-','none','null','clear'} → update to None (clear).
            Otherwise returns cleaned value (optionally rstrip('/')).
            """
            if v is None:
                return False, None
            sv = (v or "").strip()
            if sv.lower() in {"-", "none", "null", "clear"}:
                return True, None
            if strip_trailing_slash:
                sv = sv.rstrip("/")
            return True, sv or None

        updates: dict[str, Any] = {}
        changed: list[str] = []
        kept: list[str] = []

        upd, val = _norm(base, strip_trailing_slash=True)
        if upd:
            updates["external_data_base"] = val
            changed.append(f"external_data_base → `{val or 'cleared'}`")
        else:
            kept.append("external_data_base")

        upd, val = _norm(links)
        if upd:
            updates["external_links_path"] = val
            changed.append(f"external_links_path → `{val or 'cleared'}`")
        else:
            kept.append("external_links_path")

        upd, val = _norm(wallet)
        if upd:
            updates["external_wallet_path"] = val
            changed.append(f"external_wallet_path → `{val or 'cleared'}`")
        else:
            kept.append("external_wallet_path")

        upd, val = _norm(writer)
        if upd:
            updates["external_links_write_path"] = val
            changed.append(f"external_links_write_path → `{val or 'cleared'}`")
        else:
            kept.append("external_links_write_path")

        if updates:
            save_settings(gid, updates)

        st = load_settings(gid) or {}
        base_now = (st.get("external_data_base") or "—")
        links_now = (st.get("external_links_path") or "—")
        wallet_now = (st.get("external_wallet_path") or "—")
        writer_now = (st.get("external_links_write_path") or "—")

        emb = discord.Embed(
            title="External paths updated" if updates else "No changes",
            color=0x3BA55C if updates else 0x5865F2
        )
        if changed:
            emb.add_field(name="Changed", value="• " + "\n• ".join(changed), inline=False)
        if kept:
            emb.add_field(name="Kept (unchanged)", value="• " + "\n• ".join(kept), inline=False)
        emb.add_field(name="Current values", value=(
            f"**base**: `{base_now}`\n"
            f"**links**: `{links_now}`\n"
            f"**wallet**: `{wallet_now}`\n"
            f"**writer**: `{writer_now}`"
        ), inline=False)
        emb.set_footer(text="Tip: use /showexternals to verify sources.")
        await interaction.response.send_message(embed=emb, ephemeral=True)

    # ---- Combined external settings (prefer/disable) -------------------------

    external = app_commands.Group(name="external", description="Configure external vs local link handling")

    @external.command(
        name="settings",
        description="Set or view external-link preferences (prefer external; disable local /link)."
    )
    @admin_check()
    @app_commands.describe(
        prefer_external="Prefer external linked_players over local (true/false)",
        disable_local="Disable this bot's local /link (use external only) (true/false)"
    )
    async def external_settings(
        self,
        interaction: discord.Interaction,
        prefer_external: bool | None = None,
        disable_local: bool | None = None,
    ):
        gid = interaction.guild_id
        st = load_settings(gid) or {}
        current_prefer = bool(st.get("prefer_external_links", True))
        current_disable = bool(st.get("disable_local_link", False))

        updates: dict[str, Any] = {}
        if prefer_external is not None:
            updates["prefer_external_links"] = bool(prefer_external)
        if disable_local is not None:
            updates["disable_local_link"] = bool(disable_local)

        if updates:
            save_settings(gid, updates)
            st = load_settings(gid) or {}
            current_prefer = bool(st.get("prefer_external_links", True))
            current_disable = bool(st.get("disable_local_link", False))

        emb = discord.Embed(
            title="External link settings",
            color=0x3BA55C,
            description="View or update how this guild resolves `linked_players`."
        )
        emb.add_field(name="prefer_external_links", value=str(current_prefer))
        emb.add_field(name="disable_local_link", value=str(current_disable))
        emb.set_footer(text="Tip: use /showexternals to see which sources are used.")
        await interaction.response.send_message(embed=emb, ephemeral=True)

    # ---- Combined diagnostics -------------------------------------------------

    @app_commands.command(
        name="showexternals",
        description="Show which linked_players and wallet sources are used, with previews and hashes."
    )
    @admin_check()
    async def showexternals(self, interaction: discord.Interaction):
        gid = interaction.guild_id
        st = load_settings(gid) or {}

        # ---------- LINKS (linked_players.json) ----------
        prefer_external = bool(st.get("prefer_external_links", True))
        disable_local = bool(st.get("disable_local_link", False))
        external_path = (st.get("external_links_path") or "").strip()
        base = (st.get("external_data_base") or "").strip().rstrip("/")
        if not external_path and base:
            external_path = f"{base}/linked_players.json"

        external_is_url = external_path.lower().startswith(("http://", "https://"))
        external_present = bool(external_path)
        use_external_first = prefer_external or disable_local
        chosen_links = "external" if (use_external_first and external_present) else ("local" if not disable_local else "none")

        links_src_used = "none"
        links_load_ok = False
        links_detail = ""
        links_top_keys = "—"
        links_size_hint = 0
        links_hash_raw = "n/a"
        links_hash_decoded = None
        links_snapshot = None
        decoded_text = None

        links_doc = None
        raw_text = None

        if chosen_links == "external" and external_present:
            links_src_used = f"external:{external_path}"
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

                links_hash_raw = _content_hash(raw_text)
                links_top_keys = ", ".join(list(data.keys())[:10]) or "—"

                unwrapped, changed, _ = unwrap_links_json(data)
                if changed:
                    decoded_text = json.dumps(unwrapped, ensure_ascii=False, indent=2)
                    links_hash_decoded = _content_hash(decoded_text)
                    data = unwrapped

                links_doc = data
                links_load_ok = True
                links_size_hint = _size_hint(links_doc)
                links_snapshot = _preview_json(links_doc, decoded_text or raw_text)
                links_detail = "ok"
            except (HTTPError, URLError, TimeoutError, ValueError, json.JSONDecodeError) as e:
                links_detail = f"external load failed: {e}"

        if not links_load_ok and not disable_local:
            candidates = []
            if external_present and not external_is_url:
                candidates.append(external_path)
            candidates.extend(["settings/linked_players.json", "data/linked_players.json"])
            for path in candidates:
                ok, det, doc, raw = _try_local_json_and_text(path)
                if ok and isinstance(doc, dict):
                    links_src_used = f"local:{path}"
                    links_hash_raw = _content_hash(raw or json.dumps(doc, ensure_ascii=False))
                    links_top_keys = ", ".join(list(doc.keys())[:10]) or "—"
                    unwrapped, changed, _ = unwrap_links_json(doc)
                    if changed:
                        decoded_text = json.dumps(unwrapped, ensure_ascii=False, indent=2)
                        links_hash_decoded = _content_hash(decoded_text)
                        doc = unwrapped
                    links_doc = doc
                    links_load_ok = True
                    links_detail = det
                    links_size_hint = _size_hint(doc)
                    links_snapshot = _preview_json(doc, decoded_text or raw)
                    break
            if not links_load_ok and not links_detail:
                links_detail = "no usable local file found"

        links_embed = discord.Embed(
            title="linked_players status",
            color=0x3BA55C if links_load_ok else 0xED4245,
            description=f"**Chosen source**: `{chosen_links}`",
        )
        links_embed.add_field(name="prefer_external_links", value=str(prefer_external))
        links_embed.add_field(name="disable_local_link", value=str(disable_local))
        links_embed.add_field(name="external_data_base", value=(base or "—"), inline=False)
        links_embed.add_field(name="external_links_path", value=(external_path or "—"), inline=False)
        links_embed.add_field(name="Resolved source used", value=links_src_used, inline=False)
        links_embed.add_field(name="Load result", value=("✅ ok" if links_load_ok else f"❌ {links_detail}"), inline=False)

        if links_load_ok and isinstance(links_doc, dict):
            links_embed.add_field(name="Top-level keys (raw)", value=links_top_keys or "—", inline=False)
            links_embed.add_field(name="Content hash (raw)", value=links_hash_raw or "n/a", inline=True)
            if links_hash_decoded:
                links_embed.add_field(name="Content hash (decoded from 'data')", value=links_hash_decoded, inline=True)
            links_embed.add_field(name="Snapshot (first ~900 chars)", value=f"```json\n{links_snapshot}\n```", inline=False)
            links_embed.set_footer(text=f"size_hint={links_size_hint} • type={type(links_doc).__name__}")

        # ---------- WALLET (wallet.json) ----------
        explicit_wallet = (st.get("external_wallet_path") or "").strip()
        wallet_candidates: list[str] = []
        if explicit_wallet:
            wallet_candidates.append(explicit_wallet)
        if base:
            wallet_candidates.append(f"{base}/wallet.json")
        wallet_candidates += ["data/wallet.json", "wallet.json"]

        wallet_chosen = None
        wallet_doc = None
        wallet_raw = None
        wallet_note = ""

        for p in wallet_candidates:
            try:
                if p.lower().startswith(("http://", "https://")):
                    d, r = _read_http_json_and_text(p)
                    if isinstance(d, dict):
                        wallet_chosen, wallet_doc, wallet_raw = p, d, r
                        break
                else:
                    ok, det, d, r = _try_local_json_and_text(p)
                    if ok and isinstance(d, dict):
                        wallet_chosen, wallet_doc, wallet_raw = p, d, (r or json.dumps(d, ensure_ascii=False))
                        break
                    else:
                        wallet_note = det or wallet_note
            except Exception as e:
                wallet_note = f"{type(e).__name__}: {e}"

        wallet_embed = discord.Embed(
            title="wallet.json status" if wallet_chosen else "wallet.json status — not found",
            color=0x3BA55C if wallet_chosen else 0xED4245
        )
        wallet_embed.add_field(name="external_data_base", value=(base or "—"), inline=False)
        wallet_embed.add_field(name="external_wallet_path", value=(explicit_wallet or "—"), inline=False)
        if wallet_chosen:
            wallet_embed.add_field(name="Resolved source used", value=wallet_chosen, inline=False)
            wallet_embed.add_field(name="Content hash", value=_content_hash(wallet_raw), inline=False)
            snippet = (wallet_raw[:900] + ("…" if len(wallet_raw) > 900 else ""))
            wallet_embed.add_field(name="Snapshot (first ~900 chars)", value=f"```json\n{snippet}\n```", inline=False)
        else:
            wallet_embed.add_field(
                name="Search attempted",
                value="```\n" + "\n".join(wallet_candidates) + "\n```",
                inline=False
            )
            if wallet_note:
                wallet_embed.add_field(name="Note", value=wallet_note, inline=False)

        await interaction.response.send_message(embeds=[links_embed, wallet_embed], ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(AdminLinks(bot))
