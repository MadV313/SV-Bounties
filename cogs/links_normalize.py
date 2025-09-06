# cogs/links_normalize.py
from __future__ import annotations

import json
import base64
import traceback
from typing import Any, Tuple

from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

import discord
from discord import app_commands, Interaction, Embed
from discord.ext import commands

from utils.settings import load_settings
from utils.storageClient import load_file, save_file  # your existing helpers


# ---------------- helpers ----------------

_B64_CHARS = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=\n\r\t ")


def _looks_base64(s: str) -> bool:
    s = s.strip()
    return bool(s) and all(c in _B64_CHARS for c in s)


def _unwrap_links_json(obj: Any) -> Tuple[Any, bool, str]:
    """
    Returns (fixed_obj, changed, reason)
    Accepts {"data": "<base64 json>"} or {"data": "<json string>"} or {"data": {...}}.
    Handles accidental double-wraps; leaves anything else untouched.
    """
    changed_any = False
    reason = "No wrapper"
    seen = 0

    while isinstance(obj, dict) and set(obj.keys()) == {"data"} and seen < 3:
        seen += 1
        d = obj["data"]
        if isinstance(d, (dict, list)):
            obj = d
            changed_any = True
            reason = "Unwrapped nested dict/list"
            continue
        if isinstance(d, str) and _looks_base64(d):
            try:
                decoded = base64.b64decode(d, validate=True).decode("utf-8", "ignore")
                obj = json.loads(decoded)
                changed_any = True
                reason = "Unwrapped base64→JSON"
                continue
            except Exception:
                pass
        if isinstance(d, str):
            try:
                obj = json.loads(d)
                changed_any = True
                reason = "Unwrapped raw JSON string"
                continue
            except Exception:
                pass
        # not decodable
        reason = "Wrapper detected but not decodable"
        break

    return obj, changed_any, reason


def _read_http_json_and_text(url: str, timeout: float = 8.0) -> tuple[dict, str]:
    """Fetch JSON from HTTP(S). Returns (parsed_dict, raw_text)."""
    req = Request(url, headers={"User-Agent": "SV-Bounties/links-normalize"})
    with urlopen(req, timeout=timeout) as resp:  # nosec - admin-provided URL
        charset = resp.headers.get_content_charset() or "utf-8"
        raw = resp.read().decode(charset, errors="replace")
    return json.loads(raw), raw


def _count(v: Any) -> int:
    if isinstance(v, dict):
        return len(v)
    if isinstance(v, list):
        return len(v)
    return 0


# --------------- Cog ---------------------

class LinksCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    links = app_commands.Group(name="links", description="Manage linked players file")

    @links.command(name="normalize", description="Normalize linked_players.json to plain JSON (writes back to persistent repo)")
    @app_commands.default_permissions(administrator=True)
    async def normalize(self, interaction: Interaction):
        await interaction.response.defer(ephemeral=True)

        try:
            settings = load_settings() or {}

            # Resolve read (public) path
            prefer_external = bool(settings.get("prefer_external_links", True))
            read_path = None
            if prefer_external:
                read_path = (settings.get("external_links_path") or "").strip()
            if not read_path:
                read_path = (settings.get("local_links_path") or "data/linked_players.json").strip()

            if not read_path:
                await interaction.followup.send("❌ No links path configured.", ephemeral=True)
                return

            is_read_url = read_path.lower().startswith(("http://", "https://"))

            # Resolve write (writable) path
            write_path = (settings.get("external_links_write_path") or "").strip()
            if not write_path:
                if is_read_url:
                    await interaction.followup.send(
                        "❌ The configured read path is an HTTP URL (likely read-only).\n"
                        "Please set a writable path with your admin command (e.g. external_links_write_path) "
                        "so I can write the normalized JSON back to the persistent repo.",
                        ephemeral=True
                    )
                    return
                # If read_path is local (file), we can write back to it directly.
                write_path = read_path

            # ----- Load from READ path (what other bots use) -----
            def _load_dict_from(path: str):
                if path.lower().startswith(("http://", "https://")):
                    doc, raw = _read_http_json_and_text(path)
                    return doc, raw
                # storageClient may return dict or JSON string
                raw_obj = load_file(path)
                if isinstance(raw_obj, dict):
                    return raw_obj, json.dumps(raw_obj, ensure_ascii=False)
                if isinstance(raw_obj, str):
                    try:
                        return json.loads(raw_obj), raw_obj
                    except Exception:
                        pass
                # Last resort: try reading as local file path
                raise ValueError("Failed to load JSON object from read path")

            doc, _ = _load_dict_from(read_path)

            # ----- Unwrap to plain JSON -----
            fixed, changed, reason = _unwrap_links_json(doc)

            before_n = _count(doc)
            after_n = _count(fixed)

            # ----- Write back to WRITER path if changed -----
            wrote = False
            write_err = None
            if changed:
                try:
                    save_file(write_path, fixed)  # no indent kwarg
                    wrote = True
                except Exception as e:
                    write_err = f"{type(e).__name__}: {e}"

            # ----- Verify by re-reading READ path (with cache-buster if URL) -----
            verified = False
            verify_note = ""
            if changed and wrote:
                try:
                    verify_path = read_path
                    if is_read_url:
                        sep = '&' if '?' in verify_path else '?'
                        verify_path = f"{verify_path}{sep}t={int(discord.utils.utcnow().timestamp())}"
                    ver_doc, _ = _load_dict_from(verify_path)
                    ver_plain, _, _ = _unwrap_links_json(ver_doc)
                    verified = (ver_plain == fixed)
                    if not verified:
                        verify_note = "Remote content differs (cache or different read/write target)"
                except Exception as e:
                    verify_note = f"verify failed: {type(e).__name__}: {e}"

            # ----- Build embed -----
            if changed and wrote:
                title = "✅ Normalized & wrote linked_players.json"
                color = 0x2ecc71
            elif changed and not wrote:
                title = "❌ Normalized but failed to write"
                color = 0xe74c3c
            else:
                title = "ℹ️ Nothing to change"
                color = 0x3498db

            keys_preview = ""
            if isinstance(fixed, dict):
                keys = list(fixed.keys())[:5]
                keys_preview = ", ".join(keys) + ("…" if len(fixed) > 5 else "")
            elif isinstance(fixed, list):
                keys_preview = f"{min(5, len(fixed))} items previewed"

            emb = Embed(title=title, color=color)
            emb.add_field(name="Read path (public)", value=f"```{read_path}```", inline=False)
            emb.add_field(name="Write path (writable)", value=f"```{write_path}```", inline=False)
            emb.add_field(name="Result", value=reason, inline=False)
            emb.add_field(name="Counts", value=f"Before: **{before_n}** · After: **{after_n}**", inline=False)
            if changed:
                emb.add_field(name="Write", value=("ok" if wrote else f"error: {write_err}"), inline=False)
            if keys_preview:
                emb.add_field(name="Preview", value=f"```{keys_preview}```", inline=False)
            if changed and wrote:
                emb.add_field(name="Verify (re-read read path)", value=("✅ match" if verified else f"⚠️ {verify_note}"), inline=False)

            await interaction.followup.send(embed=emb, ephemeral=True)

        except Exception as e:
            tb = traceback.format_exc()
            emb = Embed(title="❌ Normalize failed", description=str(e), color=0xe74c3c)
            emb.add_field(name="Traceback", value=f"```{tb[-1500:]}```", inline=False)
            await interaction.followup.send(embed=emb, ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(LinksCog(bot))
