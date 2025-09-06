# cogs/links_normalize.py
from __future__ import annotations

import json
import base64
import traceback
from typing import Any, Tuple, Union

import discord
from discord import app_commands, Interaction, Embed
from discord.ext import commands

from utils.settings import load_settings
from utils.storageClient import load_file, save_file  # your existing helpers


def _unwrap_links_json(obj: Any) -> Tuple[Any, bool, str]:
    """
    Returns (fixed_obj, changed, reason)
    Accepts {"data": "<base64 json>"} or {"data": "<json string>"} or {"data": {...}}.
    Leaves anything else untouched.
    """
    if isinstance(obj, dict) and set(obj.keys()) == {"data"}:
        d = obj["data"]
        # Already proper dict/list?
        if isinstance(d, (dict, list)):
            return d, True, "Unwrapped nested dict/list"
        # Try base64 → JSON
        if isinstance(d, str):
            try:
                decoded = base64.b64decode(d, validate=True).decode("utf-8")
                return json.loads(decoded), True, "Unwrapped base64→JSON"
            except Exception:
                pass
            # Try raw JSON string
            try:
                return json.loads(d), True, "Unwrapped raw JSON string"
            except Exception:
                pass
        return obj, False, "Wrapper detected but not decodable"
    return obj, False, "No wrapper"


class LinksCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    links = app_commands.Group(name="links", description="Manage linked players file")

    @links.command(name="normalize", description="Normalize linked_players.json to plain JSON")
    @app_commands.default_permissions(administrator=True)
    async def normalize(self, interaction: Interaction):
        await interaction.response.defer(ephemeral=True)

        try:
            settings = load_settings() or {}
            # Resolve path preference
            path = None
            if settings.get("prefer_external_links"):
                path = settings.get("external_links_path")
            # Fallback to local default
            if not path:
                path = settings.get("local_links_path", "data/linked_players.json")

            # Load current file (works for http(s) or local via your helper)
            raw = load_file(path, default={})

            fixed, changed, reason = _unwrap_links_json(raw)

            # Basic stats for the embed
            def _count(v: Any) -> int:
                if isinstance(v, dict):
                    return len(v)
                if isinstance(v, list):
                    return len(v)
                return 0

            before_n = _count(raw)
            after_n = _count(fixed)

            if changed:
                # Write back plain JSON (no wrapper)
                save_file(path, fixed, indent=2)
                title = "✅ Normalized linked_players.json"
                color = 0x2ecc71
            else:
                title = "ℹ️ Nothing to change"
                color = 0x3498db

            # Make a small preview of keys for sanity
            keys_preview = ""
            if isinstance(fixed, dict):
                keys = list(fixed.keys())[:5]
                keys_preview = ", ".join(keys) + ("…" if len(fixed) > 5 else "")
            elif isinstance(fixed, list):
                keys_preview = f"{min(5, len(fixed))} items previewed"

            emb = Embed(title=title, color=color)
            emb.add_field(name="Resolved path", value=f"```{path}```", inline=False)
            emb.add_field(name="Result", value=reason, inline=False)
            emb.add_field(name="Counts", value=f"Before: **{before_n}** · After: **{after_n}**", inline=False)
            if keys_preview:
                emb.add_field(name="Preview", value=f"```{keys_preview}```", inline=False)

            await interaction.followup.send(embed=emb, ephemeral=True)

        except Exception as e:
            tb = traceback.format_exc()
            emb = Embed(title="❌ Normalize failed", description=str(e), color=0xe74c3c)
            emb.add_field(name="Traceback", value=f"```{tb[-1500:]}```", inline=False)
            await interaction.followup.send(embed=emb, ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(LinksCog(bot))
