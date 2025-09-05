# cogs/link.py
import discord
from discord import app_commands
from discord.ext import commands
from utils.linking import (
    link_locally,
    resolve_from_any,
    load_external_links,
    load_local_links,
)
from utils.settings import load_settings  # NEW

class LinkCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @app_commands.command(name="link", description="Link your Discord to your in-game gamertag")
    @app_commands.describe(gamertag="Your in-game name (Xbox or Steam)")
    async def link(self, interaction: discord.Interaction, gamertag: str):
        guild_id = interaction.guild_id
        user_id = str(interaction.user.id)

        # NEW: allow guilds (like your server) to disable local linking and defer to Rewards bot
        s = load_settings(guild_id)
        if s.get("disable_local_link"):
            return await interaction.response.send_message(
                "ℹ️ Linking on this server is handled by the **Rewards bot**. "
                "Please use that bot’s `/link` command instead.",
                ephemeral=True
            )

        # If external has a different tag for this user, show it (FYI), but we still link locally (per guild).
        ext = load_external_links(guild_id) or {}
        prior_ext = None
        rec = ext.get(user_id)
        if isinstance(rec, dict):
            prior_ext = rec.get("gamertag")

        # Save per-guild local link
        link_locally(guild_id, user_id, gamertag)

        # Sanity echo from local (per guild)
        local_map = load_local_links(guild_id)
        local = local_map.get(user_id)

        msg = f"✅ Linked **{interaction.user.mention}** to **{gamertag}** locally (this server only)."
        if prior_ext and prior_ext.lower() != gamertag.lower():
            msg += f"\nℹ️ External mapping shows **{prior_ext}** for your account (kept separate)."

        await interaction.response.send_message(msg, ephemeral=True)

    @app_commands.command(name="whois", description="Resolve a user or gamertag from known links")
    @app_commands.describe(
        user="Discord user (optional if you provide gamertag)",
        gamertag="Gamertag (optional if you select a user)"
    )
    async def whois(
        self,
        interaction: discord.Interaction,
        user: discord.User | None = None,
        gamertag: str | None = None
    ):
        guild_id = interaction.guild_id
        did = str(user.id) if user else None
        resolved_did, resolved_tag = resolve_from_any(
            guild_id,
            discord_id=did,
            gamertag=gamertag
        )
        if not (resolved_did or resolved_tag):
            return await interaction.response.send_message("❌ No link found.", ephemeral=True)

        out = []
        if resolved_did:
            out.append(f"**Discord**: <@{resolved_did}> (`{resolved_did}`)")
        if resolved_tag:
            out.append(f"**Gamertag**: `{resolved_tag}`")
        await interaction.response.send_message("\n".join(out), ephemeral=True)

async def setup(bot: commands.Bot):
    await bot.add_cog(LinkCog(bot))
