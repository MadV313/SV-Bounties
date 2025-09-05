# cogs/link.py
import discord
from discord import app_commands
from discord.ext import commands
from utils.linking import link_locally, resolve_from_any, load_external_links, load_local_links

class LinkCog(commands.Cog):
    def __init__(self, bot): self.bot = bot

    @app_commands.command(name="link", description="Link your Discord to your in-game gamertag")
    @app_commands.describe(gamertag="Your in-game name (Xbox or Steam)")
    async def link(self, interaction: discord.Interaction, gamertag: str):
        user_id = str(interaction.user.id)
        # If external has a different tag for this user, show it (FYI), but we still link locally.
        ext = load_external_links() or {}
        prior_ext = ext.get(user_id, {}).get("gamertag") if isinstance(ext.get(user_id), dict) else None

        link_locally(user_id, gamertag)
        # sanity echo
        local = load_local_links().get(user_id)

        msg = f"✅ Linked **{interaction.user.mention}** to **{gamertag}** locally."
        if prior_ext and prior_ext.lower() != gamertag.lower():
            msg += f"\nℹ️ External mapping shows **{prior_ext}** for your account (kept separate)."

        await interaction.response.send_message(msg, ephemeral=True)

    @app_commands.command(name="whois", description="Resolve a user or gamertag from known links")
    @app_commands.describe(user="Discord user (optional if you provide gamertag)",
                           gamertag="Gamertag (optional if you select a user)")
    async def whois(self, interaction: discord.Interaction, user: discord.User | None=None, gamertag: str | None=None):
        did = str(user.id) if user else None
        resolved_did, resolved_tag = resolve_from_any(discord_id=did, gamertag=gamertag)
        if not (resolved_did or resolved_tag):
            return await interaction.response.send_message("❌ No link found.", ephemeral=True)
        out = []
        if resolved_did: out.append(f"**Discord**: <@{resolved_did}> (`{resolved_did}`)")
        if resolved_tag: out.append(f"**Gamertag**: `{resolved_tag}`")
        await interaction.response.send_message("\n".join(out), ephemeral=True)

async def setup(bot: commands.Bot):
    await bot.add_cog(LinkCog(bot))
