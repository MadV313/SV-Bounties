# cogs/bounty.py
import discord
from discord import app_commands
from discord.ext import commands

from utils.linking import resolve_from_any
from utils.bounties import create_bounty, list_open
from utils.settings import load_settings
from tracer.config import MAPS
from tracer.map_renderer import render_track_png
from tracer.tracker import load_track

def admin_or_staff():
    def pred(i: discord.Interaction):
        perms = getattr(i.user, "guild_permissions", None)
        return bool(perms and (perms.administrator or perms.manage_guild))
    return app_commands.check(lambda i: pred(i))

class BountyCog(commands.Cog):
    def __init__(self, bot): self.bot = bot

    @app_commands.command(name="bounty_add", description="Place a bounty (SV tickets integration pending)")
    @app_commands.describe(
        user="Discord user (preferred if linked)",
        gamertag="Gamertag (use if not linked)",
        amount="SV ticket amount to place",
        note="Optional note"
    )
    @admin_or_staff()
    async def bounty_add(self, interaction: discord.Interaction,
                         user: discord.User | None=None,
                         gamertag: str | None=None,
                         amount: int=100,
                         note: str | None=None):
        await interaction.response.defer(thinking=True, ephemeral=True)
        if amount <= 0:
            return await interaction.followup.send("❌ Amount must be positive.", ephemeral=True)

        did = str(user.id) if user else None
        resolved_did, resolved_tag = resolve_from_any(discord_id=did, gamertag=gamertag)
        if not resolved_tag:
            return await interaction.followup.send("❌ Couldn’t resolve target. Provide a gamertag or ensure they ran `/link`.", ephemeral=True)

        # TODO: integrate wallet debit here (SV tickets). For now, just record the bounty.
        b = create_bounty(str(interaction.user.id), resolved_did, resolved_tag, amount, note)
        await interaction.followup.send(
            f"🎯 Bounty created on **{resolved_tag}** for **{amount} SV tickets**.\nID: `{b['id']}`",
            ephemeral=True
        )

    @app_commands.command(name="bounty_list", description="List open bounties")
    async def bounty_list(self, interaction: discord.Interaction):
        opens = list_open()
        if not opens:
            return await interaction.response.send_message("No open bounties.")
        # simple list for now
        lines = [f"• `{b['id']}` — {b['target_gamertag']} — {b['amount']} SVt" for b in opens]
        await interaction.response.send_message("\n".join(lines))

async def setup(bot: commands.Bot):
    await bot.add_cog(BountyCog(bot))
