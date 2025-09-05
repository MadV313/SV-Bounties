# cogs/bounty.py
import discord
from discord import app_commands
from discord.ext import commands

from utils.linking import resolve_from_any
from utils.bounties import (
    create_bounty,
    list_open,
    remove_bounty_by_gamertag,
    remove_bounty_by_discord_id,
    clear_all_bounties,
)
from utils.settings import load_settings
from utils import live_pulse  # LIVE pulse manager
from tracer.config import MAPS  # kept (may be used later)
from tracer.map_renderer import render_track_png  # kept
from tracer.tracker import load_track  # kept


def admin_or_staff():
    def pred(i: discord.Interaction):
        perms = getattr(i.user, "guild_permissions", None)
        return bool(perms and (perms.administrator or perms.manage_guild))
    return app_commands.check(lambda i: pred(i))


class BountyCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        # ensure live pulse is initialized (safe to call multiple times)
        live_pulse.init(bot)

    @app_commands.command(name="bounty_add", description="Place a bounty and start live tracking")
    @app_commands.describe(
        user="Discord user (preferred if linked)",
        gamertag="Gamertag (use if not linked)",
        amount="SV ticket amount to place",
        note="Optional note"
    )
    @admin_or_staff()
    async def bounty_add(
        self,
        interaction: discord.Interaction,
        user: discord.User | None = None,
        gamertag: str | None = None,
        amount: int = 100,
        note: str | None = None
    ):
        await interaction.response.defer(thinking=True, ephemeral=True)
        if amount <= 0:
            return await interaction.followup.send("❌ Amount must be positive.", ephemeral=True)

        gid = interaction.guild_id
        did = str(user.id) if user else None
        # 🔁 per-guild linking
        resolved_did, resolved_tag = resolve_from_any(gid, discord_id=did, gamertag=gamertag)
        if not resolved_tag:
            return await interaction.followup.send(
                "❌ Couldn’t resolve target. Provide a gamertag or ensure they ran `/link`.",
                ephemeral=True
            )

        # TODO: integrate wallet debit here (SV tickets). For now, just record the bounty.
        b = create_bounty(str(interaction.user.id), resolved_did, resolved_tag, amount, note)

        # Public announcement (per-guild settings) & live pulse start
        s = load_settings(gid)
        bounty_channel_id = s.get("bounty_channel_id")
        posted = False
        if bounty_channel_id:
            ch = interaction.client.get_channel(int(bounty_channel_id))
            if isinstance(ch, discord.TextChannel):
                embed = discord.Embed(
                    title="🎯 New Bounty Posted",
                    description=f"**Target:** `{resolved_tag}`\n**Amount:** **{amount} SV tickets**",
                    color=discord.Color.red()
                )
                if note:
                    embed.add_field(name="Note", value=note, inline=False)
                embed.set_footer(text=f"Bounty ID: {b['id']}")
                try:
                    await ch.send(embed=embed)
                    posted = True
                except Exception:
                    # If posting fails, we still proceed silently
                    pass

        # Begin LIVE pulse: one message in the bounty channel updated on each ADM point
        if gid:
            live_pulse.start_for(gid, resolved_tag)

        await interaction.followup.send(
            ("✅ Bounty created and live tracking started." if posted else "✅ Bounty created. Live tracking will start on next location update.")
            + f"\nTarget: **{resolved_tag}** — **{amount} SV tickets**\nID: `{b['id']}`",
            ephemeral=True
        )

    @app_commands.command(name="bounty_list", description="List open bounties")
    async def bounty_list(self, interaction: discord.Interaction):
        opens = list_open()
        if not opens:
            return await interaction.response.send_message("No open bounties.")
        # simple list for now (global store; can be made per-guild later if desired)
        lines = [
            f"• `{b['id']}` — {b['target_gamertag']} — {b['amount']} SVt"
            for b in opens
        ]
        await interaction.response.send_message("\n".join(lines))

    @app_commands.command(name="bounty_remove", description="Remove bounties (by user, by gamertag, or all)")
    @app_commands.describe(
        user="Discord user (preferred if linked)",
        gamertag="Gamertag (use if not linked)",
        remove_all="Remove ALL open bounties"
    )
    @admin_or_staff()
    async def bounty_remove(
        self,
        interaction: discord.Interaction,
        user: discord.User | None = None,
        gamertag: str | None = None,
        remove_all: bool = False
    ):
        await interaction.response.defer(ephemeral=True, thinking=True)

        gid = interaction.guild_id

        if remove_all:
            # stop all pulses for this guild
            if gid:
                live_pulse.stop_all_for_guild(gid)
            n = clear_all_bounties()
            return await interaction.followup.send(f"🧹 Cleared **{n}** open bounties.", ephemeral=True)

        if user:
            if gid:
                # best-effort stop (display_name may not equal gamertag, but this halts common cases)
                live_pulse.stop_for(gid, user.display_name)
            n = remove_bounty_by_discord_id(str(user.id))
            return await interaction.followup.send(f"Removed **{n}** bounty(ies) for {user.mention}.", ephemeral=True)

        if gamertag:
            if gid:
                live_pulse.stop_for(gid, gamertag)
            n = remove_bounty_by_gamertag(gamertag)
            return await interaction.followup.send(f"Removed **{n}** bounty(ies) for `{gamertag}`.", ephemeral=True)

        await interaction.followup.send(
            "❌ Provide a target via `user` or `gamertag`, or set `remove_all: true`.",
            ephemeral=True
        )


async def setup(bot: commands.Bot):
    await bot.add_cog(BountyCog(bot))
