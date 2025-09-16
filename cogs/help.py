# cogs/help.py
from __future__ import annotations

import discord
from discord import app_commands
from discord.ext import commands


def _is_admin(u: discord.abc.User | discord.Member | None) -> bool:
    try:
        perms = getattr(u, "guild_permissions", None)
        return bool(perms and (perms.administrator or perms.manage_guild))
    except Exception:
        return False


class HelpCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(name="help", description="Show all bot commands, what they do, and how to use them.")
    async def help(self, interaction: discord.Interaction):
        is_admin = _is_admin(interaction.user)

        # ---------- Embed: User Commands ----------
        e_user = discord.Embed(
            title="SV Bounties — Commands",
            description="Here’s everything you can do with the bot. Admin-only commands are marked 🔒.",
            color=0x2f3136,
        )
        e_user.add_field(
            name="General: Linking",
            value=(
                "**/link** — Link your Discord to your in-game gamertag *(guild-local)*\n"
                "• Args: `gamertag`\n"
                "• Usage: `/link gamertag: KingSlayer99`\n"
                "• Note: If the guild has external linking only, you’ll be directed to the Rewards bot.\n\n"
                "**/whois** — Look up a Discord user or gamertag\n"
                "• Args: `user?`, `gamertag?` (provide either)\n"
                "• Usage: `/whois user: @Player`  or  `/whois gamertag: KingSlayer99`"
            ),
            inline=False,
        )

        # ---------- Embed: Trace / Forensics ----------
        e_trace = discord.Embed(
            title="Trace & Forensics",
            description="Render movement paths and review recent actions.",
            color=0xF39C12,
        )
        e_trace.add_field(
            name="🔒 /trace",
            value=(
                "Render a player’s movement path on the current map, with action markers and an ADM snapshot text file.\n"
                "• Args: `user?`, `gamertag?`, `window_hours?` (default **24**)\n"
                "• **Channel:** Must be run in the configured **admin channel** (if one is set).\n"
                "• Examples:\n"
                "  • `/trace user: @Player window_hours: 12`\n"
                "  • `/trace gamertag: KingSlayer99 window_hours: 24`"
            ),
            inline=False,
        )

        e_trace.add_field(
            name="🔒 /tracked",
            value=(
                "Show last-known locations for tracked players (**must be run in the configured admin channel**).\n"
                "Outputs a map image with pins and a paginated list."
            ),
            inline=False,
        )

        # ---------- Embed: Bounties ----------
        e_bounty = discord.Embed(
            title="Bounties",
            description="Create, pay, and manage bounties. Some actions require the bounty channel.",
            color=0x5865F2,
        )
        e_bounty.add_field(
            name="/svbounty",
            value=(
                "Post a new bounty in the **configured bounty channel** with automatic map updates.\n"
                "• Args: `user?`, `gamertag?`, `tickets (2–10)`, `reason?`\n"
                "• Must be run **in the bounty channel** and the invoker must be **linked**.\n"
                "• Examples:\n"
                "  • `/svbounty user: @Target tickets: 5 reason: camping NWAF`\n"
                "  • `/svbounty gamertag: KingSlayer99 tickets: 3`"
            ),
            inline=False,
        )
        e_bounty.add_field(
            name="/bounty_pay",
            value=(
                "Pay off a bounty (**cost = 2× current bounty**).\n"
                "• Args: `user?`, `gamertag?`\n"
                "• Must be run in the **bounty channel**.\n"
                "• Example: `/bounty_pay user: @Target`"
            ),
            inline=False,
        )
        e_bounty.add_field(
            name="/svbounty_remove",
            value=(
                "(Admin) Remove an active bounty by user or gamertag.\n"
                "• Args: `user?`, `gamertag?`\n"
                "• Must be run in the **bounty channel**.\n"
                "• Example: `/svbounty_remove gamertag: KingSlayer99`"
            ),
            inline=False,
        )
        e_bounty.add_field(
            name="🔒 /clear",
            value=(
                "(Admin) Clear the **last 20 messages** in the bounty channel.\n"
                "• Must be run in the **bounty channel**."
            ),
            inline=False,
        )

        # ---------- Embed: Admin / Setup ----------
        e_admin = discord.Embed(
            title="Admin & Setup",
            description="Server owners/admins can configure channels, external data, FTP/API, and diagnostics.",
            color=0x3BA55C,
        )
        e_admin.add_field(
            name="🔒 /setchannels",
            value=(
                "Set the PRIVATE **admin** channel (trace output) and PUBLIC **bounty** channel.\n"
                "• Args: `admin_channel` (TextChannel), `bounty_channel` (TextChannel)\n"
                "• Example: `/setchannels admin_channel: #admin-logs bounty_channel: #bounties`"
            ),
            inline=False,
        )
        e_admin.add_field(
            name="🔒 /settings",
            value="Show the current core settings (admin/bounty channels, active map).",
            inline=False,
        )
        e_admin.add_field(
            name="🔒 /sync",
            value="Force-sync slash commands (use if commands were added/changed).",
            inline=False,
        )
        e_admin.add_field(
            name="🔒 /settings_here",
            value="Show this guild’s settings (channels, active map, external links path).",
            inline=False,
        )

        e_admin.add_field(
            name="🔒 /set externals",
            value=(
                "Set external data locations.\n"
                "• Fields (leave empty to keep; use `-` to clear): `base?`, `links?`, `wallet?`, `writer?`\n"
                "• Example: `/set externals base: https://example.com/data wallet: https://example.com/data/wallet.json`"
            ),
            inline=False,
        )
        e_admin.add_field(
            name="🔒 /external settings",
            value=(
                "Control resolution behavior for `linked_players`.\n"
                "• Args: `prefer_external?` (bool), `disable_local?` (bool)\n"
                "• Example: `/external settings prefer_external: true disable_local: false`"
            ),
            inline=False,
        )
        e_admin.add_field(
            name="🔒 /showexternals",
            value=(
                "Diagnostics for **linked_players** and **wallet** sources with hash + preview.\n"
                "Shows which path is used (external/local), load status, and JSON snapshot."
            ),
            inline=False,
        )

        e_admin.add_field(
            name="🔒 /set_creds",
            value=(
                "Configure Nitrado API & FTP for ADM scanning (per guild); optionally set active map.\n"
                "• Args: `nitrado_api_token?`, `nitrado_service_id?`, `host`, `username`, `password`, `port=21`,\n"
                "  `console` (Xbox/PlayStation), `interval_sec=10`, `map_choice?`\n"
                "• Example: `/set_creds console: Xbox host: ftp.example.com username: name password: ***** interval_sec: 10`"
            ),
            inline=False,
        )
        e_admin.add_field(
            name="🔒 /show_creds",
            value="Show current FTP/API config (passwords/tokens redacted).",
            inline=False,
        )
        e_admin.add_field(
            name="🔒 /clear_creds",
            value="Clear the saved FTP/API configuration for this guild.",
            inline=False,
        )

        # annotate when the viewer isn’t an admin
        if not is_admin:
            lock_note = (
                "You are **not** an admin here — commands marked 🔒 require Administrator or Manage Server."
            )
            e_admin.set_footer(text=lock_note)

        # Send all embeds (ephemeral)
        await interaction.response.send_message(embeds=[e_user, e_trace, e_bounty, e_admin], ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(HelpCog(bot))
