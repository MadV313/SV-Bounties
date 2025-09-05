# cogs/help.py
import discord
from discord import app_commands
from discord.ext import commands
from typing import Optional, List

HELP_CATEGORIES = [
    ("Quickstart", "quickstart"),
    ("Admin", "admin"),
    ("Bounties", "bounties"),
    ("Linking", "linking"),
    ("Troubleshooting", "troubleshooting"),
    ("All", "all"),
]

def is_guild_admin(interaction: discord.Interaction) -> bool:
    # Manage Guild is a good proxy for “server admin” for these commands
    perms = interaction.user.guild_permissions if interaction.guild else None
    return bool(perms and perms.manage_guild)

def build_quickstart_embed(guild: Optional[discord.Guild]) -> discord.Embed:
    e = discord.Embed(
        title="SV Bounties — Quickstart (Noob-Friendly)",
        description=(
            "Follow these steps to set up the bot from scratch. This guide keeps things **plain English**.\n\n"
            "1) **Invite the bot** to your server and give it a role with at least:\n"
            "   - View Channels, Send Messages, Embed Links, Attach Files\n\n"
            "2) Go to the channel where you want bounties posted and run:\n"
            "   • `/setchannels admin_channel: #your-admin-channel bounty_channel: #your-bounties-channel`\n\n"
            "3) Tell the bot which **map** you use:\n"
            "   • `/setmap livonia`  **or**  `/setmap chernarus`\n\n"
            "4) Hook up your **FTP** so the bot can read your DayZ ADM logs:\n"
            "   • `/setftp host: <ip-or-host> user: <name> password: <pass> port: 21 adm_dir: /adm interval_sec: 60`\n"
            "     - `interval_sec` controls how often we check logs (e.g. 60 = 1 minute). 10–120 is typical.\n"
            "     - You can rerun `/setftp` anytime to change it.\n\n"
            "5) **Sync slash commands** (first time only or after updates):\n"
            "   • `/sync`\n\n"
            "6) (Optional) Link a Discord member to a gamertag so names resolve nicely:\n"
            "   • `/linklocal @user gamertag: YourGamertag`\n\n"
            "That’s it. The bot will fetch logs every interval and post/update live tracking embeds without spamming when a player hasn’t moved."
        ),
        color=discord.Color.blurple()
    )
    if guild:
        e.set_footer(text=f"Server: {guild.name}")
    return e

def build_admin_embed() -> discord.Embed:
    e = discord.Embed(
        title="Admin Commands (Plain English)",
        color=discord.Color.dark_gold()
    )
    e.add_field(
        name="`/setchannels`",
        value=(
            "**Purpose:** Pick your admin & bounty channels **in one go**.\n"
            "**Usage:** `/setchannels admin_channel: #admin-ops bounty_channel: #bounties`\n"
            "**Notes:** Saves both IDs to this server’s settings."
        ),
        inline=False
    )
    e.add_field(
        name="`/setmap`",
        value=(
            "**Purpose:** Choose world geometry for coordinate math.\n"
            "**Usage:** `/setmap livonia` or `/setmap chernarus`\n"
            "**Notes:** Defaults to Livonia if you don’t set it—better to set it explicitly."
        ),
        inline=False
    )
    e.add_field(
        name="`/setftp`",
        value=(
            "**Purpose:** Configure FTP so the bot can read DayZ ADM logs.\n"
            "**Usage:** `/setftp host: my.ftp.host user: name password: pass port: 21 adm_dir: /adm interval_sec: 60`\n"
            "**Tips:**\n"
            "• `interval_sec` = how often to poll (e.g., `60` = 1 minute).\n"
            "• Safe range: 10–120s. Use `60–120` to reduce chatter.\n"
            "• Rerun to change any value later."
        ),
        inline=False
    )
    e.add_field(
        name="`/clearftp`",
        value=(
            "**Purpose:** Clear/remove saved FTP settings for this server.\n"
            "**When to use:** You’re changing providers or credentials."
        ),
        inline=False
    )
    e.add_field(
        name="`/settings_here`",
        value=(
            "**Purpose:** Show what the bot currently thinks your settings are in this server.\n"
            "**Usage:** `/settings_here`"
        ),
        inline=False
    )
    e.add_field(
        name="`/sync`",
        value=(
            "**Purpose:** (Re)register slash commands in this server.\n"
            "**When to run:** First install, or after you update/add commands."
        ),
        inline=False
    )
    return e

def build_bounties_embed() -> discord.Embed:
    e = discord.Embed(
        title="Bounties Commands",
        description="Create, list, and manage bounties posted to your configured **bounty channel**.",
        color=discord.Color.green()
    )
    e.add_field(
        name="`/bounty_create`",
        value=(
            "**Purpose:** Open a new bounty.\n"
            "**Typical fields:** title, target (name/GT), reward, extra notes.\n"
            "**Behavior:** Posts an embed in the bounty channel."
        ),
        inline=False
    )
    e.add_field(
        name="`/bounty_list`",
        value=(
            "**Purpose:** View **open** (and sometimes closed) bounties.\n"
            "**Behavior:** Shows a clean list for reference."
        ),
        inline=False
    )
    e.add_field(
        name="`/bounty_close`",
        value=(
            "**Purpose:** Mark a bounty as **closed** (completed/canceled).\n"
            "**Tip:** Use ID or pick from a dropdown if implemented in your build."
        ),
        inline=False
    )
    e.add_field(
        name="`/bounty_clear`",
        value=(
            "**Purpose:** Cleanup utility for admins to clear stale bounties.\n"
            "**Warning:** This action can remove multiple entries; use carefully."
        ),
        inline=False
    )
    e.set_footer(text="Exact field names can vary slightly by your current build; the flow above matches your repo’s cogs.")
    return e

def build_linking_embed() -> discord.Embed:
    e = discord.Embed(
        title="Linking Commands",
        description="Tie Discord users to in-game identities so embeds read nicely and lookups work.",
        color=discord.Color.teal()
    )
    e.add_field(
        name="`/linklocal`",
        value=(
            "**Purpose:** Link a Discord user to a **gamertag** locally in this server.\n"
            "**Usage:** `/linklocal user: @Player gamertag: SomeGT`"
        ),
        inline=False
    )
    e.add_field(
        name="`/resolvelink`",
        value=(
            "**Purpose:** Look up a user ↔ gamertag mapping.\n"
            "**Usage:** `/resolvelink user: @Player` **or** `/resolvelink gamertag: SomeGT`"
        ),
        inline=False
    )
    e.add_field(
        name="`/loadexternallinks`",
        value=(
            "**Purpose:** Load a shared list of links from an external JSON URL (e.g., GitHub raw/Gist).\n"
            "**Shape:** `[ { \"discord_id\": \"123...\", \"gamertag\": \"SomeGT\" }, ... ]`"
        ),
        inline=False
    )
    return e

def build_troubleshooting_embed() -> discord.Embed:
    e = discord.Embed(
        title="Troubleshooting & FAQs",
        color=discord.Color.red()
    )
    e.add_field(
        name="Bot replies but no slash commands?",
        value=(
            "Run `/sync`. If commands still don’t appear, kick the bot, reinvite with **applications.commands** scope, and try again."
        ),
        inline=False
    )
    e.add_field(
        name="No embeds or images in bounty channel?",
        value=(
            "Ensure the bot role can **Embed Links** and **Attach Files** in that channel. Also check channel overrides."
        ),
        inline=False
    )
    e.add_field(
        name="ADM polling not updating?",
        value=(
            "Check `/settings_here` and re-run `/setftp` with correct `host/user/password/adm_dir`.\n"
            "If your FTP lacks REST support, consider switching providers or ensuring resume is enabled."
        ),
        inline=False
    )
    e.add_field(
        name="Too many updates (chatty)?",
        value=(
            "Rerun `/setftp` with a larger `interval_sec` (e.g., 60–120). The bot also avoids updating when a player hasn't moved."
        ),
        inline=False
    )
    e.add_field(
        name="Where are settings stored?",
        value=(
            "`data/settings/<guild_id>.json` for per-server settings, `data/ftp_config.json` for FTP, `data/bounties.json` for bounties."
        ),
        inline=False
    )
    return e

def build_all_embed(guild: Optional[discord.Guild]) -> List[discord.Embed]:
    # Split into multiple embeds to avoid hitting length limits
    return [
        build_quickstart_embed(guild),
        build_admin_embed(),
        build_bounties_embed(),
        build_linking_embed(),
        build_troubleshooting_embed(),
    ]

class HelpSelect(discord.ui.Select):
    def __init__(self, guild: Optional[discord.Guild]):
        options = [
            discord.SelectOption(label=label, value=value)
            for label, value in HELP_CATEGORIES
        ]
        super().__init__(placeholder="Choose a help section…", min_values=1, max_values=1, options=options)
        self.guild = guild

    async def callback(self, interaction: discord.Interaction):
        value = self.values[0]
        if value == "quickstart":
            embeds = [build_quickstart_embed(self.guild)]
        elif value == "admin":
            embeds = [build_admin_embed()]
        elif value == "bounties":
            embeds = [build_bounties_embed()]
        elif value == "linking":
            embeds = [build_linking_embed()]
        elif value == "troubleshooting":
            embeds = [build_troubleshooting_embed()]
        else:
            embeds = build_all_embed(self.guild)

        await interaction.response.edit_message(embeds=embeds, view=self.view)

class HelpView(discord.ui.View):
    def __init__(self, guild: Optional[discord.Guild]):
        super().__init__(timeout=300)  # 5 minutes
        self.add_item(HelpSelect(guild))

class HelpCog(commands.Cog):
    """Friendly, structured help for SV Bounties."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(name="help", description="Show help, quickstart, and admin setup in plain English.")
    @app_commands.describe(category="Pick a specific section (optional).")
    @app_commands.choices(category=[
        app_commands.Choice(name=label, value=value) for label, value in HELP_CATEGORIES
    ])
    async def help_cmd(self, interaction: discord.Interaction, category: Optional[app_commands.Choice[str]] = None):
        await self._show_help(interaction, category.value if category else None)

    @app_commands.command(name="helpp", description="Alias of /help.")
    @app_commands.describe(category="Pick a specific section (optional).")
    @app_commands.choices(category=[
        app_commands.Choice(name=label, value=value) for label, value in HELP_CATEGORIES
    ])
    async def helpp_cmd(self, interaction: discord.Interaction, category: Optional[app_commands.Choice[str]] = None):
        await self._show_help(interaction, category.value if category else None)

    async def _show_help(self, interaction: discord.Interaction, category_value: Optional[str]):
        # Build the default view + embeds
        view = HelpView(interaction.guild)

        if category_value == "quickstart":
            embeds = [build_quickstart_embed(interaction.guild)]
        elif category_value == "admin":
            embeds = [build_admin_embed()]
        elif category_value == "bounties":
            embeds = [build_bounties_embed()]
        elif category_value == "linking":
            embeds = [build_linking_embed()]
        elif category_value == "troubleshooting":
            embeds = [build_troubleshooting_embed()]
        elif category_value == "all":
            embeds = build_all_embed(interaction.guild)
        else:
            # Default landing = quickstart + dropdown
            embeds = [build_quickstart_embed(interaction.guild)]

        await interaction.response.send_message(
            embeds=embeds,
            view=view,
            ephemeral=True  # keep setup noise out of public channels
        )

async def setup(bot: commands.Bot):
    await bot.add_cog(HelpCog(bot))
