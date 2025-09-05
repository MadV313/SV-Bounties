# cogs/admin_links.py
import discord
from discord import app_commands
from discord.ext import commands
from utils.settings import load_settings, save_settings

def admin_check():
    def pred(i: discord.Interaction):
        perms = getattr(i.user, "guild_permissions", None)
        return bool(perms and (perms.administrator or perms.manage_guild))
    return app_commands.check(lambda i: pred(i))

class AdminLinks(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

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

async def setup(bot: commands.Bot):
    await bot.add_cog(AdminLinks(bot))
