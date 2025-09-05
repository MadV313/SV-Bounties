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
    def __init__(self, bot): self.bot = bot

    @app_commands.command(name="setexternallinks", description="Set path/URL to external linked_players.json (SV13 persistent repo)")
    @admin_check()
    @app_commands.describe(path="Local path or URL to linked_players.json (leave blank to disable)")
    async def setexternallinks(self, interaction: discord.Interaction, path: str | None = None):
        s = load_settings()
        s["external_links_path"] = path or None
        save_settings(s)
        await interaction.response.send_message(
            f"✅ External links source {'set' if path else 'cleared'}."
            + (f"\n`{path}`" if path else ""),
            ephemeral=True
        )

async def setup(bot: commands.Bot):
    await bot.add_cog(AdminLinks(bot))
