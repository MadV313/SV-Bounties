import discord
from discord import app_commands
from discord.ext import commands

class AdminMisc(commands.Cog):
    def __init__(self, bot): 
        self.bot = bot

    @app_commands.command(name="sync", description="Force sync slash commands (admin)")
    async def sync(self, interaction: discord.Interaction):
        if not interaction.user.guild_permissions.administrator:
            return await interaction.response.send_message("❌ You don’t have permission.", ephemeral=True)
        cmds = await interaction.client.tree.sync()
        await interaction.response.send_message(f"✅ Synced {len(cmds)} command(s).", ephemeral=True)

async def setup(bot):
    await bot.add_cog(AdminMisc(bot))
