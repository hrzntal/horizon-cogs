from .discordlink import DiscordLinkCog

def setup(bot):
	bot.add_cog(DiscordLinkCog(bot))
