import io
import logging

from .models.DiscordLink import DiscordLink
from datetime import datetime, timedelta, timezone
from discord import DiscordException, Embed, Guild, Member, Message, Role
from redbot.core import checks, commands, Config
from redbot.core.bot import Red
from redbot.core.commands import Cog, Context
from sqlalchemy import select, update
from sqlalchemy.engine import Row
from typing import List, MutableMapping

__version__ = "1.0.1"
__author__ = ["atakiya"]

log = logging.getLogger("red.horizon.cogs.discordlink")

class DiscordLinkCog(Cog):
	def __init__(self, bot: Red):
		self.bot = bot
		self.config = Config.get_conf(self, identifier=10411412211011697108, force_registration=True)

		default_guild = {
			"verified_role": None,
			"members_only": False,
		}
		self.config.register_guild(**default_guild)

		self.db = self.get_database()
		# Used for tracking last sent bot message
		self.last_message = None

	async def red_get_data_for_user(self, *, user_id: int) -> MutableMapping[str, io.BytesIO]:
		return MutableMapping()

	@commands.group()
	async def discordlink(self, ctx: Context):
		"""
		Manage discord links
		"""
		pass

	@commands.guild_only()
	@discordlink.group()
	@checks.admin_or_permissions(administrator=True)
	async def preferences(self, ctx: Context):
		pass

	@preferences.command()
	async def membersonly(self, ctx: Context):
		"""
		Toggle whether or not to restrict gameserver entry to guild members only
		"""
		current_setting = await self.config.guild(ctx.guild).members_only()
		new_setting = not current_setting
		await self.config.guild(ctx.guild).members_only.set(new_setting)
		await ctx.send(f"Guild Member restricted server entry is now {'enabled' if new_setting else 'disabled'}")

	@preferences.command()
	async def verifiedrole(self, ctx: Context, new_role_id: int = None):
		"""
		Set or get the role that will be given to users who have verified their Discord account.
		"""
		current_role_id: int = await self.config.guild(ctx.guild).verified_role()
		current_role: Role = ctx.guild.get_role(current_role_id)
		try:
			new_role: Role = ctx.guild.get_role(new_role_id)

			if current_role == new_role:
				return await ctx.send(f"The verified role is already set to `{current_role.name}`")

			if new_role_id == -1:
				await self.config.guild(ctx.guild).verified_role.set(None)
				return await ctx.send("Users will no longer gain a role after verifying.")

			if not new_role:
				return await ctx.send("That role doesn't exist.")

			await self.config.guild(ctx.guild).verified_role.set(new_role_id)
			await ctx.send(f"Verified role set to {new_role.name}.")

		except (ValueError, KeyError, AttributeError):
			if(AttributeError):
				if current_role:
					return await ctx.send(f"Current verified role: `{current_role.name}`\n{AttributeError}")
				return await ctx.send("No verified role set.")
			return await ctx.send(f"There was a problem setting the verified role.")

	@commands.cooldown(2, 60, type=commands.BucketType.user)
	@commands.cooldown(6, 60, type=commands.BucketType.guild)
	@commands.max_concurrency(3, per=commands.BucketType.guild, wait=False)
	@commands.command()
	async def verify(self, ctx: Context, *, one_time_password: str = None):
		"""
		Link your BYOND key with your Discord account.
		"""

		verified_role: int = await self.config.guild(ctx.guild).verified_role()

		# First let's try to delete the message, as the OTP is still to be handled like a secret.
		try:
			await ctx.message.delete()
		except (DiscordException):
			await ctx.send("I can't delete messages in this channel.\nPlease delete the message with your OTP token yourself.")

		# Check if user already has the role, if so, don't bother doing anything.
		if verified_role in ctx.author.roles:
			return await ctx.send("You are already verified.\nIf this is an error, please contact staff")

		embed = Embed(
			title="Please wait...",
			description="Attempting to verify your account..."
		)
		message: Message = await ctx.send(embed=embed)
		self.last_message = message

		# Start showing a typing indicator
		async with ctx.typing():
			discord_link = await self.discord_link_for_discord_id(ctx.guild, ctx.author.id)

			# Check if they might already be verified.
			if discord_link and discord_link.valid:
				# They are already verified, so let's just add any missing role(s).
				if verified_role:
					try:
						await ctx.author.add_roles(ctx.guild.get_role(verified_role), reason=f"Reverified by Discord Link (ckey: `{discord_link.ckey}`)")
					except (DiscordException):
						# Uh oh, we couldn't add the role.
						log.exception(f"Failed to add role {verified_role} to {ctx.author.id}, {DiscordException}")
						await ctx.send("I can't add the missing role(s) to you. Please contact staff.")

				# Let them know of course
				embed.title = "Success!"
				embed.description = "You are already verified. If you were missing any roles, they have been added."
				embed.color = 0x00FF00
				return await message.edit(embed=embed)

			# No OTP and no valid Discordlink. This won't do.
			if one_time_password is None:
				embed.title = "Could not verify!"
				embed.description = f"""No OTP token given.
										Please log into the server and get your OTP token.
										Usage: {ctx.prefix}verify super-cool-token"""
				embed.set_footer(text="Error: No token passed.")
				embed.color = 0xFF0000
				return await message.edit(embed=embed)

			# They have supplied an OTP token, let's see if its valid
			discord_link = await self.discord_link_for_token(ctx, one_time_password)
			# It is not valid, or it doesn't exist.
			if not discord_link:
				embed.title = "Could not verify!"
				embed.description = """
					Invalid OTP token.
					Please make sure you generated a token by joining the server first.
					Else make sure you copied the token correctly. Do not add anything after the token.
					The token should have the format of words between dashes.
					e.g. `super-cool-token`"""
				embed.set_footer(text="Error: Invalid or expired OTP token.")
				embed.color = 0xFF0000
				return await message.edit(embed=embed)

			# It does exist and matched. Let's continue.
			# Update their db entry with their discordid.
			await self.update_discord_link(ctx, one_time_password, ctx.author.id)

			# Give them roles too, if any.
			if verified_role:
				await ctx.author.add_roles(ctx.guild.get_role(verified_role), reason=f"Verified by Discord Link (ckey: `{discord_link.ckey}`)")

			# Expensive, but let's just check if all went well.
			discord_link = await self.discord_link_for_discord_id(ctx.guild, ctx.author.id)
			# It did not, uh oh.
			if not discord_link:
				log.warning(f"The returned discord {ctx.author.id}.")
				embed.title = "Could not verify!"
				embed.description = "Something went wrong. Please contact staff before proceeding."
				embed.set_footer(text="Error: Could not verify link after creation.")
				embed.color = 0xFF0000
				return await message.edit(embed=embed)

		# Let them know they've been verified.
		embed.title = "Success!"
		embed.description = "Verification complete!\nYou can now log in to the server."
		embed.color = 0x00FF00
		await message.edit(embed=embed, delete_after=30)
		# Reset last message reference
		self.last_message = None
		return

	@verify.error
	async def verify_error(self, ctx: Context, error):
		# Delete the user's message if it exists.
		# This could be caused by the deletion call in verify not being invoked before an exception is raised.
		try:
			await ctx.message.delete()
		except (DiscordException):
			pass

		embed = Embed(
				description=f"```\n{format(error)}```",
				color=0xFF0000
		)

		if isinstance(error, commands.MaxConcurrencyReached):
			embed.description = "There are too many verifications in progress, please try again in 30 seconds."
			embed.color = 0xFFFF00
		elif isinstance(error, commands.CommandOnCooldown):
			embed.description = f"{format(error)}"
			embed.color = 0xFFFF00
		else:
			log.exception(error)
			embed.title = "Unexpected error occurred."
			embed.description = f"Please try again. If this error persists, contact staff.\n```\n{format(error)}```"
		await ctx.send(embed=embed, delete_after=30)

		# Also delete the bot's waiting message, as an error occured and no further processing will be done.
		if self.last_message:
			await self.last_message.delete()
			self.last_message = None

	@commands.Cog.listener()
	async def on_member_join(self, member: Member):
		await self.handle_member_join(member)

	async def handle_member_join(self, member: Member):
		guild = member.guild
		if guild is None:
			return

		members_only = await self.config.guild(member.guild).members_only()

		if not members_only:
			return

		await self.discord_link_for_discord_id(guild, member.id)

	@commands.Cog.listener()
	async def on_member_remove(self, member: Member):
		guild = member.guild
		if guild is None:
			return

		if await self.bot.cog_disabled_in_guild(self, guild):
			return

		await self.handle_member_remove(member)

	async def handle_member_remove(self, member: Member):
		guild = member.guild

		if not guild:
			return

		# Are we restricting server entry to guild members only?
		members_only = await self.config.guild(guild).members_only()
		if not members_only:
			return

		await self.clear_all_valid_discord_links_for_discord_id(guild, member.id)

	async def update_discord_link(self, ctx: Context, one_time_token: str, user_discord_snowflake: str) -> bool:
		"""
		Given a one time token, and a discord user snowflake, insert the snowflake for the matching record in the discord links table

		Parameters
		----------
		one_time_token: str
			The one time token identifying the user
		user_discord_snowflake: str
			The discord id of the user
		"""

		#stmt = text("""
		#	UPDATE :tablename
		#	SET
		#		discord_id = :discord_id,
		#		valid = TRUE
		#	WHERE
		#		one_time_token = :one_time_token
		#	AND timestamp >= Now() - INTERVAL 4 HOUR
		#	AND :discord_id IS NULL
		#""").bindparams(tablename=DiscordLink, discord_id=user_discord_snowflake, one_time_token=one_time_token)

		stmt = update(
			DiscordLink
		).where(
			DiscordLink.one_time_token == one_time_token,
			DiscordLink.timestamp >= datetime.now(timezone.utc) - timedelta(hours=4)
		).values(
			discord_id = user_discord_snowflake,
			valid = True
		)

		await self.db.query(ctx, stmt, commit=True)

	async def discord_link_for_token(self, ctx: Context, one_time_token: str) -> DiscordLink or None:
		"""
		Given a one time token, search the discord_links table for that one time token and return the ckey it's connected to
		checks that the timestamp of the one time token has not exceeded 4 hours (hence expired)
		"""

		#stmt = text("""
		#	SELECT ckey
		#	FROM :tablename
		#	WHERE one_time_token = :one_time_token
		#	AND timestamp >= Now() - INTERVAL 4 HOUR
		#	AND discord_id IS NULL
		#	ORDER BY timestamp DESC
		#	LIMIT 1
		#""").bindparams(tablename=DiscordLink, one_time_token=one_time_token)

		stmt = select(
			DiscordLink
		).where(
			DiscordLink.one_time_token == one_time_token,
			DiscordLink.timestamp >= datetime.now(timezone.utc) - timedelta(hours=4)
		).order_by(
			DiscordLink.timestamp.desc()
		).limit(1)

		result: Row = await self.db.query_single(ctx, stmt)
		log.debug(f"discord_link_for_token: {result}")
		return result

	async def discord_link_for_discord_id(self, guild: Guild, discord_id: str) -> DiscordLink or None:
		"""
		Given a valid discord id, return the latest record linked to that user
		"""

		#stmt = text("""
		#	SELECT *
		#	FROM :tablename
		#	WHERE discord_id = :discord_id
		#	AND ckey IS NOT NULL
		#	ORDER BY timestamp DESC
		#	LIMIT 1
		#""").bindparams(tablename=DiscordLink, discord_id=discord_id)
		stmt = select(
			DiscordLink
		).where(
			DiscordLink.discord_id == discord_id
		).order_by(
			DiscordLink.timestamp.desc()
		).limit(1)

		result: Row = await self.db.query_single(guild, stmt)
		log.debug(f"discord_link_for_discord_id: {result}")
		return result

	async def discord_link_for_ckey(self, ctx: Context, ckey: str) -> DiscordLink or None:
		"""
		Given a valid ckey, return the latest record linked to that user
		"""

		#stmt = text("""
		#	SELECT *
		#	FROM :tablename
		#	WHERE ckey = :ckey
		#	AND discord_id IS NOT NULL
		#	ORDER BY timestamp DESC
		#	LIMIT 1
		#""").bindparams(tablename=tablename, ckey=ckey)

		stmt = select(
			DiscordLink
		).where(
			DiscordLink.ckey == ckey,
			DiscordLink.discord_id != None
		).order_by(
			DiscordLink.timestamp.desc()
		).limit(1)

		result: Row = await self.db.query_single(ctx, stmt)
		log.debug(f"discord_link_for_ckey: {result}")
		return result

	async def clear_all_valid_discord_links_for_ckey(self, ctx: Context, ckey: str):
		"""
		Set the valid field to false for all links for the given ckey

		Parameters
		----------
		ckey: str
			The ckey to invalidate the links for
		"""

		#stmt = text("""
		#	UPDATE :tablename
		#	SET valid = FALSE
		#	WHERE ckey = :ckey
		#	AND valid = TRUE
		#""").bindparams(tablename=tablename, ckey=ckey)

		stmt = update(
			DiscordLink
		).where(
			DiscordLink.ckey == ckey,
			DiscordLink.valid == True
		).values(
			valid = False
		)

		await self.db.query(ctx, stmt, commit=True)

	async def clear_all_valid_discord_links_for_discord_id(self, guild: Guild, discord_id: int):
		"""
		Set the valid field to false for all links for the given discord id

		Parameters
		----------
		discord_id: str
			The discord id to invalidate the links for
		"""

		#stmt = text("""
		#	UPDATE :tablename
		#	SET valid = FALSE
		#	WHERE discord_id = :discord_id
		#	AND valid = TRUE
		#""").bindparams(tablename=tablename, discord_id=discord_id)

		stmt = update(
			DiscordLink
		).where(
			DiscordLink.discord_id == discord_id,
			DiscordLink.valid == True
		).values(
			valid = False
		)

		await self.db.query(guild, stmt, commit=True)

	async def all_discord_links_for_ckey(self, ctx: Context, ckey: str) -> List[DiscordLink]:
		"""
		Given a valid ckey, return a list of all the valid records in the discord_links table for this user as discord link records
		ordered by timestamp descending
		"""

		#stmt = text("""
		#	SELECT * FROM :tablename
		#	WHERE ckey = :ckey
		#	AND discord_id IS NOT NULL
		#	ORDER BY timestamp DESC
		#""").bindparams(tablename=tablename, ckey=ckey)

		stmt = select(
			DiscordLink
		).where(
			DiscordLink.ckey == ckey,
			DiscordLink.discord_id != None
		).order_by(
			DiscordLink.timestamp.desc()
		)

		result: List[Row] = await self.db.query(ctx, stmt)
		log.debug(f"all_discord_links_for_ckey: {result}")
		return result

	def get_database(self) -> Cog:
		db = self.bot.get_cog("DeeBee")
		if not db:
			raise ModuleNotFoundError("Database cog not found.")
		return db
