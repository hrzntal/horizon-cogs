import logging
import socket

from discord import DiscordException, Embed, Guild
from redbot.core import checks, commands, Config
from redbot.core.bot import Red
from redbot.core.commands import Cog, Context
from sqlalchemy.engine import ChunkedIteratorResult, ScalarResult
from sqlalchemy.exc import ResourceClosedError
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from typing import List

__version__ = "2.0.0"
__author__ = ["atakiya"]

log = logging.getLogger("red.horizon.cogs.deebee")

class DeeBee(Cog):
	def __init__(self, bot: Red):
		self.bot = bot
		self.config = Config.get_conf(self, identifier=10411412211011697108, force_registration=True)

		# Config variables shown on query
		self.visible_config = [
			"db_dialect",
			"db_driver",
			"db_host",
			"db_port",
			"db_user",
			"db_schema",
		]
		default_guild = {
			"db_dialect": "mysql",
			"db_driver": "asyncmy",
			"db_host": "127.0.0.1",
			"db_port": 3306,
			"db_user": "ss13",
			"db_password": "password",
			"db_schema": "feedback",
		}
		self.config.register_guild(**default_guild)
		self.engine = None

	@commands.guild_only()
	@commands.group()
	@checks.admin_or_permissions(administrator=True)
	async def deebee(self, ctx: Context):
		"""
		Database connector management commands
		"""
		pass

	@commands.guild_only()
	@deebee.group(aliases=["config", "cfg"])
	@checks.is_owner()
	async def preferences(self, ctx: Context):
		"""
		Configuration for the DB connector
		"""
		pass

	@deebee.command()
	async def reconnect(self, ctx: Context):
		"""
		Recreate the pool (for when it dies)
		"""
		await self.recreate_engine(ctx.guild)
		await ctx.send(f"Database Connected")

	@preferences.command()
	async def dialect(self, ctx: Context, dialect: str):
		try:
			await self.config.guild(ctx.guild).db_dialect.set(dialect)
			await ctx.send(f"Set database dialect to: `{dialect}`")
		except (ValueError, KeyError, AttributeError):
			await ctx.send(
				"""Invalid dialect. Has to be one of `mssql`, `mysql`, `oracle`, `postgresql`, `sqlite`.
				For a full list visit <https://docs.sqlalchemy.org/en/latest/dialects/index.html>
				Keep in mind that external dialects require additional dependencies to be installed"""
			)

	@preferences.command()
	async def driver(self, ctx: Context, driver: str):
		try:
			await self.config.guild(ctx.guild).db_driver.set(driver)
			await ctx.send(f"Set database driver to: `{driver}`")
		except (ValueError, KeyError, AttributeError):
			await ctx.send(
				f"Invalid driver. Visit <https://docs.sqlalchemy.org/en/latest/dialects/index.html> for a full list."
			)
	@preferences.command()
	async def host(self, ctx: Context, db_host: str):
		"""
		Sets the Database host, defaults to localhost (127.0.0.1)
		"""
		try:
			await self.config.guild(ctx.guild).db_host.set(db_host)
			await ctx.send(f"Database host set to: `{db_host}`")
		except (ValueError, KeyError, AttributeError):
			await ctx.send(
				"There was an error setting the database's ip/hostname. Please check your entry and try again!"
			)

	@preferences.command()
	async def port(self, ctx: Context, db_port: int):
		"""
		Sets the Database port, defaults to 3306
		"""
		try:
			if (
				1024 <= db_port <= 65535
			):  # We don't want to allow reserved ports to be set
				await self.config.guild(ctx.guild).db_port.set(db_port)
				await ctx.send(f"Database port set to: `{db_port}`")
			else:
				await ctx.send(f"{db_port} is not a valid port!")
		except (ValueError, KeyError, AttributeError):
			await ctx.send(
				"There was a problem setting your port. Please check to ensure you're attempting to use a port from 1024 to 65535"
			)

	@preferences.command(aliases=["name", "user"])
	async def username(self, ctx: Context, user: str):
		"""
		Sets the user that will be used with the database. Defaults to SS13
		"""
		try:
			await self.config.guild(ctx.guild).db_user.set(user)
			await ctx.send(f"User set to: `{user}`")
		except (ValueError, KeyError, AttributeError):
			await ctx.send(
				"There was a problem setting the username for your database."
			)

	@preferences.command()
	async def password(self, ctx: Context, passwd: str):
		"""
		Sets the password for connecting to the database

		This will be stored locally, it is recommended to ensure that your user cannot write to the database
		"""
		try:
			await self.config.guild(ctx.guild).db_password.set(passwd)
			await ctx.send("Your password has been set.")
			try:
				await ctx.message.delete()
			except (DiscordException):
				await ctx.send(
					"I do not have the required permissions to delete messages, please remove/edit the password manually."
				)
		except (ValueError, KeyError, AttributeError):
			await ctx.send(
				"There was a problem setting the password for your database."
			)

	@preferences.command(aliases=["db"])
	async def database(self, ctx: Context, db: str):
		"""
		Sets the database to login to, defaults to feedback
		"""
		try:
			await self.config.guild(ctx.guild).db_schema.set(db)
			await ctx.send(f"Database set to: `{db}`")
		except (ValueError, KeyError, AttributeError):
			await ctx.send("There was a problem setting your notes database.")

	@preferences.command()
	async def current(self, ctx: Context):
		"""
		Gets the current settings for the database
		"""
		settings = await self.config.guild(ctx.guild).all()
		embed = Embed(title="__Current settings:__")
		for k, v in settings.items():
			# Ensures that the database password is not sent
			# Whitelist for extra safety
			if k in self.visible_config:
				if v == "":
					v = None
				embed.add_field(name=f"{k}:", value=v, inline=False)
			else:
				embed.add_field(name=f"{k}:", value="`redacted`", inline=False)
		await ctx.send(embed=embed)

	async def get_engine(self, guild: Guild):
		"""
		Returns the engine for the database, or creates one with guild context configuration if it doesn't exist
		"""
		if not self.engine:
			await self.create_engine(guild)
		return self.engine

	async def create_engine(self, guild: Guild):
		dialect = await self.config.guild(guild).db_dialect()
		driver = await self.config.guild(guild).db_driver()
		schema = await self.config.guild(guild).db_schema()
		host = socket.gethostbyname(await self.config.guild(guild).db_host())
		port = await self.config.guild(guild).db_port()
		user = await self.config.guild(guild).db_user()
		passwd = await self.config.guild(guild).db_password()

		self.engine = create_async_engine(
			f"{dialect}+{driver}://{user}:{passwd}@{host}:{port}/{schema}",
			echo=False,
			future=True,
			pool_timeout=5,
			pool_recycle=300
		)

	async def recreate_engine(self, guild: Guild):
		"""
		Recreates the engine with the current guild context configuration
		"""
		if self.engine:
			await self.engine.dispose()
		self.engine = await self.create_engine(guild)

	async def query(self, ctx: Context, stmt: str, commit: bool=False) -> List[ScalarResult] or None:
		"""
		Use our active engine pool to query the database with the given statement, including parameters
		Shorthand to pass in full Context instead of Guild
		"""
		return await self.query(ctx.guild, stmt, commit)

	async def query(self, guild: Guild, stmt: str, commit: bool=False, single_result: bool=False) -> List[ScalarResult] or ScalarResult or None:
		"""
		Use our active engine pool to query the database with the given statement, including parameters
		"""
		if not self.engine:
			await self.create_engine(guild)

		engine = await self.get_engine(guild)
		async_session = sessionmaker(
			engine,
			class_=AsyncSession
		)

		try:
			log.debug(f"Executing query statment {stmt}")
			async with async_session() as session:
				session: Session
				result: ChunkedIteratorResult = await session.execute(stmt)
				if commit:
					await session.commit()
				if result:
					try:
						if single_result:
							return result.scalar_one_or_none()
						else:
							return result.scalars().all()
					except (ResourceClosedError):
						return None
				else:
					return None
		except:
			raise

	async def query_commit(self, guild: Guild, stmt: str) -> List[ScalarResult] or None:
		"""
		Use a session to pass in the given query, with a commit

		Same as passing `commit=True` to query_database
		"""
		return await self.query(guild, stmt, commit=True)

	async def query_single(self, guild: Guild, stmt: str, commit: bool=False) -> ScalarResult or None:
		"""
		Use a session to pass in the given query, returning a single result

		Same as passing `single_result=True` to query_database
		"""
		return await self.query(guild, stmt, commit, single_result=True)
