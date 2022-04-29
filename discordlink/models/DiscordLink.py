from sqlalchemy import BIGINT, BOOLEAN, INTEGER, TIMESTAMP, VARCHAR, BigInteger, Column, Integer
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class DiscordLink(Base):
	__tablename__ = 'discord_links'

	id = Column(INTEGER, primary_key=True)
	ckey = Column(VARCHAR(32))
	discord_id = Column(BIGINT)
	timestamp = Column(TIMESTAMP)
	one_time_token = Column(VARCHAR(100))
	valid = Column(BOOLEAN)
