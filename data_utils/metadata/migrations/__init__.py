"""
Migrations of the metadata tables.
"""
from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import inspect

from data_utils.metadata.table import Base


# The version of databases built before there were migrations
BASELINE = "0001"


def get_config(conn):
    """Configuration of the migrations, which run on a connection.

    :param conn: SQLAlchemy connection, whose transaction is the one of the
                 migrations.
    :type conn: sqlalchemy.Connection

    :return: Alembic configuration.
    :rtype: alembic.config.Config
    """
    config = Config()
    config.set_main_option("script_location", str(Path(__file__).parent))
    config.attributes["connection"] = conn
    return config


def upgrade(conn):
    """Bring the metadata tables to the current version.

    :param conn: SQLAlchemy connection, whose transaction is the one of the
                 migrations.
    :type conn: sqlalchemy.Connection
    """
    config = get_config(conn)

    # A database built before the migrations has the tables of the first
    # version, but no record of it
    schema = Base.metadata.schema
    inspector = inspect(conn)
    if (
        inspector.has_table("table_", schema)
        and not inspector.has_table("alembic_version", schema)
    ):
        command.stamp(config, BASELINE)

    command.upgrade(config, "head")
