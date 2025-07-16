"""
Database-related helpers.
"""
from pathlib import Path
from getpass import getuser

import sqlparse
from sqlalchemy.orm.session import Session as SQLAlchemySession
from sqlalchemy import create_engine, text
from decouple import Config, RepositoryEnv, config

from sqlalchemy_utils import create_database, database_exists


class Session(SQLAlchemySession):
    """Manages persistence operations for ORM-mapped objects.
    """
    def execute(self, statement):
        """Executes a SQL statement construct and returns a CursorResult.

        :param statement: Statement to be executed.
        :type statement: str
        """
        if len(sqlparse.split(statement)) > 1:
            raise ValueError("Only one SQL statement is allowed per file.")

        return super().execute(text(statement))


def load_env(directory):
    """Load environment variables.

    :param directory: Path for metadata directory.
    :type directory: str
    """
    db_params = {}

    # Environment file directory
    dotenv_file = Path(directory) / ".env"

    # If the file does not exist,
    # try to retrieve from the shell environment
    if dotenv_file.exists():
        env_config = Config(RepositoryEnv(dotenv_file))
    else:
        env_config = config

    # Get environment variables
    db_params["user"] = env_config("DB_USER", getuser())
    db_params["host"] = env_config("DB_HOST", "localhost")
    db_params["port"] = env_config("DB_PORT", "5432")
    db_params["database"] = env_config("DB_NAME", "postgres")
    db_params["dialect"] = env_config("DB_DIALECT", "postgresql")

    return db_params


def connect(user, host, port, database, dialect):
    """Connect to database using SQLAlchemy.

    :param user: Name of the database user.
    :type user: str

    :param host: Hostname of the database server.
    :type host: str

    :param port: Network port of the database server.
    :type port: int

    :param database: Name of the database.
    :type database: str

    :param dialect: Dialect of the database.
    :type dialect: str

    :return: SQLAlchemy session.
    :rtype: sqlalchemy.orm.session.Session
    """
    engine = create_engine(
        f"{dialect}://{user}:@{host}:{port}/{database}",
        isolation_level="SERIALIZABLE", pool_recycle=60)

    # Create database
    if not database_exists(engine.url):
        create_database(engine.url)

    # Connect
    conn = Session(bind=engine.connect())

    return conn
