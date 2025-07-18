"""
Database-related helpers.
"""
import logging
from pathlib import Path
from getpass import getuser
from textwrap import dedent

import sqlparse
from dvc.api import DVCFileSystem
from sqlparse.sql import Statement, Identifier
from sqlalchemy.orm.session import Session as SQLAlchemySession
from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError
from decouple import Config, RepositoryEnv, config
from sqlalchemy_utils import create_database, database_exists

from data_utils.metadata.table import TableMeta


logging.basicConfig(
    level=logging.INFO, datefmt="%Y-%m-%d %H:%M:%S",
    format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s")


class DatabaseMeta():
    """Database metadata."""
    def __init__(self, directory, name):
        """
        :param directory: Path for metadata directory.
        :type directory: str

        :param name: Name of the database.
        :type name: str
        """
        self.directory = Path(directory)
        self.name = name

        # Retrieve files to ignore
        self.ignore = []
        if (path := self.directory / ".dataignore").exists():
            with path.open("r", encoding="utf-8") as file:
                self.ignore = file.read().splitlines()

    @property
    def tables(self):
        # Load DVC repository
        data_repo = DVCFileSystem(self.directory / "data")

        # Retrieve table metadata
        tables = []
        for meta_path in sorted((self.directory / "metadata").glob("*.json")):
            meta_relative = meta_path.relative_to(self.directory)
            if str(meta_relative) in self.ignore:
                continue

            tables.append(TableMeta.from_json(meta_path, data_repo))
        return tables

    def grant(self, conn, users=None):
        """Grants read on all objects in database.

        :param conn: SQLAlchemy connection or session.
        :type conn: sqlalchemy.Engine
        """
        # Iterates all schemas in database and grant
        # read permissions to database role
        conn.execute(dedent(f"""
        DO $do$
        DECLARE
        schema TEXT;
        BEGIN
        FOR schema IN (
            SELECT nspname
            FROM pg_namespace
            WHERE nspname NOT LIKE 'information_schema%'
            AND nspname NOT LIKE 'pg_catalog%'
            AND nspname NOT LIKE 'pg_toast%'
            AND nspname NOT LIKE 'pg_temp%'
        ) LOOP
            EXECUTE format($$ GRANT USAGE ON SCHEMA %I TO {self.name} $$, schema);
            EXECUTE format($$ GRANT SELECT ON ALL SEQUENCES IN SCHEMA %I TO {self.name} $$, schema);
            EXECUTE format($$ GRANT SELECT ON ALL TABLES IN SCHEMA %I TO {self.name} $$, schema);
        END LOOP;
        END;
        $do$;"""))  # noqa: E501

        # Grant permissions to users by proxy of the database role
        if users:
            conn.execute(dedent("""
            GRANT {role} TO "{users}"
            """)).format(
                role=self.name,
                users='", "'.join(users)
            )

    def create_role(self, conn):
        """Connect to database using SQLAlchemy.

        :param conn: SQLAlchemy connection or session.
        :type conn: sqlalchemy.Engine
        """
        try:
            conn.execute(dedent("""
            CREATE ROLE {database}
            """).format(database=self.name))
        except ProgrammingError as error:
            message = f'role "{self.name}" already exists'
            if message in error._message():  # pylint: disable=protected-access
                return
            raise error

    def execute_directory(self, directory, conn):
        """Executes SQL scripts in a given directory.

        :param directory: Path for metadata directory.
        :type directory: str

        :param conn: SQLAlchemy connection or session.
        :type conn: sqlalchemy.Engine

        :param ignore: List of files to ignore.
        :type ignore: str
        """
        subdirectory = self.directory / directory
        for file_name in sorted(subdirectory.glob("**/*.sql")):
            file_relative = file_name.relative_to(self.directory)
            if str(file_relative) in self.ignore:
                continue
            logging.info("Script '%s': RUNNING",
                         file_name.relative_to(self.directory))

            # Load file
            with open(file_name, "r", encoding="utf-8") as file:
                sql = file.read()

            # Execute query
            conn.execute(sql)

            # Flush to database
            conn.flush()

    @classmethod
    def create_metadata(cls, conn):
        """
        :param conn: SQLAlchemy connection or session.
        :type conn: sqlalchemy.Engine
        """
        conn.execute("CREATE SCHEMA IF NOT EXISTS _metadata")
        conn.execute(dedent("""
        CREATE TABLE IF NOT EXISTS _metadata.table_(
            schema_ TEXT
          , table_ TEXT
          , hash TEXT
          , PRIMARY KEY (schema_, table_)
        )
        """))
        conn.execute(dedent("""
        CREATE TABLE IF NOT EXISTS _metadata.file_(
            path TEXT
          , schema_ TEXT
          , table_ TEXT
          , hash TEXT
          , PRIMARY KEY (path)
          , FOREIGN KEY (schema_, table_)
            REFERENCES _metadata (schema_, table_)
        )
        """))

    @classmethod
    def load_env(cls, directory):
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


class Session(SQLAlchemySession):
    # pylint: disable=too-few-public-methods
    """Manages persistence operations for ORM-mapped objects.
    """
    def _is_identifier(self, token):
        return isinstance(token, Identifier) and token.value != 'MATERIALIZED'

    def execute(self, statement):
        """Executes a SQL statement construct and returns a CursorResult.

        :param statement: Statement to be executed.
        :type statement: str
        """
        parsed = sqlparse.parse(statement)
        stmt_types = list(map(Statement.get_type, parsed))
        if len(parsed) > 1 and stmt_types != ["DROP", "CREATE"]:
            raise ValueError("Only one SQL statement is allowed per file.")

        # For DROP, CREATE pairs, validate the same identifier is used
        if stmt_types == ["DROP", "CREATE"]:
            id_drop = parsed[0].token_matching(self._is_identifier, 0)
            id_create = parsed[1].token_matching(self._is_identifier, 0)

            if id_drop.value != id_create.value:
                raise ValueError("DROP/CREATE have distinct identifiers.")

        return super().execute(text(statement))

    @classmethod
    def create(cls, user, host, port, database, dialect):
        # pylint: disable=too-many-arguments
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
