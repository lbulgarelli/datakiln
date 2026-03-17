"""
Database metadata.
"""
import logging
from pathlib import Path
from getpass import getuser
from textwrap import dedent

import sqlparse
from dvc.api import DVCFileSystem
from sqlparse.sql import Statement, Identifier
from sqlalchemy.orm.session import Session as SQLAlchemySession
from sqlalchemy import create_engine, text, URL
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.schema import CreateSchema
from decouple import Config, RepositoryEnv
from sqlalchemy_utils import create_database, database_exists

from data_utils.metadata.table import TableMeta, Base


logging.basicConfig(
    level=logging.INFO, datefmt="%Y-%m-%d %H:%M:%S",
    format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s")

DRIVER = {
    "postgresql": "psycopg2",
    "oracle": "oracledb"
}


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
        """Retrieve database tables.

        :return: Table metadata.
        :rtype: data_utils.metadata.table.TableMeta
        """
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
            """).format(
                role=self.name,
                users='", "'.join(users)
            ))

    def create_role(self, session):
        """Create database role.

        :param session: Database Session.
        :type session: data_utils.metadata.database.Session
        """
        engine = session.get_bind()
        with engine.connect() as conn:
            try:
                conn.execute(text(f"CREATE ROLE {self.name}"))
                conn.commit()
            except ProgrammingError as error:
                message = f'role "{self.name}" already exists'
                if message in error._message():  # pylint: disable=protected-access # noqa: E501
                    return
                raise error

    def execute_directory(self, directory, session, **kwargs):
        """Executes SQL scripts in a given directory.

        :param directory: Path for metadata directory.
        :type directory: str

        :param session: Database Session.
        :type session: data_utils.metadata.database.Session

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
            session.execute(sql, **kwargs)

    def create_metadata(self, session):
        """
        :param session: Database Session.
        :type session: data_utils.metadata.database.Session
        """
        engine = session.get_bind()
        with engine.connect() as conn:
            schema = CreateSchema(Base.metadata.schema, if_not_exists=True)
            conn.execute(schema)
            Base.metadata.create_all(conn, checkfirst=True)
            conn.commit()

    @classmethod
    def load_env(cls, directory):
        """Load environment variables.

        :param directory: Path for metadata directory.
        :type directory: str
        """
        db_params = {}

        # Load .env file
        dotenv_file = Path(directory) / ".env"
        env_config = Config(RepositoryEnv(dotenv_file))

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

    def __init__(self, **kwargs):
        r"""
        :param \**kwargs: SQLAlchemy session attributes.
        :type \**kwargs: dict

        :return: Database session.
        :rtype: data_utils.metadata.database.Session

        """
        self._buffer = []
        super().__init__(**kwargs)

    def _is_identifier(self, token):
        return isinstance(token, Identifier) and token.value != 'MATERIALIZED'

    def execute(self, statement, *args, buffer_size=1, max_statements=1,
                **kwargs):
        r"""Executes a SQL statement construct and returns a CursorResult.

        :param statement: Statement to be executed.
        :type statement: str

        :param buffer_size: Buffer size.
                            The statements will only be sent to the database
                            if the number of statements in the buffer exceeds
                            the value of this parameter.
        :type buffer_size: int

        :param max_statements: Maximum number of statements to allow per
                               execute call.
        :type max_statements: int

        :param \**kwargs: SQLAlchemy execute parameters.
        :type \**kwargs: dict
        """
        if not isinstance(statement, str):
            return super().execute(statement, *args, **kwargs)

        parsed = sqlparse.parse(statement)
        stmt_types = list(map(Statement.get_type, parsed))
        if (
            len(parsed) > max_statements > 0
            and set(stmt_types) != {"DROP", "CREATE"}
        ):
            raise ValueError(
                f"Only {max_statements} SQL statement(s) allowed per file.")

        # For DROP, CREATE pairs, validate the same identifier is used
        if stmt_types == ["DROP", "CREATE"]:
            id_drop = parsed[0].token_matching(self._is_identifier, 0)
            id_create = parsed[1].token_matching(self._is_identifier, 0)

            if id_drop.value != id_create.value:
                raise ValueError("DROP/CREATE have distinct identifiers.")

        # Add to buffer
        self._buffer.append(statement)
        if len(self._buffer) < buffer_size:
            return None

        # Reset buffer
        statement = ";\n\n".join(self._buffer)
        self._buffer = []

        return super().execute(text(statement), *args, **kwargs)

    @classmethod
    def create(cls, user, host, port, database, dialect, password=None):
        # pylint: disable=too-many-arguments, too-many-positional-arguments
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

        :param password: User's password.
        :type password: str

        :return: Database session.
        :rtype: data_utils.metadata.database.Session
        """
        url_object = URL.create(
            f"{dialect}+{DRIVER[dialect]}",
            username=user,
            password=password,
            host=host,
            database=database,
            port=port
        )
        engine = create_engine(
            url_object,
            isolation_level="SERIALIZABLE",
            pool_recycle=60,
            pool_pre_ping=True
        )

        # Create database
        if dialect != "oracle" and not database_exists(url_object):
            create_database(url_object)

        return Session(bind=engine, autoflush=True)
