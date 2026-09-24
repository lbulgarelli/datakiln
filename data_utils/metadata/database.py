"""
Database metadata.
"""
import logging
from pathlib import Path
from getpass import getuser
from textwrap import dedent

import sqlglot
import sqlparse
from sqlglot import exp
from dvc.api import DVCFileSystem
from sqlalchemy.orm.session import Session as SQLAlchemySession
from sqlalchemy import create_engine, text, URL, event, Engine
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.schema import CreateSchema
from decouple import Config, RepositoryEnv
from sqlalchemy_utils import create_database, database_exists

from data_utils.metadata.table import TableMeta, Base


DRIVER = {
    "postgresql": "psycopg2",
    "oracle": "oracledb"
}
logger = logging.getLogger(__name__)


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

    def grant(self, session, users=None):
        """Grants read on all objects in database.

        :param session: Database Session.
        :type session: data_utils.metadata.database.Session
        """
        # Iterates all schemas in database and grant
        # read permissions to database role
        session.execute(dedent(f"""
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
        $do$;"""), validate=False)  # noqa: E501

        # Grant permissions to users by proxy of the database role
        if users:
            session.execute(dedent("""
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
            logger.info("Script '%s': RUNNING",
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
        db_params["isolation_level"] = env_config(
            "DB_ISOLATION_LEVEL",
            "READ COMMITTED"
        )

        return db_params


@event.listens_for(Engine, "before_cursor_execute", retval=True)
def before_cursor_execute(conn, cursor, statement, parameters, context,
                          executemany):
    # pylint: disable=too-many-arguments, too-many-positional-arguments
    # pylint: disable=unused-argument, broad-exception-caught
    """Format SQL query before execution."""
    try:
        statement = sqlparse.format(
            statement,
            reindent_aligned=True,
            keyword_case="upper"
        )
    except Exception:
        pass
    return statement, parameters


@event.listens_for(Engine, "connect")
def set_oracle_nls(dbapi_connection, _connection_record):
    """Set standard NLS date/timestamp formats on Oracle connections."""
    oracle_drivers = ("oracledb", "cx_Oracle")
    if dbapi_connection.__class__.__module__.startswith(oracle_drivers):
        cursor = dbapi_connection.cursor()
        try:
            formats = [
                ("NLS_DATE_FORMAT", "YYYY-MM-DD HH24:MI:SS"),
                ("NLS_TIMESTAMP_FORMAT", "YYYY-MM-DD HH24:MI:SS"),
                ("NLS_TIMESTAMP_TZ_FORMAT", "YYYY-MM-DD HH24:MI:SSXFF TZR"),
            ]
            for param, value in formats:
                cursor.execute(f"ALTER SESSION SET {param} = '{value}'")
        finally:
            cursor.close()


class Session(SQLAlchemySession):
    # pylint: disable=too-few-public-methods
    """Manages persistence operations for ORM-mapped objects.
    """

    DIALECT_MAP = {
        "postgresql": "postgres",
        "mssql": "tsql"
    }

    def __init__(self, **kwargs):
        r"""
        :param \**kwargs: SQLAlchemy session attributes.
        :type \**kwargs: dict

        :return: Database session.
        :rtype: data_utils.metadata.database.Session

        """
        self._buffer = []
        super().__init__(**kwargs)

    def execute(self, statement, *args, buffer_size=1, max_statements=1,
                validate=True, **kwargs):
        # pylint: disable=too-many-locals
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
        # If statement is not a SQL string, let sqlalchemy handle it
        if not isinstance(statement, str):
            return super().execute(statement, *args, **kwargs)

        # Skip validation
        if not validate:
            statement = text(statement)
            return super().execute(statement, *args, **kwargs)

        # Map dialects between SQLAlchemy and SQLGlot
        dialect = self.get_bind().dialect.name
        expressions = sqlglot.parse(
            statement,
            read=self.DIALECT_MAP.get(dialect, dialect)
        )

        # Validate expression sets
        match expressions:
            # DROP/CREATE pairs
            case [exp.Drop() as drop, exp.Create() as create]:
                # Should drop and create same object (kind)
                if drop.kind != create.kind:
                    raise ValueError("DROP/CREATE have distinct types.")
                # Should drop and create same object (name)
                if drop.find(exp.Table).parts != create.find(exp.Table).parts:
                    raise ValueError("DROP/CREATE have distinct identifiers.")
            # CREATE INDEX lists
            case [exp.Create(kind="INDEX") as index, *rest]:
                table = index.find(exp.Table)
                column_sets = [tuple(index.find_all(exp.Column))]
                # Sanity checks
                for expression in rest:
                    # Expressions other than CREATE INDEX
                    signature = (type(expression), expression.kind)
                    if signature != (exp.Create, "INDEX"):
                        raise ValueError(
                            "Indexes should be created in a separate file."
                        )
                    # Multiple tables
                    if expression.find(exp.Table) != table:
                        raise ValueError(
                            "Only group indexes for the same table."
                        )
                    column_set = tuple(expression.find_all(exp.Column))
                    column_sets.append(column_set)
                # Multiple indexes for same column set
                if len(set(column_sets)) != len(column_sets):
                    raise ValueError(
                        "Multiple indexes on the same set of columns."
                    )
            # Otherwise we only allow max_statements per file
            case _ if len(expressions) > max_statements > 0:
                raise ValueError(
                    f"Only {max_statements} SQL statement(s) allowed per file."
                )

        # Add to buffer
        self._buffer.append(statement)
        if len(self._buffer) < buffer_size:
            return None

        # Reset buffer
        statement = ";\n\n".join(self._buffer)
        self._buffer = []

        return super().execute(text(statement), *args, **kwargs)

    @classmethod
    def create(
        cls,
        user,
        host,
        port,
        database,
        dialect,
        password=None,
        isolation_level="READ COMMITTED"
    ):
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

        :param isolation_level: Transaction isolation level.
        :type isolation_level: str

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
            isolation_level=isolation_level,
            pool_recycle=60,
            pool_pre_ping=True
        )

        # Create database
        if dialect != "oracle" and not database_exists(url_object):
            create_database(url_object)

        return Session(bind=engine, autoflush=True)
