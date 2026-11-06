"""
Database metadata.
"""
import logging
from pathlib import Path
from getpass import getuser
from textwrap import dedent
from functools import cached_property

import sqlglot
import sqlparse
from sqlglot import exp
from sqlalchemy.orm.session import Session as SQLAlchemySession
from sqlalchemy import (
    create_engine, inspect, select, text, URL, event, Engine
)
from sqlalchemy.exc import DBAPIError, ProgrammingError
from sqlalchemy.schema import CreateSchema
from decouple import Config, RepositoryEmpty, RepositoryEnv
from sqlalchemy_utils import create_database, database_exists

from datakiln.metadata.table import TableMeta, Object, Script, Base
from datakiln.metadata.dialect import DIALECT_MAP
from datakiln.metadata.files import DataFiles
from datakiln.metadata.graph import DatabaseGraph
from datakiln.metadata.migrations import upgrade
from datakiln.metadata.script import ScriptMeta


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

        # Objects built before that are no longer in the repo
        self.stale = []

        # Retrieve files to ignore
        self.ignore = []
        if (path := self.directory / ".dataignore").exists():
            with path.open("r", encoding="utf-8") as file:
                self.ignore = file.read().splitlines()

    def _files(self, directory, pattern):
        """Retrieve files in a subdirectory, skipping the ignored ones.

        :param directory: Subdirectory of the metadata directory.
        :type directory: str

        :param pattern: Glob pattern of the files.
        :type pattern: str

        :return: Paths of the files, in no particular order.
        :rtype: Iterator[pathlib.Path]
        """
        for path in (self.directory / directory).glob(pattern):
            if str(path.relative_to(self.directory)) not in self.ignore:
                yield path

    @cached_property
    def tables(self):
        """Retrieve database tables, in the order of their files.

        :return: Table metadata.
        :rtype: list[datakiln.metadata.table.TableMeta]
        """
        data_files = DataFiles(self.directory / "data")

        tables = []
        for path in self._files("metadata", "*.json"):
            tables.append(TableMeta.from_json(path, data_files))

        return tables

    @cached_property
    def scripts(self):
        """Retrieve the derive scripts.

        :return: Script metadata.
        :rtype: list[datakiln.metadata.script.ScriptMeta]
        """
        # The dialect is set in the environment of the database
        dialect = self.load_env(self.directory, required=False)["dialect"]
        dialect = DIALECT_MAP.get(dialect, dialect)

        scripts = []
        for path in self._files("derive", "**/*.sql"):
            scripts.append(ScriptMeta.from_sql(path, self.directory, dialect))
        return scripts

    @cached_property
    def graph(self):
        """Dependency graph of the tables (metadata) and scripts (derive),
        which gives them in the order they depend on one another.

        :return: Dependency graph.
        :rtype: datakiln.metadata.graph.DatabaseGraph
        """
        return DatabaseGraph(self.tables, self.scripts)

    def grant(self, session, users=None):
        """Grants read on the objects of the schemas of the tables and scripts,
        which excludes the stage and the metadata, to the database role, and
        the role to the users.

        :param session: Database Session.
        :type session: datakiln.metadata.database.Session

        :param users: Users to give the role to.
        :type users: list[str]
        """
        # The schemas of the repo that were built
        built = set(inspect(session.connection()).get_schema_names())
        for schema in sorted((self.graph.schemas & built) - {"public"}):
            session.execute(
                f"GRANT USAGE ON SCHEMA {schema} TO {self.name}")
            session.execute(
                f"GRANT SELECT ON ALL SEQUENCES IN SCHEMA {schema} "
                f"TO {self.name}")
            session.execute(
                f"GRANT SELECT ON ALL TABLES IN SCHEMA {schema} "
                f"TO {self.name}")

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
        :type session: datakiln.metadata.database.Session
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
        :type session: datakiln.metadata.database.Session

        :param ignore: List of files to ignore.
        :type ignore: str
        """
        for file_name in sorted(self._files(directory, "**/*.sql")):
            logger.info("Script '%s': Running.",
                        file_name.relative_to(self.directory))

            # Load file
            with open(file_name, "r", encoding="utf-8") as file:
                sql = file.read()

            # Execute query
            session.execute(sql, **kwargs)

    def load_stale(self, session):
        """Load what was built before and is no longer in the repo, because
        its metadata or script was removed or ignored.

        :param session: Database Session.
        :type session: datakiln.metadata.database.Session

        :raises ValueError: If what is in the repo requires any of them.
        """
        self.stale = []
        for built in session.execute(select(Object)).scalars():
            if built.name_ not in self.graph.nodes:
                # The pipeline commits, which would expire it
                session.expunge(built)
                self.stale.append(built)

        # They are dropped at the end, so nothing in the repo can use them
        for built in self.stale:
            name = built.name_

            # A call names a function without its parameters, and it is not
            # known which overload it uses
            function = name.partition("(")[0]
            for user, node in self.graph.nodes.items():
                if node.requires & {name, function}:
                    raise ValueError(
                        f"'{user}' requires '{name}', which was removed from "
                        "the repo."
                    )

    def forget_dependents(self, session, objects):
        """Deletes the records of the tables and scripts that need any of the
        objects. Rebuilding an object drops what is built on it, the foreign
        keys of the tables and the views of the scripts, so the next run has
        to build them again.

        :param session: Database Session.
        :type session: datakiln.metadata.database.Session

        :param objects: Tables or scripts.
        :type objects: list[datakiln.metadata.table.TableMeta]
        """
        for node in self.graph.affected(objects):
            record = node.record(session)
            if record:
                session.delete(record)

    def execute_derive(self, session, force=False, record=True, include=None,
                       **kwargs):
        """Executes the derive scripts, each one after the scripts that create
        the objects it uses. Only the scripts that changed, or that were not
        built since what they use was rebuilt, are executed.

        :param session: Database Session.
        :type session: datakiln.metadata.database.Session

        :param force: Execute all scripts, even if they are up to date.
        :type force: bool

        :param record: Whether to save the scripts executed to the metadata,
                       which is what tells later runs they are up to date.
        :type record: bool

        :param include: Scripts to execute, if not all of them.
        :type include: set[datakiln.metadata.script.ScriptMeta]
        """
        for script in self.graph.scripts:
            name = script.full_name
            if include is not None and script not in include:
                continue
            saved = script.record(session) if record else None

            if not force and saved and saved.checksum_ == script.checksum:
                logger.info("Script '%s': Already up to date.", script.path)
                continue

            logger.info("Script '%s': Running.", script.path)
            session.execute(script.sql, **kwargs)

            if record:
                session.merge(Script(
                    name_=name,
                    checksum_=script.checksum,
                    drop_=script.drop
                ))
                self.forget_dependents(session, [script])
                session.commit()

    def prune(self, session, drop=False):
        """Drops what was built before and is no longer in the repo, because
        its metadata or script was removed or ignored, and the schemas that
        are left empty.

        :param session: Database Session.
        :type session: datakiln.metadata.database.Session

        :param drop: Whether to drop them. If not, they are only listed.
        :type drop: bool
        """
        verb = "Dropping." if drop else "Removed from the repository."
        schemas = set()

        for built in self.stale:
            logger.warning("%s '%s': %s", built.kind_.capitalize(),
                           "'.'".join(built.name_.split(".")), verb)
            if built.schema:
                schemas.add(built.schema)
            if not drop:
                continue

            for statement in built.drops:
                session.execute(statement)

            # What is under it in the metadata goes with it
            session.delete(built)

        if not drop:
            return

        # A schema is only dropped if nothing is left in it, even objects
        # that are not ours, so the database is the one to tell
        for schema in sorted(schemas - self.graph.schemas):
            for name in (schema, f"_{schema}"):
                try:
                    with session.begin_nested():
                        session.execute(f"DROP SCHEMA IF EXISTS {name}")
                except DBAPIError:
                    logger.info(
                        "Schema '%s': Not in the repository, but not empty.",
                        name)
        session.commit()

    def create_metadata(self, session):
        """
        :param session: Database Session.
        :type session: datakiln.metadata.database.Session
        """
        engine = session.get_bind()
        with engine.connect() as conn:
            schema = CreateSchema(Base.metadata.schema, if_not_exists=True)
            conn.execute(schema)
            upgrade(conn)
            conn.commit()

    @classmethod
    def load_env(cls, directory, required=True):
        """Load environment variables.

        :param directory: Path for metadata directory.
        :type directory: str

        :param required: Whether the directory must have a `.env` file. If
                         not, the environment variables and the defaults are
                         used when it does not.
        :type required: bool
        """
        db_params = {}

        # Load .env file
        dotenv_file = Path(directory) / ".env"
        if required or dotenv_file.exists():
            env_config = Config(RepositoryEnv(dotenv_file))
        else:
            env_config = Config(RepositoryEmpty())

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

    # Skip formatting if explicitly requested via execution options
    if getattr(context, "execution_options", {}).get("skip_formatting", False):
        return statement, parameters

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

    def __init__(self, **kwargs):
        r"""
        :param \**kwargs: SQLAlchemy session attributes.
        :type \**kwargs: dict

        :return: Database session.
        :rtype: datakiln.metadata.database.Session

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
            read=DIALECT_MAP.get(dialect, dialect)
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
        :rtype: datakiln.metadata.database.Session
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
