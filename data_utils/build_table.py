"""
Creates table and loads data.
"""
import csv
import json
from pathlib import Path
from getpass import getuser
from textwrap import dedent
from functools import partial

from sqlalchemy.orm.session import Session
from sqlalchemy import create_engine, text
from decouple import Config, RepositoryEnv, config

from data_utils.build_json import get_parser


class ColumnMeta():
    """Metadata for column."""
    def __init__(self, name, dtype, rename=None, is_stage=False):
        # pylint: disable=too-many-arguments
        """
        :param name: Name of the column.
        :type name: str

        :param dtype: Type of the column for each dialect.
        :type dtype: dict

        :param rename: Name to be used in the database.
        :type rename: str

        :param stage: Is it in a stage table?
        :type stage: bool

        :return: Column metadata.
        :rtype: data_utils.build_table.ColumnMeta
        """
        self.name = name
        self.dtype = dtype
        self.rename = rename or name

        # Do not inforce types in stage
        if is_stage is True:
            self.dtype = dict.fromkeys(dtype.keys(), "TEXT")

    def __str__(self):
        return self.rename

    def __iter__(self):
        for item in self.__dict__.items():
            yield item

    def create(self, dialect):
        """Column SQL for create statement.

        :param dialect: Dialect of the database.
        :type dialect: str

        :return: Column SQL for create statement.
        :rtype: str
        """
        return f"{self.rename} {self.dtype[dialect]}"


class TableMeta():
    """Metadata for table."""
    ROW_ID = "meta_row_id"

    def __init__(self, name, schema, columns,
                 constraints=None, is_stage=False, encoding="utf-8"):
        # pylint: disable=too-many-arguments
        """
        :param name: Name of the table.
        :type name: str

        :param columns: Metadata for table columns.
        :type columns: list[dict]

        :param constraints: Metadata for table constraints.
        :type constraints: dict

        :param dialect: Dialect of the database.
        :type dialect: str

        :param stage: Is it a stage table?
        :type stage: bool

        :param encoding: encoding of the data file.
        :type stage: str

        :return: Table metadata.
        :rtype: data_utils.build_table.TableMeta
        """
        self.name = name
        self.schema = schema
        self.columns = []
        self.encoding = encoding
        self._IS_STAGE = is_stage

        # Instantiate metadata object for all columns
        for column in columns:
            self.columns.append(ColumnMeta(is_stage=is_stage, **column))

        # We do not enforce constraints in stage
        if is_stage is True:
            self.schema = f"stg_{self.schema}"
            return

        self.primary_key = constraints["primary_key"]

    @property
    def _stage_meta(self):
        """Metadata for stage table.

        :return: Table metadata.
        :rtype: data_utils.build_table.TableMeta

        :return: None
        """
        if self._IS_STAGE is True:
            raise TypeError("Cannot stage recursively.")

        stage_meta = TableMeta(
            name=self.name, schema=self.schema,
            encoding=self.encoding, is_stage=True,
            columns=list(map(dict, self.columns)))

        return stage_meta

    def create(self, conn):
        """Create statement.

        :param conn: SQLAlchemy connection or session.
        :type conn: sqlalchemy.Engine

        :return: None
        """
        # We replace the table if it already exists
        conn.execute(text(f"DROP TABLE IF EXISTS {self.schema}.{self.name}"))

        # Get dialect from connection
        dialect = conn.bind.dialect.name
        create = partial(ColumnMeta.create, dialect=dialect)

        # Create table
        stmt = dedent("""
        CREATE TABLE {schema}.{name} (
            {columns}
          , {row_id} SERIAL
        )
        """).format(
            name=self.name, schema=self.schema, row_id=self.ROW_ID,
            columns='\n  , '.join(map(create, self.columns)),
        )
        conn.execute(text(stmt))

        # We do not enforce constraints in stage
        if self._IS_STAGE is True:
            return

        # Add constraints
        stmt = dedent("""
        ALTER TABLE {schema}.{name} ADD PRIMARY KEY ({pk})
        """).format(
            name=self.name, schema=self.schema,
            pk=', '.join(self.primary_key)
        )
        conn.execute(text(stmt))

        # Create stage as well
        self._stage_meta.create(conn)

    def _stage(self, conn, data_path):
        """Stage data from a file.

        :param conn: SQLAlchemy connection or session.
        :type conn: sqlalchemy.Engine

        :param data_path: Path to file.
        :type data_path: str

        :return: None
        """
        if self._IS_STAGE is False:
            raise TypeError("Cannot stage into final schema.")

        # Get header from file
        with open(data_path, "r", encoding=self.encoding) as file:
            reader = csv.reader(file)
            header = next(reader)

        # To load the data, we need to order
        # the column list according to the file
        columns = sorted(self.columns, key=lambda c: header.index(c.name))

        # Generate statement
        stmt = dedent("""
        COPY {schema}.{name} (
            {columns}
        )
        FROM STDIN
        WITH (FORMAT CSV, HEADER)
        """).format(
            name=self.name, schema=self.schema,
            columns='\n  , '.join(map(str, columns))
        )

        # Execute and copy from file
        cursor = conn.connection().connection.cursor()
        with open(data_path, "r", encoding=self.encoding) as file:
            cursor.copy_expert(stmt, file)

    def load(self, conn, data_path):
        """Load data from stage.

        :param conn: SQLAlchemy connection or session.
        :type conn: sqlalchemy.Engine

        :param data_path: Path to file.
        :type data_path: str

        :return: None
        """
        # Stage data
        self._stage_meta._stage(conn, data_path)


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
        f"{dialect}://{user}:@{host}:{port}/{database}")
    con = engine.connect()
    return Session(bind=con)


def main(args=None):
    """Creates table and loads data."""
    # Get arguments
    parser = get_parser()
    args = parser.parse_args(args)

    # Get absolute of specified path
    meta_path = Path(args.directory).absolute()

    # Connect to database
    db_params = load_env(meta_path.parents[1])
    conn = connect(**db_params)

    # Load metadata
    with open(meta_path, encoding="utf-8") as file:
        meta_json = json.load(file)

    # Load data
    table_meta = TableMeta(**meta_json)
    data_path = meta_path.parents[1] / "data" / (meta_path.stem + ".csv")
    table_meta.create(conn)
    table_meta.load(conn, data_path)

    # Commit changes
    conn.commit()


if __name__ == "__main__":
    main()
