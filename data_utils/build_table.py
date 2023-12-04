"""
Creates table and loads data.
"""
import json
import logging
from pathlib import Path
from getpass import getuser
from textwrap import dedent

from sqlalchemy.orm.session import Session
from sqlalchemy import create_engine, text
from decouple import Config, RepositoryEnv, config

from data_utils.build_json import get_parser
from data_utils.io import open_excel


logging.basicConfig(
    level=logging.INFO, datefmt="%Y-%m-%d %H:%M:%S",
    format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s")


class ColumnMeta():
    """Metadata for column."""

    def __init__(self, name, dtype, rename=None):
        """
        :param name: Name of the column.
        :type name: str

        :param dtype: Type of the column for each dialect.
        :type dtype: dict

        :param rename: Name to be used in the database.
        :type rename: str

        :return: Column metadata.
        :rtype: ColumnMeta
        """
        self.name = name
        self.dtype = dtype
        self.rename = rename or name

    def __str__(self):
        return self.rename

    def __iter__(self):
        for item in self.__dict__.items():
            if item[0].startswith("_"):
                continue
            yield item


class TableBase():
    """Base metadata for table."""
    # pylint: disable=too-few-public-methods
    _row_id = "meta_row_id"

    def __init__(self, name, schema, columns, encoding="utf-8"):
        """
        :param name: Name of the table.
        :type name: str

        :param schema: Name of the database schema.
        :type schema: str

        :param columns: Metadata for table columns.
        :type columns: list[dict]

        :param encoding: encoding of the data file.
        :type encoding: str

        :return: Base table metadata.
        :rtype: TableBase
        """
        self.name = name
        self.schema = schema
        self.columns = []
        self.encoding = encoding

        # Instantiate metadata object for all columns
        for column in columns:
            self.columns.append(ColumnMeta(**column))

    def create(self, conn):
        """Create statement.

        :param conn: SQLAlchemy connection or session.
        :type conn: sqlalchemy.Engine

        :return: None
        """
        # We replace the table if it already exists
        conn.execute(text(f"DROP TABLE IF EXISTS {self.schema}.{self.name}"))

        # Creates table. All columns initially have TEXT type.
        # Types are enforced after loading the data one column
        # at a time so we can identify the specific column
        # in the data that might be violating its type specification.
        # The copy command would otherwise issue unhelpful log message.
        stmt = dedent("""
        CREATE TABLE {schema}.{name} (
            {columns} TEXT
          , {row_id} SERIAL
        )
        """).format(
            name=self.name, schema=self.schema, row_id=self._row_id,
            columns=' TEXT\n  , '.join(map(str, self.columns)),
        )
        conn.execute(text(stmt))


class StageMeta(TableBase):
    """Metadata for table."""
    _readers = {
        ".csv": open,
        ".xlsx": open_excel
    }

    def __init__(self, **kwargs):
        r"""
        :param \**kwargs: TableBase attributes.
        :type \**kwargs: dict

        :return: Table metadata.
        :rtype: TableMeta
        """
        super().__init__(**kwargs)

        self.schema = f"stg_{self.schema}"

        # Do not inforce types in stage
        for column in self.columns:
            column.dtype = dict.fromkeys(column.dtype.keys(), "TEXT")

    def load(self, conn, data_path):
        """Stage data from a file.

        :param conn: SQLAlchemy connection or session.
        :type conn: sqlalchemy.Engine

        :param data_path: Path to file.
        :type data_path: str

        :return: None
        """
        logging.info("Table '%s'.'%s': Staging.", self.schema[4:], self.name)

        # Get header from file
        reader = self._readers[data_path.suffix]
        buffer = reader(data_path, encoding=self.encoding)
        header = next(buffer)

        # To load the data, we need to order
        # the column list according to the file
        columns = sorted(self.columns, key=lambda c: header.index(c.name))

        # Generate statement
        stmt = dedent("""
        COPY {schema}.{name} (
            {columns}
        )
        FROM STDIN
        WITH (FORMAT CSV)
        """).format(
            name=self.name, schema=self.schema,
            columns='\n  , '.join(map(str, columns))
        )

        # Execute and copy from file
        cursor = conn.connection().connection.cursor()
        cursor.copy_expert(stmt, buffer)
        buffer.close()

        # Add column to log staging errors
        conn.execute(text(
            f"ALTER TABLE {self.schema}.{self.name} ADD COLUMN meta_log TEXT"))


class TableMeta(TableBase):
    """Metadata for table."""

    def __init__(self, constraints=None, **kwargs):
        r"""
        :param constraints: Metadata for table constraints.
        :type constraints: dict

        :param \**kwargs: TableBase attributes.
        :type \**kwargs: dict

        :return: Table metadata.
        :rtype: .TableMeta
        """
        super().__init__(**kwargs)

        self.primary_key = constraints["primary_key"]

    @property
    def _stage_meta(self):
        """Metadata for stage table.

        :return: Table metadata.
        :rtype: TableMeta

        :return: None
        """
        stage_meta = StageMeta(
            name=self.name, schema=self.schema,
            encoding=self.encoding,
            columns=list(map(dict, self.columns)))

        return stage_meta

    def create(self, conn):
        """Create statement.

        :param conn: SQLAlchemy connection or session.
        :type conn: sqlalchemy.Engine

        :return: None
        """
        super().create(conn)

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

    def load(self, conn, data_path):
        """Load data from stage.

        :param conn: SQLAlchemy connection or session.
        :type conn: sqlalchemy.Engine

        :param data_path: Path to file.
        :type data_path: str

        :return: None
        """
        # Stage data
        self._stage_meta.load(conn, data_path)

        logging.info("Table '%s'.'%s': Loading.", self.schema, self.name)
        stmt = dedent("""
        WITH moving AS (
          SELECT
              {columns}
            , {row_id}
          FROM stg_{schema}.{name}
        )
        , moved AS (
          INSERT INTO {schema}.{name} (
              {columns}
            , {row_id}
          )
          SELECT *
          FROM moving
          ON CONFLICT DO NOTHING
          RETURNING {row_id}
        )
        , delete AS (
          DELETE FROM stg_{schema}.{name}
          USING moved
          WHERE {name}.meta_row_id = moved.meta_row_id
          RETURNING {name}.{row_id}
        )
        , duplicate AS (
          UPDATE stg_{schema}.{name}
          SET meta_log = 'Duplicate'
          FROM moving
          WHERE {name}.meta_row_id = moving.meta_row_id
        )
        SELECT * FROM delete
        """).format(
            name=self.name, schema=self.schema, row_id=self._row_id,
            columns='\n    , '.join(map(str, self.columns))
        )
        conn.execute(text(stmt))

        # Get database dialect from connection
        dialect = conn.bind.dialect.name

        # Enforce data types
        for column in self.columns:
            stmt = dedent(f"""
            ALTER TABLE {self.schema}.{self.name}
            ALTER COLUMN {column} TYPE {column.dtype[dialect]}
            USING {column}::{column.dtype[dialect]}
            """)
            conn.execute(text(stmt))

        # Warn of any remaining staged records
        count = conn.execute(text(
            f"SELECT COUNT(*) FROM stg_{self.schema}.{self.name}"
        )).first()[0]
        if count > 0:
            logging.warning("Table '%s'.'%s': %d rows remain staged.",
                            self.schema, self.name, count)


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

    # Create table
    table_meta = TableMeta(**meta_json)
    table_meta.create(conn)

    # Load data
    data_path = (meta_path.parents[1] / "data").glob(f"{meta_path.stem}.*")
    table_meta.load(conn, next(data_path))

    # Commit changes
    conn.commit()


if __name__ == "__main__":
    main()
