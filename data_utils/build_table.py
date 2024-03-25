"""
Creates table and loads data.
"""
import re
import json
import logging
from pathlib import Path
from getpass import getuser
from textwrap import dedent
from itertools import chain
from dataclasses import dataclass

from sqlalchemy.orm.session import Session
from sqlalchemy import create_engine, text
from decouple import Config, RepositoryEnv, config
from pandas._libs.parsers import STR_NA_VALUES  # pylint: disable=no-name-in-module # noqa: E501

from data_utils.build_json import get_parser
from data_utils.io import open_excel


logging.basicConfig(
    level=logging.INFO, datefmt="%Y-%m-%d %H:%M:%S",
    format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s")


class ColumnMeta():
    """Metadata for column."""

    def __init__(self, name, dtype, description=None, required=False,
                 rename=None):
        # pylint: disable=too-many-arguments
        """
        :param name: Name of the column.
        :type name: str

        :param dtype: Type of the column for each dialect.
        :type dtype: dict

        :param description: Description of the column.
        :type description: str

        :param required: If True, column can't be NULL.
        :type required: boolean

        :param rename: Name to be used in the database.
        :type rename: str

        :return: Column metadata.
        :rtype: ColumnMeta
        """
        self.name = name
        self.dtype = dtype
        self.rename = rename or name
        self.description = description
        self.required = required

    def __str__(self):
        return self.rename

    def __iter__(self):
        for item in self.__dict__.items():
            if item[0].startswith("_"):
                continue
            yield item

    def resolve_na(self, conn, schema, name, na_values):
        """Enforce column data type.

        :param conn: SQLAlchemy connection or session.
        :type conn: sqlalchemy.Engine

        :param schema: Table schema.
        :type schema: str

        :param name: Table name.
        :type name: str

        :param na_values: Values to consider as NULL.
        :type columns: set[str]

        :return: None
        """
        stmt = dedent("""
        UPDATE {schema}.{table}
        SET {name} = NULL
        WHERE {name} IN (
            '{na_values}'
        )""").format(
            schema=schema,
            table=name,
            name=self.rename,
            na_values="'\n  , '".join(na_values)
        )
        conn.execute(text(stmt))

    def enforce_type(self, conn, schema, name, dialect):
        """Resolve NULL values

        :param conn: SQLAlchemy connection or session.
        :type conn: sqlalchemy.Engine

        :param schema: Table schema.
        :type schema: str

        :param name: Table name.
        :type name: str

        :param dialect: Dialect of the Database.
        :type dialect: str

        :return: None
        """
        stmt = dedent(f"""
        ALTER TABLE {schema}.{name}
        ALTER COLUMN {self.rename} TYPE {self.dtype[dialect]}
        USING {self.rename}::{self.dtype[dialect]}
        """)
        conn.execute(text(stmt))

    def enforce_required(self, conn, schema, name):
        """Enforce required column values.

        :param conn: SQLAlchemy connection or session.
        :type conn: sqlalchemy.Engine

        :param schema: Table schema.
        :type schema: str

        :param name: Table name.
        :type name: str

        :return: None
        """
        stmt = dedent(f"""
        ALTER TABLE {schema}.{name}
        ALTER COLUMN {self.rename} SET NOT NULL
        """)
        conn.execute(text(stmt))

    def comment(self, conn, schema, name):
        """Add column description as a column comment.

        :param conn: SQLAlchemy connection or session.
        :type conn: sqlalchemy.Engine

        :param schema: Table schema.
        :type schema: str

        :param name: Table name.
        :type name: str

        :return: None
        """
        stmt = dedent(f"""
        COMMENT ON COLUMN {schema}.{name}.{self.rename} IS '{self.description}'
        """)
        conn.execute(text(stmt))


@dataclass
class ForeignKeyReference:
    """Metadata for table referenced in foreign key.

    :param schema: Schema of foreign table.
    :type schema: str

    :param table: Table referenced by foreign key.
    :type table: str

    :param columns: Columns referenced by foreign key.
    :type columns: str
    """
    schema: str
    table: str
    columns: list


class ForeignKeyMeta():
    """Metadata for foreign key."""

    def __init__(self, table, columns, reference):
        """
        :param table: Table referencing the foreign key.
        :type table: str

        :param columns: Columns referencing the foreign key.
        :type columns: str

        :param reference: Metadata for referenced table and columns.
        :type reference: dict

        :return: Foreign key metadata.
        :rtype: ForeignKeyMeta
        """
        self.table = table
        self.columns = columns
        self.reference = ForeignKeyReference(**reference)

    def create(self, conn):
        """Create statement.

        :param conn: SQLAlchemy connection or session.
        :type conn: sqlalchemy.Engine

        :return: None
        """
        stmt = dedent("""
        ALTER TABLE {schema}.{table}
        ADD FOREIGN KEY ({columns})
        REFERENCES {schema}.{table} ({reference})
        """).format(
            columns=", ".join(self.columns),
            schema=self.reference.schema,
            table=self.reference.table,
            reference=", ".join(self.reference.columns)
        )
        conn.execute(text(stmt))

    def conditions(self):
        """Generate comparisons to match each foreign key column.

        :return: Comparisons for each foreign key column.
        :rtype: list[str]
        """
        conditions = []

        # Generate one comparison statement for each foreign key column
        for column, o_column in zip(self.columns, self.reference.columns):
            conditions.append(
                f"  AND {self.table}.{column} = "
                f"{self.reference.table}.{o_column}::TEXT"
            )

        # Remove AND clause from first comparison
        conditions[0] = conditions[0][5:]

        return conditions

    def join(self):
        """Generate JOIN statement for the foreign key.

        :return: JOIN statement.
        :rtype: list[str]
        """
        join = [f"JOIN {self.reference.schema}.{self.reference.table}"]
        join.extend(self.conditions())

        # Add ON clause to first comparison
        join[1] = "  ON" + join[1]

        return join


class TableBase():
    """Base metadata for table."""
    # pylint: disable=too-few-public-methods
    _row_id = "meta_row_id"

    def __init__(self, name, schema, columns, na_values=None,
                 encoding="utf-8"):
        # pylint: disable=too-many-arguments
        """
        :param name: Name of the table.
        :type name: str

        :param schema: Name of the database schema.
        :type schema: str

        :param columns: Metadata for table columns.
        :type columns: list[dict]

        :param na_values: Values to consider as NULL.
        :type columns: set[str]

        :param encoding: encoding of the data file.
        :type encoding: str

        :return: Base table metadata.
        :rtype: TableBase
        """
        self.name = name
        self.schema = schema
        self.columns = []
        self.encoding = encoding
        self.na_values = set(na_values or []) | STR_NA_VALUES

        # Instantiate metadata object for all columns
        for column in columns:
            self.columns.append(ColumnMeta(**column))

    def create(self, conn):
        """Create statement.

        :param conn: SQLAlchemy connection or session.
        :type conn: sqlalchemy.Engine

        :return: None
        """
        # Create the schema if it does not exists
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {self.schema}"))

        # We replace the table if it already exists
        conn.execute(text(
            f"DROP TABLE IF EXISTS {self.schema}.{self.name} CASCADE"))

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
        header = next(buffer).replace("\n", "").split(",")

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
        conn.execute(text(f"""
        ALTER TABLE {self.schema}.{self.name} ADD COLUMN meta_log TEXT
        """))


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
        self.foreign_keys = []

        # Instantiate metadata object for all foreign keys
        for fk in constraints.get("foreign_keys", []):
            fk_meta = ForeignKeyMeta(table=self.name, **fk)
            self.foreign_keys.append(fk_meta)

    @property
    def stage_meta(self):
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

        # Create stage as well
        self.stage_meta.create(conn)

    def load(self, conn):
        """Load data from stage.

        :param conn: SQLAlchemy connection or session.
        :type conn: sqlalchemy.Engine

        :return: None
        """
        # Generate JOIN statements for foreign keys
        join_fks = map(ForeignKeyMeta.join, self.foreign_keys)

        # Intialize variables
        columns = []
        where_reqs = ["WHERE meta_log IS NULL"]  # Filters can be set in transform stage by filling this column # noqa: E501

        for column in self.columns:
            # Append to list of columns to be retrieved
            columns.append(str(column))

            # Add required columns to where statement filtering not null values
            if not column.required:
                continue
            where_reqs.append(f"{self.name}.{column} IS NOT NULL")

        # Move rows from staging to final schema
        logging.info("Table '%s'.'%s': Loading.", self.schema, self.name)

        stmt = dedent("""
        WITH not_null AS (
          SELECT
              {columns}
            , {row_id}
          FROM stg_{schema}.{name}
          {where_reqs}
        )
        , moving AS (
          SELECT
              {name}.{columns_aliased}
            , {name}.{row_id}
          FROM not_null {name}
          {join_fks}
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
        , required AS (
          UPDATE stg_{schema}.{name}
          SET meta_log = 'Required'
          WHERE meta_log IS NULL
          AND meta_row_id NOT IN (SELECT meta_row_id FROM not_null)
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
            columns_aliased=f"\n    , {self.name}.".join(columns),
            columns="\n    , ".join(columns),
            where_reqs="\n     AND ".join(where_reqs),
            join_fks="\n  ".join(chain(*join_fks))
        )
        conn.execute(text(stmt))

        # The remaining rows that were not moved from staging
        # violated a foreign key constraint
        stmt = dedent(f"""
        UPDATE stg_{self.schema}.{self.name}
        SET meta_log = 'Constraint violation (foreign key)'
        WHERE meta_log IS NULL
        """)
        conn.execute(text(stmt))

        # Get database dialect from connection
        dialect = conn.bind.dialect.name

        # Enforce foreign keys
        for fk in self.foreign_keys:
            fk.create(conn)

        # Enforce constraints
        for column in self.columns:
            # Resolve NULL values
            column.resolve_na(
                conn, schema=self.schema, name=self.name,
                na_values=self.na_values)

            # Enforce data types
            column.enforce_type(
                conn, name=self.name, schema=self.schema, dialect=dialect)

            # Add comment to columns
            if column.description is not None:
                column.comment(
                    conn, schema=self.schema, name=self.name)

            # Enforce required
            if column.required:
                column.enforce_required(
                    conn, schema=self.schema, name=self.name)

        # Enforce primary key
        stmt = dedent("""
        ALTER TABLE {schema}.{name} ADD PRIMARY KEY ({pk})
        """).format(
            name=self.name, schema=self.schema,
            pk=', '.join(self.primary_key)
        )
        conn.execute(text(stmt))

        # Warn of any remaining staged records
        count = conn.execute(text(f"""
        SELECT COUNT(*) FROM stg_{self.schema}.{self.name}
        """)).first()[0]
        if count > 0:
            logging.warning(
                "Table '%s'.'%s': %d rows remain staged.",
                self.schema, self.name, count
            )


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


def stage(meta_path, conn):
    """Load metadata and stage data.

    :param meta_path: Path to the metadata
    :type meta_path: str

    :param conn: SQLAlchemy connection or session.
    :type conn: sqlalchemy.Engine

    :return: None
    """
    # Load metadata
    with open(meta_path, encoding="utf-8") as file:
        meta_json = json.load(file)

    # Create table
    table_meta = TableMeta(**meta_json)
    table_meta.create(conn)

    # Stage data
    meta_name = re.sub(r"^\d\d-", "", meta_path.stem)
    data_path = (meta_path.parents[1] / "data").glob(f"{meta_name}.*")
    table_meta.stage_meta.load(conn, next(data_path))

    return table_meta


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

    # Load to final schema and commit changes
    table_meta = stage(meta_path, conn)
    table_meta.load(conn)
    conn.commit()

    # Close connection
    conn.close()


if __name__ == "__main__":
    main()
