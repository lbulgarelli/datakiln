# pylint: disable=too-many-lines
"""
Creates table and loads data.
"""
import re
import csv
import gzip
import json
import logging
from pathlib import Path
from getpass import getuser
from textwrap import dedent
from itertools import chain
from functools import partial
from dataclasses import dataclass
from collections import defaultdict

from sqlalchemy.orm.session import Session
from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError
from decouple import Config, RepositoryEnv, config
from pandas._libs.parsers import STR_NA_VALUES  # pylint: disable=no-name-in-module # noqa: E501
from sqlalchemy_utils import create_database, database_exists

from data_utils.build_json import get_parser
from data_utils.io import open_excel


logging.basicConfig(
    level=logging.INFO, datefmt="%Y-%m-%d %H:%M:%S",
    format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s")


class ColumnMeta():
    """Metadata for column."""

    def __init__(self, name, dtype, description=None, required=False,
                 rename=None, transform=False):
        # pylint: disable=too-many-arguments, too-many-positional-arguments
        """
        :param name: Name of the column.
        :type name: str

        :param dtype: Type of the column for each dialect.
        :type dtype: dict

        :param description: Description of the column.
        :type description: str

        :param required: If True, column can't be NULL.
        :type required: bool

        :param rename: Name to be used in the database.
        :type rename: str

        :param rename: Specifies if the column is derived/created in the
                       transform step.
                       (This means the column is not present and will not be
                       retrieved from the source).
        :type rename: bool

        :return: Column metadata.
        :rtype: ColumnMeta
        """
        self.name = name
        self.dtype = dtype
        self.rename = rename or name
        self.description = description
        self.required = required
        self.transform = transform

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
        UPDATE stg_{schema}.{table}
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

    def enforce_type(self, conn, schema, name):
        """Resolve NULL values

        :param conn: SQLAlchemy connection or session.
        :type conn: sqlalchemy.Engine

        :param schema: Table schema.
        :type schema: str

        :param name: Table name.
        :type name: str

        :return: None
        """
        # Get database dialect from connection
        dialect = conn.bind.dialect.name

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
        if not self.required:
            return

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
        if self.description is None:
            return

        stmt = dedent(f"""
        COMMENT ON COLUMN {schema}.{name}.{self.rename} IS '{self.description}'
        """)
        conn.execute(text(stmt))

    def add(self, conn, schema, name):
        """Add column to table.

        :param conn: SQLAlchemy connection or session.
        :type conn: sqlalchemy.Engine

        :param schema: Table schema.
        :type schema: str

        :param name: Table name.
        :type name: str

        :return: None
        """
        stmt = dedent("""
        ALTER TABLE {schema}.{name} ADD COLUMN {column} TEXT;
        """).format(
            schema=schema,
            name=name,
            column=self.name
        )
        conn.execute(text(stmt))


class UnpivotColumnMeta(ColumnMeta):
    """Metadata for columns that are unpivoting targets."""
    def __init__(self, unpivot, regex=None, order=1000, **kwargs):
        r"""
        :param unpivot: Metadata of this column's unpivot.
        :type unpivot: UnpivotMeta

        :param regex: Pattern for target column.
        :type regex: str

        :param order: Position of the column in the target table.
        :type order: int

        :param \**kwargs: `ColumnMeta` attributes.
        :type \**kwargs: dict

        :return: Unpivot column metadata.
        :rtype: UnpivotColumnMeta
        """
        self.unpivot = unpivot
        self.regex = regex
        self.order = order

        super().__init__(**kwargs)

    def value(self, column):
        """Value for the column to be unpivoted.

        :param column: Name of the column to be unpivoted.
        :type column: dict

        :return: Unpivot value.
        :rtype: str
        """
        match = re.search(self.regex, column)
        if self.regex and not match:
            return "NULL"
        return f"'{''.join(match.groups())}'"


class UnpivotValueMeta(ColumnMeta):
    """Metadata for columns that will store unpivoted values."""
    def __init__(self, unpivot, columns, **kwargs):
        r"""
        :param unpivot: Metadata of this column's unpivot.
        :type unpivot: UnpivotMeta

        :param columns: Columns storing the values.
        :type columns: list[str]

        :return: Column metadata.
        :rtype: ColumnMeta
        """
        self.unpivot = unpivot
        super().__init__(**kwargs)

        self.columns = []
        for column in columns:
            column_meta = ColumnMeta(
                dtype={
                    "postgresql": "TEXT"
                },
                name=column
            )
            self.columns.append(column_meta)


class UnpivotMeta():
    """Metadata for unpivoting columns."""
    def __init__(self, alias, target_columns, target_values, **kwargs):
        r"""
        :param alias: Alias for the LATERAL join.
        :type alias: str

        :param target_columns: Metadata for target columns.
        :type target_columns: list[str]

        :param target_values: Metadata for target value column.
        :type target_values: dict

        :param \**kwargs: `UnpivotColumnMeta` attributes.
        :type \**kwargs: dict

        :return: Unpivot metadata.
        :rtype: UnpivotMeta
        """
        self.alias = alias

        # Target columns
        self.target_columns = []
        for target in target_columns:
            target_meta = UnpivotColumnMeta(self, **target, **kwargs)
            self.target_columns.append(target_meta)

        # Target value
        self.target_values = []
        for value in target_values:
            value_meta = UnpivotValueMeta(self, **value, **kwargs)
            self.target_values.append(value_meta)

    def columns(self, unpivot=True):
        """Unpivot columns.

        :return: Pivot columns.
        :rtype: list[ColumnMeta]
        """
        # List target columns
        if unpivot:
            return self.target_columns + self.target_values

        # List original columns
        columns = []
        for value in self.target_values:
            columns.extend(value.columns)
        return columns

    def values(self):
        """SQL with VALUES to use in LATERAL JOIN.

        :return: Values.
        :rtype: list[str]
        """
        # Generate expressions
        exp_dict = defaultdict(list)
        for idx, value in enumerate(self.target_values):
            value_columns = []
            # Retrieve name for columns being pivoted
            for pivot in value.columns:
                value_columns.append(pivot.name)

            values = []
            # Extract values from columns
            for target in self.target_columns:
                values.append(list(map(target.value, value_columns)))

            # Add columns to list for each value found
            expression = dict(zip(zip(*values), value_columns))
            keys = exp_dict.keys() | expression.keys()
            for key in keys:
                value = expression.get(key, 'NULL')
                # Prepend NULL if value not seen before
                if idx and key not in exp_dict:
                    count = len(list(exp_dict.items())[0]) + 1
                    exp_dict[key].extend(['NULL'] * count)
                exp_dict[key].append(value)

        # Parse unpivoting columns to string
        expressions = []
        for vals, v_cols in exp_dict.items():
            expression = list(vals) + v_cols
            expression = f"({", ".join(expression)})"
            expressions.append(expression)
        return expressions

    def lateral(self):
        """Returns the LATERAL JOIN to unpivot a table.

        :return: LATERAL JOIN expression.
        :rtype: list[str]
        """
        return dedent("""
        LATERAL ( VALUES
            {values}
        ) AS {alias} (
            {columns}
        )
        """).format(
            values="\n  , ".join(self.values()),
            alias=self.alias,
            columns="\n  , ".join(map(str, self.columns()))
        )[1:]


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

    def _conditions(self):
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
        join = [f"LEFT JOIN {self.reference.schema}.{self.reference.table}"]
        join.extend(self._conditions())

        # Add ON clause to first comparison
        join[1] = "  ON" + join[1]

        return join

    def is_reference_null(self):
        """Check if foreign column is null.

        :return: Comparisons for each foreign key column.
        :rtype: list[str]
        """
        conditions = []

        # Generate one comparison statement for each foreign key column
        for column in self.reference.columns:
            conditions.append(f"AND {self.reference.table}.{column} IS NULL")

        return conditions


class TableBase():
    """Base metadata for table."""
    # pylint: disable=too-few-public-methods
    _row_id = "meta_row_id"

    def __init__(self, name, schema, columns, na_values=None,
                 encoding="utf-8"):
        # pylint: disable=too-many-arguments, too-many-positional-arguments
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

        # Create index to speed up validations
        conn.execute(text(f"""
        CREATE INDEX ON {self.schema}.{self.name} (meta_row_id)
        """))


class StageMeta(TableBase):
    """Metadata for table."""
    _readers = {
        ".csv": open,
        ".csv.gz": partial(gzip.open, mode='rt'),
        ".xlsx": open_excel
    }

    def __init__(self, **kwargs):
        r"""
        :param \**kwargs: TableBase attributes.
        :type \**kwargs: dict

        :return: Stage metadata.
        :rtype: StageBase
        """
        super().__init__(**kwargs)

        self.schema = f"stg_{self.schema}"

        # Do not enforce types in stage
        for column in self.columns:
            column.dtype = dict.fromkeys(column.dtype.keys(), "TEXT")

    def create(self, conn):
        """Create statement.

        :param conn: SQLAlchemy connection or session.
        :type conn: sqlalchemy.Engine

        :return: None
        """
        super().create(conn)

        # Add column to log staging errors
        conn.execute(text(f"""
        ALTER TABLE {self.schema}.{self.name} ADD COLUMN meta_log TEXT
        """))

        # Create index to speed up validations
        conn.execute(text(f"""
        CREATE INDEX ON {self.schema}.{self.name} (meta_log)
        """))

    def load(self, conn, data_path):
        """Stage data from a file.

        :param conn: SQLAlchemy connection or session.
        :type conn: sqlalchemy.Engine

        :param data_path: Path to file.
        :type data_path: str

        :return: None
        """
        logging.info("Table '%s'.'%s': Staging file '%s'.", self.schema[4:],
                     self.name, data_path.name)

        # Get header from file
        suffix = "".join(data_path.suffixes)
        reader = self._readers[suffix]
        buffer = reader(data_path, encoding=self.encoding)
        header = next(csv.reader(buffer))

        # To load the data, we need to order
        # the column list according to the file
        columns = filter(lambda c: not c.transform, self.columns)
        columns = sorted(columns, key=lambda c: header.index(c.name))

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


class TableMeta(TableBase):
    """Metadata for table."""

    def __init__(self, constraints=None, indexes=None, unpivot=None, **kwargs):
        r"""
        :param constraints: Metadata for table constraints.
        :type constraints: dict

        :param unpivot: Metadata for unpivoting table columns.
        :type unpivot: list[dict]

        :param \**kwargs: TableBase attributes.
        :type \**kwargs: dict

        :return: Table metadata.
        :rtype: TableMeta
        """
        super().__init__(**kwargs)

        self.primary_key = constraints["primary_key"]
        self.indexes = indexes or []
        self.foreign_keys = []
        self.unpivot_meta = []

        # Instantiate metadata object for all foreign keys
        for fk in constraints.get("foreign_keys", []):
            fk_meta = ForeignKeyMeta(table=self.name, **fk)
            self.foreign_keys.append(fk_meta)

        # Unpivot
        for idx, upv in enumerate(unpivot or []):
            upv["alias"] = f"upv_{idx}"
            unpivot_meta = UnpivotMeta(**upv)
            self.unpivot_meta.append(unpivot_meta)
            for t_vals in unpivot_meta.target_values:
                self.columns.extend(t_vals.columns)

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

    def unpivot(self, conn):
        """Unpivot a table.

        :param con: SQLAlchemy connection/session.
        :type con: sqlalchemy.Engine

        :return: None
        """
        if not self.unpivot_meta:
            return

        drop = []
        columns = []
        aliased = []
        laterals = []
        for upv in self.unpivot_meta:
            drop.extend(map(str, upv.columns(unpivot=False)))
            for column in upv.columns():
                columns.append(column.name)
                aliased.append(f"{upv.alias}.{column.name}")
                column.add(conn, self.schema, self.name)
            laterals.append(upv.lateral())

        # Include columns that are not affected
        for column in self.columns:
            # Skip columns that we are dropping
            if column.name in drop:
                continue
            columns.append(column.name)
            aliased.append(f"upv.{column.name}")

        # Insert unpivoted data
        stmt = dedent("""
        WITH to_unpivot AS
        (
          DELETE FROM {schema}.{name}
          RETURNING *
        )
        INSERT INTO {schema}.{name} (
            {columns}
          , meta_row_id
        )
        SELECT
            {aliased}
          , upv.meta_row_id
        FROM to_unpivot AS upv
        CROSS JOIN {laterals}
        """).format(
            schema=self.schema,
            name=self.name,
            columns="\n  , ".join(columns),
            aliased="\n  , ".join(aliased),
            laterals="".join(laterals)
        )
        conn.execute(text(stmt))

        # Drop unpivoted columns
        stmt = dedent("""
        ALTER TABLE {schema}.{name}
            DROP COLUMN {columns}
        ;
        """).format(
            schema=self.schema,
            name=self.name,
            columns="\n,  DROP COLUMN ".join(drop)
        )
        conn.execute(text(stmt))

        # Enforce primary key
        self.enforce_pk(conn)

        # Enforce column properties
        for upv in self.unpivot_meta:
            for column in upv.columns():
                column.enforce_type(conn, self.schema, self.name)
                column.enforce_required(conn, self.schema, self.name)

    def enforce_constraints(self, conn):
        """Enforce table constraints.

        :param conn: SQLAlchemy connection or session.
        :type conn: sqlalchemy.Engine

        :return: None
        """
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
                conn, name=self.name, schema=self.schema)

            # Add comment to columns
            column.comment(
                conn, schema=self.schema, name=self.name)

            # Enforce required
            column.enforce_required(
                conn, schema=self.schema, name=self.name)

    def create_indexes(self, conn):
        """Create table indexes.

        :param conn: SQLAlchemy connection or session.
        :type conn: sqlalchemy.Engine

        :return: None
        """
        for index in self.indexes:
            logging.info("Table '%s'.'%s': Creating index %s.", self.schema,
                         self.name, tuple(index))
            stmt = dedent("""
            CREATE INDEX ON {schema}.{name} ({columns})
            """).format(
                schema=self.schema,
                name=self.name,
                columns=", ".join(index)
            )
            conn.execute(text(stmt))

    def enforce_pk(self, conn):
        """Enforce primary key.

        :param conn: SQLAlchemy connection or session.
        :type conn: sqlalchemy.Engine

        :return: None
        """
        stmt = dedent("""
        ALTER TABLE {schema}.{name} ADD PRIMARY KEY ({pk})
        """).format(
            name=self.name, schema=self.schema,
            pk=', '.join(self.primary_key)
        )
        conn.execute(text(stmt))

    def load(self, conn):
        """Load data from stage.

        :param conn: SQLAlchemy connection or session.
        :type conn: sqlalchemy.Engine

        :return: None
        """
        # Generate JOIN statements for foreign keys
        join_fks = list(map(ForeignKeyMeta.join, self.foreign_keys))
        null_fks = map(ForeignKeyMeta.is_reference_null, self.foreign_keys)

        # Get database dialect from connection
        dialect = conn.bind.dialect.name

        typed = []
        columns = []
        where_req = ["0 = 1"]
        for column in self.columns:
            # Append to list of columns to be retrieved
            columns.append(str(column))
            typed.append(f"{column.rename}::{column.dtype[dialect]}")

            # Add required columns to where statement filtering not null values
            if not column.required:
                continue
            where_req.append(f"{self.name}.{column} IS NULL")

        # Move rows from staging to final schema
        logging.info("Table '%s'.'%s': Loading.", self.schema, self.name)

        # Skipped if we are pivoting the table later
        if not self.unpivot_meta:
            self.enforce_pk(conn)

        # Flag rows missing required columns
        stmt = dedent("""
        UPDATE stg_{schema}.{name}
        SET meta_log = 'Required'
        WHERE 1 = 1
        AND meta_log IS NULL
        AND (
             {where_reqs}
        )
        """).format(
            name=self.name, schema=self.schema,
            where_reqs="\n  OR ".join(where_req)
        )
        conn.execute(text(stmt))

        # Flag rows violating a foreign key constraint
        stmt = dedent("""
        WITH missing_fk AS (
          SELECT {name}.{row_id}
          FROM stg_{schema}.{name}
          {join_fks}
          WHERE {n_fks} > 0
          AND {name}.meta_log IS NULL
          {null_fks}
        )
        UPDATE stg_{schema}.{name}
        SET meta_log = 'Constraint violation (foreign key)'
        FROM missing_fk
        WHERE {name}.{row_id} = missing_fk.{row_id}
        """).format(
            name=self.name, schema=self.schema, row_id=self._row_id,
            join_fks="\n  ".join(chain(*join_fks)),
            n_fks=len(join_fks),
            null_fks="\n  ".join(chain(*null_fks)),
        )
        conn.execute(text(stmt))

        # Move rows to final schema
        stmt = dedent("""
        INSERT INTO {schema}.{name} (
            {columns}
          , {row_id}
        )
        SELECT
            {typed}
          , {row_id}
        FROM stg_{schema}.{name}
        WHERE 1 = 1
        AND meta_log IS NULL
        ON CONFLICT DO NOTHING
        """).format(
            name=self.name, schema=self.schema, row_id=self._row_id,
            columns="\n  , ".join(columns),
            typed="\n  , ".join(typed),
        )
        conn.execute(text(stmt))

        # Deleted moved rows
        stmt = dedent("""
        DELETE FROM stg_{schema}.{name}
        USING {schema}.{name} moved
        WHERE {name}.meta_row_id = moved.meta_row_id
        """).format(
            name=self.name, schema=self.schema
        )
        conn.execute(text(stmt))

        # Rows that still remain in staging but did not
        # get flagged before this point are PK duplicates
        stmt = dedent("""
        UPDATE stg_{schema}.{name}
        SET meta_log = 'Duplicate'
        WHERE 1 = 1
        AND meta_log IS NULL
        """).format(
            name=self.name, schema=self.schema
        )
        conn.execute(text(stmt))

        # Warn of any remaining staged records
        count = conn.execute(text(f"""
        SELECT COUNT(meta_row_id) FROM stg_{self.schema}.{self.name}
        """)).first()[0]
        if count > 0:
            logging.warning(
                "Table '%s'.'%s': %d rows remain staged.",
                self.schema, self.name, count
            )

        # Unpivot
        self.unpivot(conn)

    @classmethod
    def order_columns(cls, columns, order):
        """Sort table columns in the order presented
        in a CSV file to be used in a COPY statement.

        :param columns: Columns to be ordered.
        :type columns: list[ColumnMeta]

        :param order: Column names in the order they are to be returned.
        :type order: list[str]

        :return: Ordered list of columns.
        :rtype: list[ColumnMeta]
        """
        order = order or [col.name for col in columns]
        unique = set(columns)

        # Remove new line and quotes from header
        for i, header in enumerate(order):
            header = re.sub(r'(\\n|\")', "", header)
            order[i] = header

        # Remove newline character from last field
        if "\n" in order[-1]:
            order[-1] = order[-1][:-1]

        # Subset to columns present in csv
        filtered = filter(lambda x: x.name in order, unique)

        # Order columns according to csv
        ordered = sorted(filtered, key=lambda c: order.index(c.name))
        return list(ordered)


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


def create_role(conn, database):
    """Connect to database using SQLAlchemy.

    :param conn: SQLAlchemy connection or session.
    :type conn: sqlalchemy.Engine

    :param database: Name of the database.
    :type database: str
    """
    try:
        conn.execute(text(dedent("""
        CREATE ROLE {database}
        """).format(database=database)))
    except ProgrammingError as error:
        message = f'role "{database}" already exists'
        if message in error._message():  # pylint: disable=protected-access
            return
        raise error


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

    # Create database
    if not database_exists(engine.url):
        create_database(engine.url)

    # Connect
    conn = Session(bind=engine.connect())

    # Create role for database read access
    create_role(conn, database)
    conn.commit()

    return conn


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

    # Determine data file name
    meta_name = re.sub(r"^\d\d-", "", meta_path.stem)
    data_dir = meta_path.parents[1] / "data"

    # Determine whether we need to load
    # one or multiple files
    if (table_dir := data_dir / meta_name).is_dir():
        files = table_dir.iterdir()
    else:
        files = data_dir.glob(f"{meta_name}.*")

    # Determine file type(s)
    for file in files:
        suffix = "".join(file.suffixes)
        if suffix not in StageMeta._readers:  # pylint: disable=protected-access # noqa: E501
            continue
        data_path = file.absolute()

        # Stage data
        table_meta.stage_meta.load(conn, data_path)

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
    table_meta.enforce_constraints(conn)
    table_meta.load(conn)
    table_meta.create_indexes(conn)
    conn.commit()

    # Close connection
    conn.close()


if __name__ == "__main__":
    main()
