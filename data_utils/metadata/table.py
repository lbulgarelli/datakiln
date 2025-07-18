import re
import csv
import json
import gzip
import logging
from hashlib import md5
from pathlib import Path
from textwrap import dedent
from itertools import chain
from functools import partial

from pandas._libs.parsers import STR_NA_VALUES  # pylint: disable=no-name-in-module # noqa: E501

from data_utils.io import open_excel
from data_utils.metadata.column import ColumnMeta
from data_utils.metadata.location import LFSLocation
from data_utils.metadata.foreign_key import ForeignKeyMeta
from data_utils.metadata.unpivot import UnpivotMeta


logging.basicConfig(
    level=logging.INFO, datefmt="%Y-%m-%d %H:%M:%S",
    format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s")


class TableBase():
    """Base metadata for table."""
    # pylint: disable=too-few-public-methods
    _row_id = "meta_row_id"

    def __init__(self, name, schema, columns, na_values=None,
                 encoding="utf-8", delimiter=",", checksum=None,
                 artifact=None):
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

        :param delimiter: Delimiter that separate values in data files.
        :type delimiter: str

        :param hash: Hash of metadata file.
        :type hash: str

        :return: Base table metadata.
        :rtype: TableBase
        """
        self.name = name
        self.schema = schema
        self.columns = []
        self.encoding = encoding
        self.delimiter = delimiter
        self.na_values = set(na_values or []) | STR_NA_VALUES
        self.checksum = checksum
        self.artifact = artifact

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
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema}")

        # We replace the table if it already exists
        conn.execute(f"DROP TABLE IF EXISTS {self.schema}.{self.name} CASCADE")

        # Add columns from unpivot in correct order
        columns = self.columns.copy()
        if hasattr(self, "unpivot_meta"):
            for upv in self.unpivot_meta:
                for column in upv.target_columns:
                    columns.insert(column.order, column)
                columns.extend(upv.target_values)

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
            columns=' TEXT\n  , '.join(map(str, columns)),
        )
        conn.execute(stmt)

        # Create index to speed up validations
        conn.execute(dedent(f"""
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

        self.schema = f"_{self.schema}"

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
        conn.execute(dedent(f"""
        ALTER TABLE {self.schema}.{self.name} ADD COLUMN meta_log TEXT
        """))

        # Create index to speed up validations
        conn.execute(dedent(f"""
        CREATE INDEX ON {self.schema}.{self.name} (meta_log)
        """))

    def load(self, conn):
        """Stage data.

        :param conn: SQLAlchemy connection or session.
        :type conn: sqlalchemy.Engine

        :return: None
        """
        repo_dir = self.artifact["repo"].root_dir
        data_path = Path(repo_dir) / self.artifact["name"]

        # Determine whether we need to load
        # one or multiple files
        if data_path.is_dir():
            files = list(data_path.iterdir())
        else:
            files = [data_path]

        # Determine file type(s)
        for file in files:
            suffix = "".join(file.suffixes)
            if suffix not in StageMeta._readers:  # pylint: disable=protected-access # noqa: E501
                continue
            data_path = file.absolute()

            # Stage data
            logging.info("Table '%s'.'%s': Staging file '%s'.",
                         self.schema[4:], self.name, file.name)

            # Get header from file
            suffix = "".join(data_path.suffixes)
            reader = self._readers[suffix]
            buffer = reader(data_path, encoding=self.encoding)
            header = next(csv.reader(buffer, delimiter=self.delimiter))

            # To load the data, we need to order
            # the column list according to the file
            columns = filter(lambda c: not c.transform, self.columns)
            columns = sorted(columns, key=lambda c: header.index(c.name))  # pylint: disable=cell-var-from-loop # noqa: E501

            # Generate statement
            stmt = dedent("""
            COPY {schema}.{name} (
                {columns}
            )
            FROM STDIN
            WITH (FORMAT CSV, DELIMITER E'{delimiter}')
            """).format(
                name=self.name, schema=self.schema,
                columns='\n  , '.join(map(str, columns)),
                delimiter=self.delimiter
            )

            # Execute and copy from file
            cursor = conn.connection().connection.cursor()
            cursor.copy_expert(stmt, buffer)
            buffer.close()


class TableMeta(TableBase):
    """Metadata for table."""

    def __init__(self, constraints=None, indexes=None, unpivot=None,
                 lfs_location=None, **kwargs):
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
        self.lfs_location = (
            LFSLocation(**lfs_location)
            if lfs_location else None
        )

        # Instantiate metadata object for all foreign keys
        for fk in constraints.get("foreign_keys", []):
            fk_meta = ForeignKeyMeta(table=self, **fk)
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
            encoding=self.encoding, delimiter=self.delimiter,
            columns=list(map(dict, self.columns)),
            artifact=self.artifact)

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
        conn.execute(stmt)

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
        conn.execute(stmt)

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
        logging.info("Table '%s'.'%s': Enforcing constraints.", self.schema,
                     self.name)
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

        # Enforce foreign keys
        for fk in self.foreign_keys:
            fk.create(conn)

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
            conn.execute(stmt)

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
        conn.execute(stmt)

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
        UPDATE _{schema}.{name}
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
        conn.execute(stmt)

        # Flag rows violating a foreign key constraint
        stmt = dedent("""
        WITH missing_fk AS (
          SELECT {name}.{row_id}
          from _{schema}.{name}
          {join_fks}
          WHERE {n_fks} > 0
          AND {name}.meta_log IS NULL
          {null_fks}
        )
        UPDATE _{schema}.{name}
        SET meta_log = 'Constraint violation (foreign key)'
        FROM missing_fk
        WHERE {name}.{row_id} = missing_fk.{row_id}
        """).format(
            name=self.name, schema=self.schema, row_id=self._row_id,
            join_fks="\n  ".join(chain(*join_fks)),
            n_fks=len(join_fks),
            null_fks="\n  ".join(chain(*null_fks)),
        )
        conn.execute(stmt)

        # Move rows to final schema
        stmt = dedent("""
        INSERT INTO {schema}.{name} (
            {columns}
          , {row_id}
        )
        SELECT
            {typed}
          , {row_id}
        from _{schema}.{name}
        WHERE 1 = 1
        AND meta_log IS NULL
        ON CONFLICT DO NOTHING
        """).format(
            name=self.name, schema=self.schema, row_id=self._row_id,
            columns="\n  , ".join(columns),
            typed="\n  , ".join(typed),
        )
        conn.execute(stmt)

        # Deleted moved rows
        stmt = dedent("""
        DELETE from _{schema}.{name}
        USING {schema}.{name} moved
        WHERE {name}.meta_row_id = moved.meta_row_id
        """).format(
            name=self.name, schema=self.schema
        )
        conn.execute(stmt)

        # Rows that still remain in staging but did not
        # get flagged before this point are PK duplicates
        stmt = dedent("""
        UPDATE _{schema}.{name}
        SET meta_log = 'Duplicate'
        WHERE 1 = 1
        AND meta_log IS NULL
        """).format(
            name=self.name, schema=self.schema
        )
        conn.execute(stmt)

        # Warn of any remaining staged records
        count = conn.execute(dedent(f"""
        SELECT COUNT(meta_row_id) from _{self.schema}.{self.name}
        """)).first()[0]
        if count > 0:
            logging.warning(
                "Table '%s'.'%s': %d rows remain staged.",
                self.schema, self.name, count
            )

        # Unpivot
        self.unpivot(conn)

        # Local File System location
        if self.lfs_location:
            # Add file location column to table
            self.lfs_location.add_column(conn, table=self)
            # Find all files in specified bucket/folder and join them to table
            self.lfs_location.find(conn, table=self)

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

    @classmethod
    def from_json(cls, meta_path, data_repo):
        """Load metadata.

        :param meta_path: Path to the metadata
        :type meta_path: str

        :param conn: SQLAlchemy connection or session.
        :type conn: sqlalchemy.Engine

        :return: None
        """
        # Load json
        with open(meta_path, "rb") as file:
            content = b""
            meta_hash = md5()  # noqa: DUO130
            for chunk in iter(lambda: file.read(4096), b""):
                content += chunk
                meta_hash.update(chunk)
            meta_json = json.loads(content.decode("utf-8"))
        meta_name = re.sub(r"^\d\d-", "", meta_path.stem)

        # Determine path to data file(s)
        for artifact in data_repo.listdir(".", dvc_only=True):
            artifact_path = Path(artifact["name"])
            suffix = "".join(artifact_path.suffixes)

            # Get name of artifact without suffixes
            if len(suffix) > 0:
                artifact_name = str(artifact_path.name)[:-len(suffix)]
            else:
                artifact_name = artifact_path.name

            # Match names
            if artifact_name == meta_name:
                break
        else:
            raise FileNotFoundError(
                f"No files found for metadata: '{meta_name}'")

        return TableMeta(
            artifact=artifact, checksum=meta_hash.hexdigest(),
            **meta_json)
