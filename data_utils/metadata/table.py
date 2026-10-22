"""
Table metadata.
"""
import re
import csv
import json
import gzip
import logging
from hashlib import md5
from pathlib import Path
from copy import deepcopy
from textwrap import dedent
from itertools import chain
from functools import partial

from pandas._libs.parsers import STR_NA_VALUES  # pylint: disable=no-name-in-module # noqa: E501
from sqlalchemy import MetaData as SQLAlchemyMetadata
from sqlalchemy import ForeignKeyConstraint, select
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    mapped_column,
    relationship
)

from data_utils.io import open_excel, NullStripper
from data_utils.metadata.column import ColumnMeta
from data_utils.metadata.location import SSHLocation
from data_utils.metadata.foreign_key import ForeignKeyMeta
from data_utils.metadata.unpivot import UnpivotMeta


logger = logging.getLogger(__name__)


class Base(DeclarativeBase):
    # pylint: disable=too-few-public-methods
    """
    Base ORM Class.
    """
    metadata = SQLAlchemyMetadata(schema="__metadata")


class Metadata(Base):
    # pylint: disable=too-few-public-methods
    """
    Base metadata.
    """
    __tablename__ = "table_"

    schema_: Mapped[str] = mapped_column(primary_key=True)
    table_: Mapped[str] = mapped_column(primary_key=True)
    checksum_: Mapped[str] = mapped_column()

    artifact: Mapped["Artifact"] = relationship(back_populates="table")


class Artifact(Base):
    # pylint: disable=too-few-public-methods
    """
    Artifact metadata.
    """
    __tablename__ = "artifact_"

    path_: Mapped[str] = mapped_column(primary_key=True)
    schema_: Mapped[str] = mapped_column()
    table_: Mapped[str] = mapped_column()
    checksum_: Mapped[str] = mapped_column()

    __table_args__ = (
        ForeignKeyConstraint(
            ["table_", "schema_"],
            ["table_.table_", "table_.schema_"]
        ),
    )

    table: Mapped["Metadata"] = relationship(back_populates="artifact")


class TableBase():
    """Base metadata for table."""
    # pylint: disable=too-few-public-methods, too-many-instance-attributes
    _row_id = "meta_row_id"

    def __init__(self, name, schema, columns, na_values=None,
                 encoding="utf-8", delimiter=",", checksum=None,
                 artifact=None):
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

    def create(self, session):
        """Create statement.

        :param session: Database Session.
        :type session: data_utils.metadata.database.Session

        :return: None
        """
        # Create the schema if it does not exists
        session.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema}")

        # We replace the table if it already exists
        session.execute(
            f"DROP TABLE IF EXISTS {self.schema}.{self.name} CASCADE")

        # Add columns from unpivot in correct order
        columns = self.columns.copy()
        if hasattr(self, "unpivot_meta"):
            for upv in self.unpivot_meta:
                for column in upv.target_columns + upv.target_values:
                    column = deepcopy(column)
                    column.required = False
                    columns.insert(getattr(column, "order", -1), column)

        # Prepare function to generate typed column names
        dialect = session.bind.dialect.name
        typed = partial(ColumnMeta.typed, dialect=dialect)

        # Creates table. All columns initially have TEXT type.
        # Types are enforced after loading the data one column
        # at a time so we can identify the specific column
        # in the data that might be violating its type specification.
        # The copy command would otherwise issue unhelpful log message.
        stmt = dedent("""
        CREATE TABLE {schema}.{name} (
            {columns}
          , {row_id} SERIAL
        )
        """).format(
            name=self.name, schema=self.schema, row_id=self._row_id,
            columns='\n  , '.join(map(typed, columns)),
        )
        session.execute(stmt)

        # Create index to speed up validations
        session.execute(dedent(f"""
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
            column.required = False
            column.dtype = dict.fromkeys(column.dtype.keys(), "TEXT")

    def create(self, session):
        """Create statement.

        :param session: Database Session.
        :type session: data_utils.metadata.database.Session

        :return: None
        """
        super().create(session)

        # Add column to log staging errors
        session.execute(dedent(f"""
        ALTER TABLE {self.schema}.{self.name} ADD COLUMN meta_log TEXT
        """))

        # Create index to speed up validations
        session.execute(dedent(f"""
        CREATE INDEX ON {self.schema}.{self.name} (meta_log)
        """))

    def load(self, session):
        """Stage data.

        :param session: Database Session.
        :type session: data_utils.metadata.database.Session

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
            logger.info("Table '%s'.'%s': Staging file '%s'.",
                        self.schema[1:], self.name, file.name)

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
            cursor = session.connection().connection.cursor()
            cursor.copy_expert(stmt, NullStripper(buffer))
            buffer.close()


class TableMeta(TableBase):
    """Metadata for table."""

    def __init__(self, constraints=None, indexes=None, unpivot=None,
                 location=None, **kwargs):
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
        self.uniques = constraints.get("uniques", [])
        self.indexes = indexes or []
        self.foreign_keys = []
        self.unpivot_meta = []
        self.location = (
            SSHLocation(**location)
            if location else None
        )

        # Instantiate metadata object for all foreign keys
        for fk in constraints.get("foreign_keys", []):
            dtypes = []
            for column in self.columns:
                if column.name not in fk["columns"]:
                    continue
                dtypes.append(column.dtype)
            fk_meta = ForeignKeyMeta(table=self, dtypes=dtypes, **fk)
            self.foreign_keys.append(fk_meta)

        # Unpivot
        for idx, upv in enumerate(unpivot or []):
            upv["alias"] = f"upv_{idx}"
            unpivot_meta = UnpivotMeta(**upv)
            self.unpivot_meta.append(unpivot_meta)
            for t_vals in unpivot_meta.target_values:
                self.columns.extend(t_vals.columns)

    @property
    def full_name(self):
        """Returns the full table name (schema.table), as the database folds
        it, since it is created without quotes."""
        return f"{self.schema}.{self.name}".lower()

    @property
    def requires(self):
        """Objects (schema.table) referenced by foreign keys."""
        return {
            f"{fk.reference.schema}.{fk.reference.table}".lower()
            for fk in self.foreign_keys
        } - {self.full_name}

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

    def create(self, session):
        """Create statement.

        :param session: Database Session.
        :type session: data_utils.metadata.database.Session

        :return: None
        """
        super().create(session)

        # Create stage as well
        self.stage_meta.create(session)

    def unpivot(self, session):
        """Unpivot a table.

        :param session: Database Session.
        :type session: data_utils.metadata.database.Session

        :return: None
        """
        if not self.unpivot_meta:
            return

        # Retrieve database dialect from engine
        dialect = session.bind.dialect.name

        # Retrieve information from all metadata
        drop = []
        columns = []
        aliased = []
        require = []
        laterals = []
        for upv in self.unpivot_meta:
            drop.extend(map(str, upv.columns()))
            for col in upv.target_columns + upv.target_values:
                if col.required:
                    require.append(col.name)
                columns.append(col.name)
                aliased.append(f"{upv.alias}.{col.name}::{col.dtype[dialect]}")
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
        session.execute(stmt)

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
        session.execute(stmt)

        # Enforce primary key
        self.enforce_pk(session)

        # Enforce required target columns
        stmt = dedent("""
        ALTER TABLE {schema}.{name}
            ALTER COLUMN {columns} SET NOT NULL
        ;
        """).format(
            schema=self.schema,
            name=self.name,
            columns=' SET NOT NULL\n  , ALTER COLUMN '.join(require),
        )
        session.execute(stmt)

    def resolve_columns(self, session):
        """Resolve NULL values and apply description/comment.

        :param session: Database Session.
        :type session: data_utils.metadata.database.Session

        :return: None
        """
        logger.info("Table '%s'.'%s': Resolving NULL values.", self.schema,
                    self.name)
        # Enforce constraints
        for column in self.columns:
            # Resolve NULL values
            column.resolve_na(session, self.schema, self.name, self.na_values)

            # Add comment to columns
            column.comment(session, self.schema, self.name)

    def enforce_fks(self, session):
        """Enforce foreign key constraints.

        :param session: Database Session.
        :type session: data_utils.metadata.database.Session

        :return: None
        """
        logger.info("Table '%s'.'%s': Enforcing foreign keys.", self.schema,
                    self.name)
        # Enforce foreign keys
        for fk in self.foreign_keys:
            fk.create(session)

    def create_indexes(self, conn):
        """Create table indexes.

        :param conn: SQLAlchemy connection or session.
        :type conn: sqlalchemy.Engine

        :return: None
        """
        for index in self.indexes:
            logger.info("Table '%s'.'%s': Creating index %s.", self.schema,
                        self.name, tuple(index))
            stmt = dedent("""
            CREATE INDEX ON {schema}.{name} ({columns})
            """).format(
                schema=self.schema,
                name=self.name,
                columns=", ".join(index)
            )
            conn.execute(stmt)

    def create_uniques(self, conn):
        """Create table unique indexes.

        :param conn: SQLAlchemy connection or session.
        :type conn: sqlalchemy.Engine

        :return: None
        """
        for index in self.uniques:
            logger.info("Table '%s'.'%s': Creating unique index %s.",
                        self.schema, self.name, tuple(index))
            stmt = dedent("""
            CREATE UNIQUE INDEX ON {schema}.{name} ({columns})
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

    def load(self, session, batch_size=1000000):
        # pylint: disable=too-many-locals
        """Load data from stage.

        :param session: Database Session.
        :type session: data_utils.metadata.database.Session

        :return: None
        """
        # Get database dialect from connection
        dialect = session.bind.dialect.name

        # Generate JOIN statements for foreign keys
        join_fks = []
        null_fks = []
        for idx, fk in enumerate(self.foreign_keys):
            join_fks.append(fk.join(f"fk_{idx}", dialect))
            null_fks.append(fk.distinct_reference(f"fk_{idx}", dialect))

        typed = []
        columns = []
        where_req = ["0 = 1"]
        for column in self.columns:
            # Append to list of columns to be retrieved
            columns.append(str(column))
            typed.append(f"{column.rename}::{column.dtype[dialect]}")

            # Add required columns to where statement filtering not null values
            if not column.required and column.name not in self.primary_key:
                continue
            where_req.append(f"{self.name}.{column} IS NULL")

        # Move rows from staging to final schema
        logger.info("Table '%s'.'%s': Loading.", self.schema, self.name)

        # Skipped if we are pivoting the table later
        if not self.unpivot_meta:
            self.enforce_pk(session)

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
        session.execute(stmt, buffer_size=5)

        # Flag rows violating a foreign key constraint
        stmt = dedent("""
        WITH missing_fk AS (
          SELECT {name}.{row_id}
          FROM _{schema}.{name}
          {join_fks}
          WHERE {n_fks} > 0
          AND {name}.meta_log IS NULL
          AND (
            0 = 1
            {null_fks}
          )
        )
        UPDATE _{schema}.{name}
        SET meta_log = 'Constraint violation (foreign key)'
        FROM missing_fk
        WHERE {name}.{row_id} = missing_fk.{row_id}
        """).format(
            name=self.name, schema=self.schema, row_id=self._row_id,
            join_fks="\n  ".join(chain(*join_fks)),
            n_fks=len(join_fks),
            null_fks="\n    ".join(chain(*null_fks)),
        )
        session.execute(stmt, buffer_size=1)

        # Retrieve max id staged for insert
        stmt = dedent("""
        SELECT MAX({row_id})
        FROM _{schema}.{name}
        WHERE meta_log IS NULL
        """).format(
            name=self.name,
            schema=self.schema,
            row_id=self._row_id,
        )
        max_row_id = session.execute(stmt, buffer_size=1).first()[0]

        # Move rows to final schema
        for batch_start in range(0, max_row_id, batch_size):
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
            AND {row_id} >  {batch_start}
            AND {row_id} <= {batch_max}
            ON CONFLICT DO NOTHING
            """).format(
                name=self.name, schema=self.schema, row_id=self._row_id,
                columns="\n  , ".join(columns),
                typed="\n  , ".join(typed),
                batch_start=batch_start,
                batch_max=batch_start+batch_size
            )
            session.execute(stmt, buffer_size=1)

        # Deleted moved rows
        stmt = dedent("""
        DELETE from _{schema}.{name}
        USING {schema}.{name} moved
        WHERE {name}.meta_row_id = moved.meta_row_id
        """).format(
            name=self.name, schema=self.schema
        )
        session.execute(stmt, buffer_size=5)

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
        session.execute(stmt, buffer_size=5)

        # Warn of any remaining staged records
        count = session.execute(dedent(f"""
        SELECT COUNT(meta_row_id) from _{self.schema}.{self.name}
        """)).first()[0]
        if count > 0:
            logger.warning(
                "Table '%s'.'%s': %d rows remain staged.",
                self.schema, self.name, count
            )

        # Unpivot
        self.unpivot(session)

        # Local File System location
        if self.location:
            # Add file location column to table
            self.location.add_column(session, table=self)
            # Find all files in specified bucket/folder and join them to table
            self.location.find(session, table=self)

    def create_metadata(self, session):
        """
        :param session: Database Session.
        :type session: data_utils.metadata.database.Session
        """
        # Table metadata
        meta = Metadata(
            schema_=self.schema, table_=self.name, checksum_=self.checksum)
        session.merge(meta)

        # Artifact metadata
        repo_dir = Path(self.artifact["repo"].root_dir)
        data_path = Path(repo_dir.name) / self.artifact["name"]
        artifact = Artifact(
            path_=str(data_path),
            schema_=self.schema, table_=self.name,
            checksum_=self.artifact["md5"])
        session.merge(artifact)

    def is_updated(self, session):
        """
        :param session: Database Session.
        :type session: data_utils.metadata.database.Session
        """
        stmt = select(Metadata).where(
            Metadata.schema_ == self.schema,
            Metadata.table_ == (self.name)
        )
        metadata = session.execute(stmt).fetchone()

        return (
            metadata is None or
            metadata[0].checksum_ != self.checksum or
            metadata[0].artifact.checksum_ != self.artifact["md5"]
        )

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
            if artifact_name.lower() == meta_name:
                break
        else:
            raise FileNotFoundError(
                f"No files found for metadata: '{meta_name}'")

        return TableMeta(
            artifact=artifact, checksum=meta_hash.hexdigest(),
            **meta_json)
