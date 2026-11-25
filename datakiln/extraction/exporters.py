"""
Data and metadata export logic for the extraction pipeline.
"""
import csv
import gzip
import hashlib
import json
import logging
import threading
import time
from copy import copy
from pathlib import Path
from typing import Any

from sqlalchemy import Boolean, Column, MetaData, Table
from sqlalchemy.dialects import registry
from sqlalchemy.dialects.mssql import BIT
from sqlalchemy.engine import Engine, Result
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql.selectable import SelectBase
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_fixed,
)

from datakiln.metadata.database import Session

logger = logging.getLogger(__name__)
db_retry = retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(10),
    retry=retry_if_exception_type((SQLAlchemyError, OSError)),
    before_sleep=before_sleep_log(logger, logging.WARNING)
)


class MetadataExtractor:
    """Class to handle metadata extraction and generation."""

    @staticmethod
    def get_metadata(
        session: Session,
        schema: str,
        tables: list[str]
    ) -> MetaData:
        """Generate column metadata from DBMS."""
        logger.info("Schema '%s': Retrieving metadata.", schema)
        metadata = MetaData(schema=schema)
        metadata.reflect(
            bind=session.get_bind(),
            only=lambda name, _: name.lower() in tables,
            views=True
        )
        return metadata

    @staticmethod
    def generate_column_meta(
        column: Column,
        dialect: str = None,
        compile_type: bool = True
    ) -> dict:
        """Generate column metadata from DBMS."""
        dialect_obj = registry.load(dialect)()

        # SQL Server has no adapted type for its bit, which is a boolean
        sql_type = column.type
        if isinstance(sql_type, BIT):
            sql_type = Boolean()
        type_ = copy(sql_type.dialect_impl(dialect_obj))

        # The collation names are the ones of the source, which the target
        # does not have
        if hasattr(type_, "collation"):
            type_.collation = None

        return {
            "name": column.name.lower(),
            "dtype": {
                dialect: type_.compile(dialect=dialect_obj)
                if compile_type else type_
            }
        }

    @staticmethod
    def generate_table_meta(
        meta_dir: Path,
        table: Table,
        dialect: str = None
    ) -> dict:
        """Generate table metadata from DBMS."""
        meta = {
            # The build names tables in lower case, SQL Server keeps its case
            "name": table.name.lower(),
            "schema": "public",
            "encoding": "utf-8",
            "columns": [
                MetadataExtractor.generate_column_meta(column, dialect)
                for column in table.columns
            ],
            "constraints": {
                "primary_key": [
                    column.name.lower()
                    for column in table.primary_key
                ]
            }
        }
        # The data file is found by this file name, in lower case
        meta_path = meta_dir / f"{table.name.lower()}.json"
        with open(meta_path, "w", encoding="utf-8") as file:
            json.dump(meta, file, indent=4)


class HashingFileWriter:
    """A file-like writer that calculates the MD5 hash on-the-fly."""

    def __init__(self, fp):
        """Initialize the hashing file writer."""
        self.fp = fp
        self.hasher = hashlib.md5()  # noqa: DUO130

    def write(self, b):
        """Write bytes to the file and update the hash."""
        self.fp.write(b)
        self.hasher.update(b)
        return len(b)

    def flush(self):
        """Flush the file buffer."""
        self.fp.flush()

    def close(self):
        """Close the file."""
        self.fp.close()


class SeenRows:
    """Tells which rows an earlier query of the same table returned.

    The filters only read the columns of a row, so identical rows match the
    same groups: a group that returned a row returned every copy of it. A
    row that an earlier group returned is therefore skipped, and the copies
    within one group are all kept.
    """

    def __init__(self):
        """Start with no query done."""
        # Digests of the rows that the earlier queries returned, and of the
        # rows that the current one has returned so far
        self.earlier = set()
        self.current = set()

        # Whether to keep the digests of the current query, and how many
        # of its rows an earlier query had returned
        self.remember = True
        self.skipped = 0

    def start(self, last: bool) -> None:
        """Start the next query, which has the rows of the previous ones.

        :param last: Whether it is the last query, whose rows nothing
                     needs to be told apart from.
        :type last: bool
        """
        # What the previous query returned is now what an earlier one did
        self.earlier |= self.current
        self.current = set()

        # No query comes after the last one, so its rows need not be kept
        self.remember = not last
        self.skipped = 0

    def repeated(self, data: tuple) -> bool:
        """Tell whether an earlier query returned this row.

        :param data: Values of the row.
        :type data: tuple

        :return: Whether to skip the row.
        :rtype: bool
        """
        # A digest of the row takes less memory than the row
        digest = hashlib.blake2b(
            repr(data).encode(), digest_size=16
        ).digest()

        # An earlier query returned it, so every copy was written already
        if digest in self.earlier:
            self.skipped += 1
            return True

        # The later queries must be able to tell that this one returned it
        if self.remember:
            self.current.add(digest)
        return False


class RowWriter:
    """Writes the rows of the queries of one table to its CSV, each row once.

    A row that an earlier query returned is skipped; see SeenRows.
    """

    def __init__(
        self,
        writer: Any,
        engine: Engine,
        insert_to: Table | None,
        table_name: str
    ):
        """Keep where the rows go.

        :param writer: CSV writer of the file.
        :type writer: csv.writer

        :param engine: Engine of the source, which inserts the rows.
        :type engine: sqlalchemy.engine.Engine

        :param insert_to: Table that also receives the rows, if any.
        :type insert_to: sqlalchemy.Table, optional

        :param table_name: Name of the table, for the log.
        :type table_name: str
        """
        # Where the rows go
        self.writer = writer
        self.engine = engine
        self.insert_to = insert_to
        self.table_name = table_name

        # The rows of the earlier queries, once there is more than one
        # query, and how much has been written
        self.seen = None
        self.count = 0
        self.chunks = 0

    @staticmethod
    def meta_index(keys: list[str]) -> int:
        """Find the column meta_row_id, which is not written.

        :param keys: Names of the columns.
        :type keys: list[str]

        :return: Its position, or the number of columns if there is none.
        :rtype: int
        """
        # The names are written in lower case
        header = list(map(str.lower, keys))

        # Without the column, nothing is removed from a row
        if "meta_row_id" in header:
            return header.index("meta_row_id")
        return len(header)

    def start(self, keys: list[str], first: bool, last: bool) -> None:
        """Start the rows of the next query.

        :param keys: Names of the columns of the query.
        :type keys: list[str]

        :param first: Whether it is the first query, which writes the header.
        :type first: bool

        :param last: Whether it is the last query.
        :type last: bool
        """
        # The queries share one file and one header, without meta_row_id
        if first:
            header = list(map(str.lower, keys))
            idx = self.meta_index(keys)
            self.writer.writerow(header[:idx] + header[idx + 1:])

        # Rows that several queries return are told apart only when there
        # is more than one query
        if first and not last:
            self.seen = SeenRows()
        if self.seen is not None:
            self.seen.start(last)

    def write_result(
        self,
        result: Result,
        chunk_size: int,
        abort: threading.Event | None
    ) -> bool:
        """Write the rows of a result in chunks.

        :param result: Result of a query, which streams its rows.
        :type result: sqlalchemy.engine.Result

        :param chunk_size: Number of rows of a chunk.
        :type chunk_size: int

        :param abort: Event that stops the pipeline, if any.
        :type abort: threading.Event, optional

        :return: Whether all the rows were written, and not the pipeline
                 aborted.
        :rtype: bool
        """
        # The time to fetch a chunk is told apart from the time to write it
        keys = list(result.keys())
        ready = time.time()

        # Write the rows in chunks, as the database returns them
        for partition in result.partitions(chunk_size):
            fetched = time.time()

            # The pipeline stops the extraction when another table failed
            if abort and abort.is_set():
                logger.warning(
                    "Table '%s': Pipeline aborted, stopping extraction.",
                    self.table_name
                )
                return False

            # Write the chunk, and say how long it took
            self.chunks += 1
            self.write(partition, keys)
            logger.info(
                "Table '%s': Processed chunk %d, %d rows so far "
                "(fetched in %.1f s, written in %.1f s).",
                self.table_name, self.chunks, self.count,
                fetched - ready, time.time() - fetched
            )
            ready = time.time()

        # Say how many rows an earlier group had already returned
        if self.seen is not None and self.seen.skipped:
            logger.info(
                "Table '%s': Skipped %d rows that earlier groups returned.",
                self.table_name, self.seen.skipped
            )
        return True

    def write(self, partition: list, keys: list[str]) -> None:
        """Write the rows of a chunk that no earlier query returned.

        :param partition: Rows of the chunk.
        :type partition: list

        :param keys: Names of the columns.
        :type keys: list[str]
        """
        # The rows to insert are collected while the others are written
        idx = self.meta_index(keys)
        inserts = []
        for row in partition:
            # Remove meta_row_id if present
            data = row[:idx] + row[idx + 1:]

            # An earlier query already returned this row
            if self.seen is not None and self.seen.repeated(data):
                continue

            # Write row to CSV file
            self.writer.writerow(data)
            self.count += 1

            # Convert row to dictionary for insertion
            if self.insert_to is not None:
                inserts.append(dict(zip(keys, row)))

        # Insert batch into table using a separate connection
        if inserts:
            self._insert(inserts)

    @db_retry
    def _insert(self, batch: list[dict]) -> None:
        """Insert rows into the table, trying again if the database fails."""
        # One transaction for the chunk, so all its rows are inserted or none
        with self.engine.begin() as conn:
            stmt = self.insert_to.insert().execution_options(
                skip_formatting=True
            )
            conn.execute(stmt, batch)


class DataExporter:
    # pylint: disable=too-few-public-methods
    """Class to handle data export logic."""

    @staticmethod
    def _execute(
        session: Session,
        query: SelectBase,
        table_name: str,
        position: int,
        total: int
    ) -> Result:
        """Execute a query, and tell how long the database took to start it.

        :param session: Session of the source.
        :type session: datakiln.metadata.database.Session

        :param query: Statement to execute.
        :type query: sqlalchemy.sql.selectable.SelectBase

        :param table_name: Name of the table, for the log.
        :type table_name: str

        :param position: Position of the query among those of the table.
        :type position: int

        :param total: Number of queries of the table.
        :type total: int

        :return: Result that streams the rows.
        :rtype: sqlalchemy.engine.Result
        """
        # The database receives and starts the statement, which takes
        # the time before the first row
        start = time.time()
        result = session.execute(
            query,
            execution_options={
                "stream_results": True,
                "skip_formatting": True
            }
        )

        # A table with several queries says which group each one is
        group = ""
        if total > 1:
            group = f" of group {position + 1} of {total}"

        # The size of the parameters, such as the seeds of a filter
        params = result.context.compiled.params
        logger.info(
            "Table '%s': Executed the statement%s in %.1f s, with %d "
            "characters of parameters.",
            table_name, group, time.time() - start,
            sum(len(str(val)) for val in params.values())
        )
        return result

    @staticmethod
    @db_retry
    def stream_to_csv(
        session: Session,
        queries: list[SelectBase],
        export_path: Path,
        table_name: str = "unknown",
        insert_to: Table | None = None,
        chunk_size: int = 10000,
        abort: threading.Event | None = None
    ) -> str:
        # pylint: disable=too-many-arguments
        # pylint: disable=too-many-positional-arguments
        """Streams the SQL queries of a table into a compressed CSV, one
        after the other. A row that an earlier query returned is not
        written again.

        Returns the MD5 checksum of the generated compressed CSV file.
        """
        with open(export_path, 'wb') as f_raw:
            hash_writer = HashingFileWriter(f_raw)
            with gzip.open(
                hash_writer,
                mode='wt',
                encoding='utf-8',
                newline=''
            ) as o_file:
                logger.info("File '%s': Streaming data.", export_path.name)

                # The queries of the table write to this one file
                rows = RowWriter(
                    csv.writer(o_file),
                    session.get_bind(),
                    insert_to,
                    table_name
                )

                # Each query writes its rows after those of the previous one
                for position, query in enumerate(queries):
                    # The database starts the query, and says its columns
                    result = DataExporter._execute(
                        session, query, table_name, position, len(queries)
                    )
                    rows.start(
                        list(result.keys()),
                        position == 0,
                        position == len(queries) - 1
                    )

                    # Write its rows, unless the pipeline aborted
                    if not rows.write_result(result, chunk_size, abort):
                        break

                # The rows written, without those that earlier groups had
                logger.info(
                    "Table '%s': Extracted %d rows.", table_name, rows.count)

        return hash_writer.hasher.hexdigest()
