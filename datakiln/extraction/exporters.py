"""
Data and metadata export logic for the extraction pipeline.
"""
import csv
import json
import gzip
import logging
import hashlib
import threading
from pathlib import Path
from typing import List, Dict, Optional

from tenacity import (
    retry,
    stop_after_attempt,
    wait_fixed,
    retry_if_exception_type,
    before_sleep_log
)
from sqlalchemy import MetaData, Table, Column, Select
from sqlalchemy.dialects import registry
from sqlalchemy.exc import SQLAlchemyError

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
        tables: List[str]
    ) -> MetaData:
        """Generate column metadata from DBMS."""
        logger.info("Schema '%s': Retrieving metadata.", schema)
        metadata = MetaData(schema=schema)
        metadata.reflect(bind=session.bind, only=tables, views=True)
        return metadata

    @staticmethod
    def generate_column_meta(
        column: Column,
        dialect: str = None,
        compile_type: bool = True
    ) -> Dict:
        """Generate column metadata from DBMS."""
        dialect_obj = registry.load(dialect)
        type_ = column.type.dialect_impl(dialect_obj())
        return {
            "name": column.name,
            "dtype": {
                dialect: type_.compile() if compile_type else type_
            }
        }

    @staticmethod
    def generate_table_meta(
        meta_dir: Path,
        table: Table,
        dialect: str = None
    ) -> Dict:
        """Generate table metadata from DBMS."""
        meta = {
            "name": table.name,
            "schema": "public",
            "encoding": "utf-8",
            "columns": [
                MetadataExtractor.generate_column_meta(column, dialect)
                for column in table.columns
            ],
            "constraints": {
                "primary_key": [
                    column.name
                    for column in table.primary_key
                ]
            }
        }
        meta_path = meta_dir / f"{table.name}.json"
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


class DataExporter:
    # pylint: disable=too-few-public-methods
    """Class to handle data export logic."""

    @staticmethod
    @db_retry
    def stream_to_csv(
        session: Session,
        query: Select,
        export_path: Path,
        table_name: str = "unknown",
        insert_to: Optional[Table] = None,
        chunk_size: int = 10000,
        abort: Optional[threading.Event] = None
    ) -> str:
        # pylint: disable=too-many-locals, too-many-arguments
        # pylint: disable=too-many-positional-arguments
        """Streams a single SQL query into a compressed CSV.

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
                writer = csv.writer(o_file)
                count = 0
                idx = 0

                result = session.execute(
                    query,
                    execution_options={
                        "stream_results": True,
                        "skip_formatting": True
                    }
                )

                # Write CSV header
                header = list(result.keys())
                try:
                    idx = header.index("meta_row_id")
                    header.pop(idx)
                except ValueError:
                    idx = len(header)
                writer.writerow(header)

                # Write data in chunks
                chunk_idx = 0
                engine = session.get_bind()
                for partition in result.partitions(chunk_size):
                    if abort and abort.is_set():
                        logger.warning(
                            "Table '%s': Pipeline aborted, stopping "
                            "extraction.", table_name
                        )
                        break

                    chunk_idx += 1
                    batch_inserts = []
                    for row in partition:
                        # Remove meta_row_id if present
                        data = row[:idx] + row[idx + 1:]

                        # Write row to CSV file
                        writer.writerow(data)
                        count += 1

                        # Convert row to dictionary for insertion
                        if insert_to is not None:
                            batch = dict(zip(result.keys(), row))
                            batch_inserts.append(batch)

                    # Insert batch into table using a separate connection
                    if batch_inserts:
                        @db_retry
                        def _insert_batch(batch):
                            with engine.begin() as conn:
                                stmt = insert_to.insert().execution_options(
                                    skip_formatting=True
                                )
                                conn.execute(stmt, batch)
                        _insert_batch(batch_inserts)

                    logger.info(
                        "Table '%s': Processed chunk %d, %d rows so far.",
                        table_name, chunk_idx, count
                    )

                logger.info(
                    "Table '%s': Extracted %d rows.", table_name, count)

        return hash_writer.hasher.hexdigest()
