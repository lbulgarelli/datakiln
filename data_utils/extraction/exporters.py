"""
Data and metadata export logic for the extraction pipeline.
"""
import csv
import json
import gzip
import logging
import hashlib
from pathlib import Path
from typing import List, Dict, Optional

from sqlalchemy import MetaData, Table, Column
from sqlalchemy.dialects import registry

from data_utils.metadata.database import Session


class MetadataExtractor:
    """Class to handle metadata extraction and generation."""

    @staticmethod
    def get_metadata(
        session: Session,
        schema: str,
        tables: List[str]
    ) -> MetaData:
        """Generate column metadata from DBMS."""
        logging.info("RETRIEVING SCHEMA METADATA: %s", schema)
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
    def stream_to_csv(
        session: Session,
        queries: List[str],
        export_path: Path,
        table_name: str = "unknown",
        insert_to: Optional[Table] = None
    ) -> str:
        # pylint: disable=too-many-locals
        """Streams multiple SQL queries sequentially into a compressed CSV.

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
                logging.info("STREAMING DATA TO: %s", export_path.name)
                writer = csv.writer(o_file)
                header_written = False
                count = 0
                idx = 0

                total_chunks = len(queries)
                for i, query in enumerate(queries):
                    result = session.execute(
                        query,
                        execution_options={"stream_results": True}
                    )

                    if not header_written:
                        header = list(result.keys())
                        try:
                            idx = header.index("meta_row_id")
                            header.pop(idx)
                        except ValueError:
                            idx = len(header)
                        writer.writerow(header)
                        header_written = True

                    yield_size = 10000
                    for partition in result.partitions(yield_size):
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

                        # Insert batch into table
                        if batch_inserts:
                            session.execute(insert_to.insert(), batch_inserts)
                            session.commit()

                    logging.info(
                        "PROCESSED CHUNK %d/%d ON %s (ACCUMULATED %d ROWS)",
                        i + 1, total_chunks, table_name, count
                    )

                logging.info("EXTRACTED %d ROWS FROM %s", count, table_name)

        return hash_writer.hasher.hexdigest()
