"""
File system location metadata.
"""
import re
import logging
from pathlib import Path
from stat import S_ISDIR
from textwrap import dedent

from decouple import config
from paramiko import SSHClient, AutoAddPolicy

from data_utils.metadata.column import ColumnMeta


logger = logging.getLogger(__name__)


class SSHLocation(ColumnMeta):
    # pylint: disable=too-many-instance-attributes
    """Column with local file locations."""

    def __init__(self, host, folder, regex, match,
                 default_values=None, match_map=None,
                 **kwargs):
        # pylint: disable=too-many-arguments, too-many-positional-arguments
        """
        :param folder: Directory path to search for files.
        :type folder: str

        :param regex: Regular expression to use in order to
                      match file names against chosen column.
        :type regex: str

        :param match: Column to perform match against in table.
        :type match: str

        :param default_values: Default values for each match.
        :type replace: list

        :param match_map: Replace values for each match using a dict.
        :type replace: list
        """
        self.host = host
        self.folder = Path(folder)
        self.regex = regex
        self.match = match
        self.default_values = default_values
        self.match_map = match_map

        super().__init__(**kwargs, dtype={"postgresql": "TEXT[]"})

    def _match(self, string):
        """Find matching pattern on string and normalize to default values."""
        match = re.search(self.regex, string)
        if match:
            extracted = []
            groups = match.groups()

            # Iterates over each match to apply default values
            for i in range(len(self.match)):
                if groups[i] is not None:
                    extracted.append(groups[i])
                else:
                    extracted.append(self.default_values[i])

            # Return True if match was found
            return extracted

        return [''] * len(self.match)

    def _glob(self, sftp, path):
        """Recursively iterate directory."""
        for entry in sftp.listdir_attr(path.as_posix()):
            file_path = path / entry.filename
            if S_ISDIR(entry.st_mode):
                yield from self._glob(sftp, file_path)
            else:
                yield file_path

    def find(self, conn, table):
        """Find file names in directory and store them in a staging table.

        :param con: SQLAlchemy connection/session.
        :type con: sqlalchemy.Engine

        :param table: Table metadata
        :type table: TableMeta

        :return: None
        """

        logger.info("Table '%s'.'%s': Searching local file system.",
                    table.schema, table.name)

        # List to append new rows and counter for number of matches
        formatted_rows = []
        n_matches = 0

        # Establish SSH connection
        ssh = SSHClient()
        ssh.set_missing_host_key_policy(AutoAddPolicy())
        ssh.connect(
            hostname=self.host,
            username=config("SSH_USER"),
            password=config("SSH_PASSWORD")
        )

        # Iterate through all files in folder and subfolders
        sftp = ssh.open_sftp()
        for file_path in self._glob(sftp, self.folder):
            matches = self._match(file_path.name)

            # Count matches if there’s any non-empty text in any of the groups
            if "".join(matches).strip():
                n_matches += 1

            # Apply transformation mappings from self.match_map
            transformed_matches = []
            for i, value in enumerate(matches):
                transformed_value = self.match_map[i].get(value, value)
                transformed_matches.append(transformed_value)

            # Format the row as an SQL-compatible string
            row_string = "({values}, {file})".format(  # pylint: disable=consider-using-f-string # noqa: E501
                values=", ".join(repr(val) for val in transformed_matches),
                file=repr(str(file_path))
            )
            formatted_rows.append(row_string)

        # Log how many local matches were found
        logger.log(logging.INFO if formatted_rows else logging.WARNING,
                   "Table '%s'.'%s': Found %d local files matches.",
                   table.schema, table.name, n_matches)

        # Create table on Stage
        self.create_table(conn, formatted_rows, table)

        # Join tables based on matched columns only
        self.insert_file_locations(conn, self.match, table)

    def create_table(self, conn, rows, table):
        """Create LFS Location table.

        :param conn: SQLAlchemy connection/session.
        :type conn: sqlalchemy.Engine

        :param rows: List of formatted rows to be inserted as values.
        :type rows: list

        :param table: Table metadata
        :type table: TableMeta

        :return: None
        """
        # Table stage name
        stg_name = f"_{table.schema}.{table.name}_local_fs"

        # Drop table statement
        conn.execute(f"DROP TABLE IF EXISTS {stg_name}")

        # Create statement
        stmt = dedent("""
        CREATE TABLE {stg_name} AS
        SELECT
            {columns}
          , ARRAY_AGG({name}) AS {name}
        FROM ( VALUES
              {values}
             ) t ({columns}, {name})
        GROUP BY {columns}
        """).format(
            stg_name=stg_name,
            columns="\n  , ".join(self.match),
            values=',\n      '.join(rows),
            name=self.name
        )
        conn.execute(stmt)

    def add_column(self, conn, table):
        """Add local location column to table.

        :param conn: SQLAlchemy connection/session.
        :type conn: sqlalchemy.Engine

        :param table: Table metadata
        :type table: TableMeta

        :return: None
        """
        stmt = dedent("""
        ALTER TABLE {schema}.{name}
            ADD COLUMN {column} TEXT[]
            {notnull}
        """).format(
            schema=table.schema,
            name=table.name,
            column=self.name,
            notnull='NOT NULL' if self.required else ''
        )
        conn.execute(stmt)

    def insert_file_locations(self, conn, match_columns, table):
        """Add staged local file system column to loaded table.

        :return: str
        """
        stmt = dedent("""
        UPDATE {schema}.{table_name} ot
        SET {name} = fst.{name}
        FROM _{schema}.{stg_name} fst
        WHERE {cols_match}
        """).format(
            schema=table.schema,
            table_name=table.name,
            name=self.name,
            stg_name=f'{table.name}_local_fs',
            cols_match=" AND ".join(
                f"ot.{col} = fst.{col}"
                for col in match_columns
            )
        )
        conn.execute(stmt)
