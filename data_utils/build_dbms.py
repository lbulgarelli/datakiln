"""
Builds table metadata from DBMS
"""
import json
import gzip
import logging
from uuid import uuid4
from pathlib import Path
from textwrap import dedent
from getpass import getpass, getuser

import pandas as pd
from sqlalchemy import MetaData, URL
from sqlalchemy import create_engine, text
from sqlalchemy.dialects import registry

from data_utils.build_table import load_env
from data_utils.build_metadata import get_parser

logging.basicConfig(
    level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S',
    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s')

DRIVER = {
    "postgresql": "psycopg2",
    "oracle": "oracledb"
}

# Generate an UUID for the run
UUID = str(uuid4()).rsplit("-", maxsplit=1)[-1]


def get_metadata(conn, schema, tables):
    """Generate column metadata from DBMS.

    :param conn: SQLAlchemy connection or session.
    :type conn: sqlalchemy.Engine

    :param schema: Name of the schema in the source DBMS.
    :type schema: str

    :param tables: Name of the tables in the source DBMS.
    :type tables: list[str]

    :return: Schema metatada.
    :rtype: sqlalchemy.sql.schema.MetaData
    """
    logging.info("RETRIEVING SCHEMA METADATA: %s", schema)
    metadata = MetaData(schema=schema)
    metadata.reflect(bind=conn, only=tables)

    return metadata


def generate_column_meta(column, dialect=None):
    """Generate column metadata from DBMS.

    :param column: Column metadata.
    :type column: sqlalchemy.sql.schema.Column

    :param dialect: Dialect of the source DBMS.
    :type dialect: str

    :return: Column metatada.
    :rtype: dict
    """
    # Data type conversion
    dialect_obj = registry.load(dialect)
    dialect_type = column.type.dialect_impl(dialect_obj())

    return {
        "name": column.name,
        "dtype": {
            dialect: dialect_type.compile()
        }
    }


def generate_table_meta(meta_dir, table, dialect=None):
    """Generate table metadata from DBMS.

    :param meta_dir: Medatada directory.
    :type meta_dir: Path

    :param table: Table metadata.
    :type table: sqlalchemy.sql.schema.Table

    :param dialect: Dialect of the source DBMS.
    :type dialect: str

    :return: Table metadata.
    :rtype: dict
    """
    meta = {
        "name": table.name,
        "schema": "public",
        "encoding": "utf-8",
        "columns": [
            generate_column_meta(column, dialect)
            for column in table.columns
        ],
        "constraints": {
            "primary_key": [
                column.name
                for column in table.primary_key
            ]
        }
    }

    # Write metadata to file
    meta_path = meta_dir / f"{table.name}.json"
    with open(meta_path, "w", encoding="utf-8") as file:
        json.dump(meta, file, indent=4)


def generate_filter(columns, reference, alias=None, cohort=False):
    """Generate table metadata from DBMS.

    :param columns: Columns used to filter.
    :type columns: list[str]

    :param reference: Columns used to filter in the reference table.
    :type reference: dict

    :param alias: Alias to use for the filtering table.
    :type alias: str

    :return: .
    :rtype: str
    """
    # Handle table name
    table = "cohort" if cohort else reference["table"]

    # Generate IN expressions
    return dedent("""
    OR EXISTS (
      SELECT 1
      FROM tmp_{table}_{uuid} {alias}
      WHERE 1 = 1
      AND {columns}
    )
    """).format(
        table=table,
        alias=alias,
        uuid=UUID,
        columns="\n  AND ".join(
            f"src.{column} = {alias}.{reference}"
            for column, reference in zip(columns, reference["columns"])
        )
    )


def generate_table_data(conn, data, schema, table, filters, export=True):
    # pylint: disable=too-many-arguments, too-many-positional-arguments
    """Generate table metadata from DBMS.

    :param conn: SQLAlchemy connection or session.
    :type conn: sqlalchemy.Engine

    :param data: Data of retrieved tables.
    :type data: pandas.DataFrame

    :param schema: Name of the schema in the source DBMS.
    :type schema: str

    :param table: Name of the table in the source DBMS.
    :type table: str

    :param filters: Metadata for filters.
    :type filters: dict

    :param export: Is the table going to be exported?
    :type export: bool
    """
    logging.info("GENERATING TABLE DATA: %s.%s", schema, table)

    # Generate query with filters
    sql = dedent("""
    CREATE TABLE tmp_{table}_{uuid} AS
    SELECT src.*
    FROM {schema}.{table} src
    WHERE 0 = 1
    {filters}
    """).format(
        schema=schema,
        table=table,
        uuid=UUID,
        filters="".join(
            generate_filter(alias=f"f_{idx}", **filter)[1:]
            for idx, filter in enumerate(filters)
        )
    )
    conn.execute(text(sql))

    # Skip retrieval if its not going to be exported
    if not export:
        data[schema][table] = []
        return

    # Retrieve data
    data[schema][table] = pd.read_sql(dedent(f"""
    SELECT *
    FROM tmp_{table}_{UUID}
    """), conn, chunksize=10000)


def main(args=None):
    # pylint: disable=too-many-locals, too-many-branches, too-many-statements
    """Downloads data and creates sample metadata for tables."""
    # Arguments
    parser = get_parser()
    parser.add_argument('--dialect', metavar="dialect", type=str,
                        required=True, choices=DRIVER.keys(),
                        help='Dialect of the source DBMS.')
    parser.add_argument('--host', metavar="host", type=str,
                        required=True, help='Hostname of the source DBMS.')
    parser.add_argument('--port', metavar="port", type=str,
                        required=True,
                        help='Port to use when connecting to the source DBMS.')
    parser.add_argument('--user', metavar="user", type=str,
                        required=False, default=getuser(),
                        help='Username to use when connecting to the source'
                             'DBMS.')
    parser.add_argument('--password', metavar="password", type=str,
                        required=False,
                        help='Password to use when connecting to the source'
                             'DBMS.')
    parser.add_argument('--database', metavar="database", type=str,
                        required=True,
                        help='Database to connect to in the source DBMS.')
    parser.add_argument('--metadata', action='store_true', default=False,
                        help='Whether to export the JSON table metadata.')
    parser.add_argument('--include', type=str, required=False, nargs='+',
                        help='Limit data extraction to the specified tables.')
    args = parser.parse_args(args)
    directory = Path(args.directory).absolute()

    # Create output directory
    meta_dir = Path(directory) / "metadata"
    meta_dir.mkdir(exist_ok=True)

    # Connect to source database
    conn = create_engine(URL.create(
        f"{args.dialect}+{DRIVER[args.dialect]}",
        username=args.user,
        password=args.password or getpass(),
        host=args.host,
        database=args.database,
    )).connect()
    conn.execution_options(stream_results=True)

    # Retrieve cohort data
    data = {}
    logging.info("GENERATING COHORT DATA")
    with open(directory / 'extract' / 'cohort.sql', encoding="utf-8") as file:
        conn.execute(text(dedent("""
        CREATE TABLE tmp_cohort_{uuid} AS
        {query}
        """).format(
            uuid=UUID,
            query=file.read()
        )))

    # Create data directory if not exists
    data_dir = directory / "data"
    data_dir.mkdir(exist_ok=True)

    # Define which tables to include
    included = []
    dependencies = set()
    for file in sorted(directory.glob("extract/*.json"), reverse=True):
        # Load extraction metadata
        extract_meta = json.load(file.open())
        table = f"{extract_meta['schema']}.{extract_meta['table']}"

        # We include all tables if none are specified
        if args.include is None:
            included.insert(0, extract_meta)
        # If table is specified, include it
        # Any tables used in filtering need to be included as well
        elif table in set(args.include) | dependencies:
            included.insert(0, extract_meta)
            for table_filter in extract_meta["filters"]:
                # Skip cohort filter
                if table_filter.get("cohort", False):
                    continue
                reference = table_filter["reference"]
                dependencies.add(f"{reference['schema']}.{reference['table']}")

    # Retrieve information
    for extract_meta in included:
        # Create dict entry for schema
        if (schema := extract_meta["schema"]) not in data:
            data[schema] = {}

        # Retrieve data
        table = f"{extract_meta['schema']}.{extract_meta['table']}"
        export = table in args.include
        generate_table_data(conn, data, **extract_meta, export=export)

        # Skip export if empty
        table = extract_meta["table"]
        if not data[schema][table]:
            continue

        # Export to file
        first_chunk = True
        file_path = data_dir / f"{table}.csv.gz"
        logging.info("EXPORTING TABLE DATA: %s.%s", schema, table)
        with gzip.open(file_path, mode='wt', encoding='utf-8') as o_file:
            for chunk in data[schema][table]:
                chunk.to_csv(
                    o_file,
                    header=first_chunk,
                    encoding="utf-8",
                    index=False)
                first_chunk = False

    # Drop tables
    for schema, tables in data.items():
        for table in list(tables.keys()):
            conn.execute(text(f"DROP TABLE tmp_{table}_{UUID}"))

            # We remove tables from dict if they are not included
            if f"{schema}.{table}" not in args.include:
                tables.pop(table)

    # Drop cohort table
    conn.execute(text(f"DROP TABLE tmp_cohort_{UUID}"))

    # Skip metadata generation
    if not args.metadata:
        return

    # Get target database params
    db_params = load_env(directory)

    for schema, tables in data.items():
        # Retrieve metadata from DBMS
        metadata = get_metadata(conn, schema, tables)

        for _, table in metadata.tables.items():
            # Generate table metadata
            logging.info("GENERATING TABLE METADATA: %s", table)
            generate_table_meta(meta_dir, table, db_params["dialect"])

    # Close connection
    conn.close()


if __name__ == "__main__":
    main()
