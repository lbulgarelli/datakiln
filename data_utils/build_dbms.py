"""
Builds table metadata from DBMS
"""
import json
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
from data_utils.build_json import get_parser

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
    JOIN tmp_{table}_{uuid} {alias}
      ON {columns}
    """).format(
        table=table,
        alias=alias,
        uuid=UUID,
        columns="\n  AND ".join(
            f"src.{column} = {alias}.{reference}"
            for column, reference in zip(columns, reference["columns"])
        )
    )


def generate_table_data(conn, data, schema, table, filters):
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
    """
    logging.info("GENERATING TABLE DATA: %s.%s", schema, table)

    # Generate query with filters
    sql = dedent("""
    CREATE TABLE tmp_{table}_{uuid} AS
    SELECT src.*
    FROM {schema}.{table} src
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

    # Retrieve data:
    conn.execute(text(sql))
    data[schema][table] = pd.concat(pd.read_sql(dedent(f"""
    SELECT *
    FROM tmp_{table}_{UUID}
    """), conn, chunksize=10000), ignore_index=True)


def main(args=None):
    # pylint: disable=too-many-locals
    """Downloads data and creates sample jsons for tables."""
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
    parser.add_argument('--json', action='store_true', default=False,
                        help='Whether to export the JSON table metadata.')
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

    # Retrieve information
    for file in sorted(directory.glob("extract/*.json")):
        # Load extraction metadata
        extract_meta = json.load(file.open())

        # Create dict entry for schema
        if (schema := extract_meta["schema"]) not in data:
            data[schema] = {}

        # Retrieve data
        generate_table_data(conn, data, **extract_meta)

        # Create data directory if not exists
        data_dir = directory / "data"
        data_dir.mkdir(exist_ok=True)

        # Export to file
        table = extract_meta["table"]
        file_path = data_dir / f"{table}.csv.gz"
        data[schema][table].to_csv(
            file_path,
            encoding="utf-8",
            compression='gzip',
            index=False)  # Remove index for data_utils

    # Delete tables
    for schema, tables in data.items():
        for table in tables.keys():
            conn.execute(text(f"DROP TABLE tmp_{table}_{UUID}"))
    conn.execute(text(f"DROP TABLE tmp_cohort_{UUID}"))

    # Skip json metadata generation
    if not args.json:
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
