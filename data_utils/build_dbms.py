"""
Builds table metadata from DBMS
"""
import json
import logging
from pathlib import Path
from textwrap import dedent
from getpass import getpass, getuser

import pandas as pd
from sqlalchemy import MetaData, URL
from sqlalchemy.engine import create_engine
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


def generate_filter(data, columns, reference, cohort=False):
    """Generate table metadata from DBMS.

    :param data: Data of retrieved tables.
    :type data: pandas.DataFrame

    :param columns: Columns used to filter.
    :type columns: list[str]

    :param reference: Columns used to filter in the reference table.
    :type reference: dict

    :param cohort: Use cohort query to filter results.
    :type cohort: bool

    :return: .
    :rtype: str
    """
    # Determine values to use in filter
    if not cohort:
        filter_data = data[reference["schema"]][reference["table"]]
    else:
        filter_data = data["cohort"]

    # Adjust for single column
    if len(columns) == 1:
        columns.append("0")
        reference["columns"].append(0)
        filter_data[0] = 0

    # Generate IN expressions
    return dedent("""
    ({columns}) IN (
        {values}
    )
    """).format(
        columns=", ".join(columns),
        values="\n  , ".join([
            str(tuple(row[reference["columns"]].fillna('').values))
            for _, row in filter_data.iterrows()
        ])
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
    SELECT *
    FROM {schema}.{table}
    WHERE 1 = 1
    AND {filters}
    """).format(
        schema=schema,
        table=table,
        filters="\nOR ".join(
            generate_filter(data, **filter)[1:]
            for filter in filters
        )
    )

    # Retrieve data
    data[schema][table] = pd.read_sql(sql, conn)


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

    # Retrieve cohort data
    data = {}
    logging.info("GENERATING COHORT DATA")
    with open(directory / 'extract' / 'cohort.sql', encoding="utf-8") as file:
        data["cohort"] = pd.read_sql(file.read(), conn)

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
        file_path = data_dir / f"{table}.csv"
        data[schema][table].to_csv(
            file_path,
            encoding="utf-8",
            index=False)  # Remove index for data_utils

    # Skip json metadata generation
    if not args.json:
        return

    # Get target database params
    db_params = load_env(directory)

    _ = data.pop("cohort")
    for schema, tables in data.items():
        # Retrieve metadata from DBMS
        metadata = get_metadata(conn, schema, tables)

        for _, table in metadata.tables.items():
            # Generate table metadata
            logging.info("GENERATING TABLE METADATA: %s", table)
            generate_table_meta(meta_dir, table, db_params["dialect"])


if __name__ == "__main__":
    main()
