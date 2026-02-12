"""
Builds table metadata from DBMS
"""
import re
import csv
import json
import gzip
import logging
from uuid import uuid4
from hashlib import md5
from pathlib import Path
from textwrap import dedent
from itertools import repeat
from getpass import getpass, getuser

import pandas as pd
from sqlalchemy import inspect, MetaData
from sqlalchemy.dialects import registry

from data_utils.metadata.database import DatabaseMeta, Session
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


def get_metadata(session, schema, tables):
    """Generate column metadata from DBMS.

    :param session: Database Session.
    :type session: data_utils.metadata.database.Session

    :param schema: Name of the schema in the source DBMS.
    :type schema: str

    :param tables: Name of the tables in the source DBMS.
    :type tables: list[str]

    :return: Schema metatada.
    :rtype: sqlalchemy.sql.schema.MetaData
    """
    logging.info("RETRIEVING SCHEMA METADATA: %s", schema)
    metadata = MetaData(schema=schema)
    metadata.reflect(bind=session.bind, only=tables, views=True)

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


def retrieve_cohort(session, directory, cohort):
    # pylint: disable=too-many-locals
    """Retrieve cohort.

    :param session: Database Session.
    :type session: data_utils.metadata.database.Session

    :param directory: Path for metadata directory.
    :type directory: str

    :param cohort: Name of cohort table or name of cohort file.
    :type cohort: str
    """
    logging.info("GENERATING COHORT DATA: %s", cohort)

    # If a table is specified, we copy it over
    if not cohort.endswith(".sql"):
        # Connect to source (build) database
        db_params = DatabaseMeta.load_env(directory)
        engine = Session.create(**db_params).bind
        schema, table = cohort.split(".")

        # Retrieve data types
        dtype = {}
        inspector = inspect(engine)
        for col_info in inspector.get_columns(table, schema=schema):
            col_name = col_info["name"]
            col_type = col_info["type"]
            dialect_obj = registry.load(session.bind.dialect.name)
            dialect_type = col_type.dialect_impl(dialect_obj())
            dtype[col_name] = dialect_type

        # Transfer cohort data
        name = cohort.replace(".", "__")
        name = f'tmp_{name}_{UUID}'
        cohort = pd.read_sql_table(table, engine, schema)
        cohort.to_sql(name, session.bind, index=False,
                      dtype=dtype)
        return name

    # Otherwise we create it from the cohort SQL file
    name = cohort.replace(".sql", "")
    name = f'tmp_{name}_{UUID}'
    with open(directory / 'extract' / cohort, encoding="utf-8") as file:
        session.execute(dedent("""
        CREATE TABLE {name} AS
        {query}
        """).format(
            name=name,
            uuid=UUID,
            query=file.read()
        ))
        session.commit()
    return name


def alias_column(column, default):
    """Impute table alias for column reference.

    :param column: Current column reference.
    :type column: str

    :param default: Default alias to impute.
    :type default: str

    :return: Column reference.
    :rtype: str
    """
    # If already aliased, skip
    if "." in column:
        return column

    # Curly brackets delimit literal values
    if value := re.search(r'\{(.*?)\}', column):
        return value.group(1)

    return f"{default}.{column}"


def generate_join(session, columns, reference):
    """Generate table metadata from DBMS.

    :param session: Database Session.
    :type session: data_utils.metadata.database.Session

    :param columns: Columns used to filter.
    :type columns: list[str]

    :param reference: Columns used to filter in the reference table.
    :type reference: dict

    :return: JOIN expression for filter query.
    :rtype: str
    """
    table = f"tmp_{reference['table']}_{UUID}"

    # When table is not extracted, use source
    if not inspect(session.bind).has_table(table):
        table = f"{reference['schema']}.{reference['table']}"

    # Generate EXISTS subqueries
    return dedent("""
    JOIN {table} {alias}
      ON {columns}
    """).format(
        table=table,
        alias=reference["alias"],
        columns="\n  AND ".join(
            f"{alias_column(col, 'ref')} {op} {alias}.{ref}"
            for col, op, alias, ref in zip(
                columns,
                reference.get("operators", repeat("=")),
                repeat(reference.get("alias", "ref")),
                reference["columns"]
            )
        )
    ).strip()


def columns_indexed(session, table, columns):
    """Evaluate the existence of a index with the specified columns.

    :param session: Database Session.
    :type session: data_utils.metadata.database.Session

    :param table: Name of the table in the source DBMS.
    :type table: str

    :param columns: Columns in the index.
    :type columns: list[str]

    :return: Does the index exist?
    :rtype: bool
    """
    insp = inspect(session.bind)
    for idx in insp.get_indexes(table):
        if set(idx["column_names"]) == set(columns):
            return True
    return False


def resolve_columns(columns, join):
    """Resolve columns to use in database index when using JOIN clauses.

    :param columns: Columns to resolve.
    :type columns: list[str]

    :param join: Metadata for JOIN clauses.
    :type join: str

    :return: Columns to use in index.
    :rtype: list[str]
    """
    resolved = []
    for column in columns:
        # Skip duplicates and literal values
        if column in resolved or re.search(r'\{(.*?)\}', column):
            continue

        # Add columns without alias directly
        if "." not in column:
            resolved.append(column)
            continue

        # Resolved aliased columns
        alias, column = column.split(".", 1)
        for joined in join:
            if joined["reference"]["alias"] != alias:
                continue

            more = resolve_columns(joined["columns"], join)
            if not set(more) <= set(resolved):
                resolved.extend(more)

    return resolved


def generate_filter(session, columns, reference, cohort=None, join=None):
    """Generate table metadata from DBMS.

    :param session: Database Session.
    :type session: data_utils.metadata.database.Session

    :param columns: Columns used to filter.
    :type columns: list[str]

    :param reference: Columns used to filter in the reference table.
    :type reference: dict

    :param cohort: Name of cohort table or name of cohort file.
    :type cohort: str

    :param join: Metadata for JOIN clauses.
    :type join: str

    :return: Cohort filter query.
    :rtype: str
    """
    # Handle table name and cohort filter
    if cohort is None:
        table = reference["table"]
    elif cohort.endswith(".sql"):
        table = cohort[:-4]
    else:
        table = cohort.replace(".", "__")
    table = f"tmp_{table}_{UUID}"

    # The list may include columns from joined tables, so we
    # retrieve joined columns to create the appropriate index
    ref_columns = resolve_columns(reference["columns"], join)

    # Generate hash for column list
    columns_hash = md5("_".join(ref_columns).encode()).hexdigest()  # noqa: DUO130, E501

    # Create index to speed-up filter
    if not columns_indexed(session, table, ref_columns):
        sql = dedent("""
        CREATE INDEX {table}_idx_{hash} ON {table} ({columns})
        """).format(
            table=table,
            hash=columns_hash,
            columns=", ".join(ref_columns)
        )
        session.execute(sql)

    # Generate EXISTS subqueries
    return dedent("""
    SELECT 1
    FROM {table} ref
    {join}
    WHERE 1 = 1
    AND {columns}
    """).format(
        table=table,
        join="\n".join(
            generate_join(session, **join_meta)
            for join_meta in join or []
        ),
        columns="\nAND ".join(
            f"{alias_column(col, 'src')} {op} {alias_column(ref, 'ref')}"
            for col, op, ref in zip(
                columns,
                reference.get("operators", repeat("=")),
                reference["columns"]
            )
        )
    ).split("\n")


def generate_table_data(session, schema, table, filters, export_to=None):
    # pylint: disable=too-many-arguments, too-many-positional-arguments
    """Generate table metadata from DBMS.

    :param session: Database Session.
    :type session: data_utils.metadata.database.Session

    :param schema: Name of the schema in the source DBMS.
    :type schema: str

    :param table: Name of the table in the source DBMS.
    :type table: str

    :param filters: Metadata for filters.
    :type filters: dict

    :param export_to: Export data to this folder.
    :type export_to: str
    """
    logging.info("GENERATING TABLE DATA: %s.%s", schema, table)

    # Generate query with filters
    sql = dedent("""
    CREATE TABLE tmp_{table}_{uuid} AS
    SELECT src.*
    FROM {schema}.{table} src
    WHERE 1 = 1
    AND EXISTS (
      {filters}
    )
    """).format(
        schema=schema,
        table=table,
        uuid=UUID,
        filters="\n  UNION ALL\n".join("\n  ".join(
            generate_filter(session, **filter))
            for filter in filters
        )
    )
    session.execute(sql)
    session.commit()

    # Skip retrieval if its not going to be exported
    if not export_to:
        return

    file_path = export_to / f"{table}.csv.gz"
    with gzip.open(file_path, mode='wt', encoding='utf-8') as o_file:
        logging.info("EXPORTING TABLE DATA: %s.%s", schema, table)

        # Execute query
        result = session.execute(
            dedent(f"""
            SELECT *
            FROM tmp_{table}_{UUID}
            """),
            execution_options={
                "stream_results": True
            }
        )

        # Retrieve header
        header = list(result.keys())

        # In case we are extracted from a versioned database
        try:
            idx = header.index("meta_row_id")
            header.pop(idx)
        except ValueError:
            idx = len(header)

        # Prepare to export as CSV
        writer = csv.writer(o_file)
        writer.writerow(header)

        # Export to file
        for row in result:
            writer.writerow(row[:idx] + row[idx+1:])


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
    session = Session.create(
        dialect=args.dialect,
        user=args.user,
        password=args.password or getpass(),
        host=args.host,
        database=args.database,
        port=args.port
    )

    # Create data directory if not exists
    data_dir = directory / "data"
    data_dir.mkdir(exist_ok=True)

    # Define which tables to create
    create = []
    dependencies = set()
    for file in sorted(directory.glob("extract/*.json"), reverse=True):
        # Load extraction metadata
        extract_meta = json.load(file.open())
        table = f"{extract_meta['schema']}.{extract_meta['table']}"

        # We include all tables if none are specified
        if args.include is None:
            create.insert(0, extract_meta)
        # If table is specified, include it
        # Any tables used in filtering need to be included as well
        elif table in set(args.include) | dependencies:
            create.insert(0, extract_meta)
            for table_filter in extract_meta["filters"]:
                # Skip cohort filter
                if table_filter.get("cohort", False):
                    continue
                reference = table_filter["reference"]
                dependencies.add(f"{reference['schema']}.{reference['table']}")

    # Retrieve cohort information
    cohorts = {}
    for extract_meta in create:
        for filter_ in extract_meta["filters"]:
            cohort = filter_.get("cohort", None)
            if cohort in list(cohorts) + [None]:
                continue
            cohort_name = retrieve_cohort(session, directory, cohort)
            cohorts[cohort] = cohort_name

    # Retrieve information
    exported = {}
    for extract_meta in create:
        # We only export the specified tables
        table = f"{extract_meta['schema']}.{extract_meta['table']}"
        if args.include is None or table in args.include:
            export_to = data_dir
        else:
            export_to = None

        # Retrieve data
        generate_table_data(session, **extract_meta, export_to=export_to)

        # Create list of exported tables
        if not export_to:
            continue
        if (schema := extract_meta["schema"]) not in exported:
            exported[schema] = []
        exported[schema].append(extract_meta["table"])

    # Drop tables
    for extract_meta in create:
        table = extract_meta['table']
        session.execute(f"DROP TABLE tmp_{table}_{UUID}")

    # Drop cohort table
    for cohort in cohorts.values():
        session.execute(f"DROP TABLE {cohort}")

    # Skip metadata generation
    if not args.metadata:
        return

    # Get target database params
    db_params = DatabaseMeta.load_env(directory)

    # Generate table metadata
    for schema, tables in exported.items():
        # Retrieve metadata from DBMS
        metadata = get_metadata(session, schema, tables)

        for _, table in metadata.tables.items():
            # Generate table metadata
            logging.info("GENERATING TABLE METADATA: %s", table)
            generate_table_meta(meta_dir, table, db_params["dialect"])

    # Close connection
    session.close()


if __name__ == "__main__":
    main()
