"""
Creates table and loads data.
"""
import logging
from pathlib import Path

from data_utils.build_metadata import get_parser
from data_utils.build_table import TableMeta
from data_utils.metadata.database import Database, Session


logging.basicConfig(
    level=logging.INFO, datefmt="%Y-%m-%d %H:%M:%S",
    format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s")


def execute_directory(directory, conn, ignore):
    """Executes SQL scripts in a given directory.

    :param directory: Path for metadata directory.
    :type directory: str

    :param conn: SQLAlchemy connection or session.
    :type conn: sqlalchemy.Engine

    :param ignore: List of files to ignore.
    :type ignore: str
    """
    for file_name in sorted(directory.glob("**/*.sql")):
        file_relative = file_name.relative_to(directory.parent)
        if str(file_relative) in ignore:
            continue
        logging.info("Script '%s': RUNNING",
                     file_name.relative_to(directory.parent))

        # Load file
        with open(file_name, "r", encoding="utf-8") as file:
            sql = file.read()

        # Execute query
        conn.execute(sql)

        # Flush to database
        conn.flush()


def main(args=None):
    """Creates table and loads data."""
    # Get arguments
    parser = get_parser()
    parser.add_argument("--grant", metavar="grant", type=str, nargs='+',
                        help="Grant read permissions to specified users")
    args = parser.parse_args(args)

    # Get absolute of specified path
    directory = Path(args.directory).absolute()

    # Connect to database
    db_params = Database.load_env(directory)
    conn = Session.create(**db_params)

    # Create metadata
    database = Database(directory, db_params["database"])
    database.create_metadata(conn)
    conn.commit()

    # Create role for database read access
    database.create_role(conn)
    conn.commit()

    # Retrieve files to ignore
    ignore = []
    if (ignore_file := directory / ".dataignore").exists():
        with ignore_file.open("r", encoding="utf-8", errors="ignore") as file:
            ignore = file.read().splitlines()

    # Iterate through all table metadata
    tables_meta = []
    for meta_path in sorted((directory / "metadata").glob("*.json")):
        meta_relative = meta_path.relative_to(directory)
        if str(meta_relative) in ignore:
            continue

        # Stage and flush to database
        tables_meta.append(TableMeta.stage(meta_path, conn))
        conn.flush()

    # Run transforms
    execute_directory(directory / "transform", conn, ignore)

    # Load to final schema and flush to database
    for table_meta in tables_meta:
        table_meta.enforce_constraints(conn)
        table_meta.load(conn)
        table_meta.create_indexes(conn)
        conn.flush()

    # Run views
    execute_directory(directory / "derive", conn, ignore)

    # Grant read permissions on entire database
    database.grant(conn, args.grant)

    # Commit changes
    conn.commit()


if __name__ == "__main__":
    main()
