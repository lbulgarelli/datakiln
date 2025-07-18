"""
Creates table and loads data.
"""
import logging
from pathlib import Path

from data_utils.build_metadata import get_parser
from data_utils.metadata.database import DatabaseMeta, Session


logging.basicConfig(
    level=logging.INFO, datefmt="%Y-%m-%d %H:%M:%S",
    format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s")


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
    db_params = DatabaseMeta.load_env(directory)
    conn = Session.create(**db_params)

    # Create metadata
    database = DatabaseMeta(directory, db_params["database"])
    database.create_metadata(conn)
    conn.commit()

    # Create role for database read access
    database.create_role(conn)
    conn.commit()

    # Stage and flush to database
    for table in database.tables:
        table.create(conn)
        table.stage_meta.load(conn)
        conn.flush()

    # Run transforms
    database.execute_directory("transform", conn)

    # Load to final schema and flush to database
    for table in database.tables:
        table.enforce_constraints(conn)
        table.load(conn)
        table.create_indexes(conn)
        conn.flush()

    # Run views
    database.execute_directory("derive", conn)

    # Grant read permissions on entire database
    database.grant(conn, args.grant)

    # Commit changes
    conn.commit()


if __name__ == "__main__":
    main()
