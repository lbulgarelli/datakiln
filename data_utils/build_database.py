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
    session = Session.create(**db_params)

    # Create metadata
    database = DatabaseMeta(directory, db_params["database"])
    database.create_metadata(session)

    # Create role for database read access
    database.create_role(session)

    # Stage and flush to database
    updated = []
    for table in database.tables:
        if not table.is_updated(session):
            logging.info("Table '%s'.'%s': Already up to date.",
                         table.schema, table.name)
            continue
        table.create(session)
        table.stage_meta.load(session)
        updated.append(table)

    # Run transforms
    database.execute_directory("transform", session)

    # Load to final schema and flush to database
    for table in updated:
        table.enforce_constraints(session)
        table.load(session)
        table.create_indexes(session)
        table.create_metadata(session)
        session.commit()

    # Run views
    session.close()
    database.execute_directory("derive", session)

    # Grant read permissions on entire database
    database.grant(session, args.grant)
    session.commit()
    session.close()


if __name__ == "__main__":
    main()
