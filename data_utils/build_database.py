"""
Creates table and loads data.
"""
import logging
from pathlib import Path

from data_utils.build_metadata import get_parser, setup_logging
from data_utils.metadata.database import DatabaseMeta, Session


logger = logging.getLogger(__name__)


def main(args=None):
    """Creates table and loads data."""
    # Get arguments
    parser = get_parser()
    parser.add_argument("--grant", metavar="grant", type=str, nargs='+',
                        help="Grant read permissions to specified users")
    parser.add_argument("--rebuild-all", action="store_true",
                        help="Run all derive scripts, even if up to date")
    parser.add_argument("--prune", action="store_true",
                        help="Drop the objects removed from the repo")
    args = parser.parse_args(args)
    setup_logging(args.stdout, "build.log")

    # Get absolute of specified path
    directory = Path(args.directory).absolute()

    # Connect to database
    db_params = DatabaseMeta.load_env(directory)
    session = Session.create(**db_params)

    # Create metadata
    database = DatabaseMeta(directory, db_params["database"])
    database.create_metadata(session)
    database.load_stale(session)

    # Create role for database read access
    database.create_role(session)

    # Stage and flush to database
    updated = []
    for table in database.graph.tables:
        if not table.is_updated(session):
            logger.info("Table '%s'.'%s': Already up to date.",
                        table.schema, table.name)
            continue
        table.create(session)
        table.stage_meta.load(session)
        updated.append(table)

    # Run transforms
    database.execute_directory("transform", session, max_statements=0)

    # Load to final schema and flush to database
    for table in updated:
        table.resolve_columns(session)
        table.create_uniques(session)
        table.load(session)
        table.enforce_fks(session)
        table.create_indexes(session)
        table.create_metadata(session)
        database.forget_dependents(session, [table])
        session.commit()

    # Run views
    session.close()
    database.execute_derive(session, force=args.rebuild_all)

    # Objects whose metadata or script was removed
    database.prune(session, drop=args.prune)

    # Grant read permissions on entire database
    database.grant(session, args.grant)
    session.commit()
    session.close()


if __name__ == "__main__":
    main()
