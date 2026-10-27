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
    parser.add_argument("--include", metavar="name", type=str, nargs="+",
                        help="Build only these tables and scripts, and what "
                             "they use")
    parser.add_argument("--rebuild-all", action="store_true",
                        help="Run all derive scripts, even if up to date")
    parser.add_argument("--prune", action="store_true",
                        help="Drop the objects removed from the repo")
    args = parser.parse_args(args)
    setup_logging(args.stdout, "build.log")

    # Get absolute of specified path
    directory = Path(args.directory).absolute()

    # Get database parameters
    db_params = DatabaseMeta.load_env(directory)

    # Tables and scripts to build, with what they use
    database = DatabaseMeta(directory, db_params["database"])
    include = None
    if args.include:
        selected = database.graph.find(args.include)
        include = database.graph.requirements(selected)

    # Connect to database
    session = Session.create(**db_params)

    # Create metadata
    database.create_metadata(session)
    database.load_stale(session)

    # Create role for database read access
    database.create_role(session)

    # Stage and flush to database
    updated = []
    for table in database.graph.tables:
        if include is not None and table not in include:
            continue

        # Rebuilding a table drops the foreign keys of the tables that
        # reference it, which have to be rebuilt as well to get them back
        if (
            not table.is_updated(session)
            and database.graph.adj[table].isdisjoint(updated)
        ):
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
    database.execute_derive(
        session, force=args.rebuild_all, include=include)

    # Objects whose metadata or script was removed
    database.prune(session, drop=args.prune)

    # Grant read permissions on entire database
    database.grant(session, args.grant)
    session.commit()
    session.close()


if __name__ == "__main__":
    main()
