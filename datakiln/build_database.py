"""
Creates table and loads data.
"""
import logging
from pathlib import Path

from datakiln.arguments import add_directory, add_include, add_log
from datakiln.metadata.database import DatabaseMeta, Session


logger = logging.getLogger(__name__)


def add_arguments(parser):
    """Adds the options of the command.

    :param parser: Parser of the command.
    :type parser: argparse.ArgumentParser
    """
    add_directory(parser)
    add_log(parser)
    add_include(parser, "Build only these tables and scripts, and "
                        "what they need.")
    parser.add_argument("--grant", metavar="grant", type=str, nargs='+',
                        help="Grant read permissions to specified users.")
    parser.add_argument("--rebuild-all", action="store_true",
                        help="Rebuild all tables and scripts, even if "
                             "up to date.")
    parser.add_argument("--prune", action="store_true",
                        help="Drop the objects removed from the repo.")


def run(args):
    """Creates the tables, loads the data and runs the scripts.

    :param args: Arguments of the command.
    :type args: argparse.Namespace
    """

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

        # Skip the tables that did not change, unless all are rebuilt. A table
        # is also rebuilt if a table it references is, since that drops its
        # foreign keys
        if (
            not args.rebuild_all
            and not table.needs_update(session)
            and database.graph.dependencies(table).isdisjoint(updated)
        ):
            logger.info("Table '%s'.'%s': Already up to date.",
                        table.schema, table.name)
            continue
        table.pull()
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
