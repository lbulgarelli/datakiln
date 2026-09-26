"""
Builds table metadata from DBMS.
"""
import logging
from argparse import BooleanOptionalAction
from getpass import getuser
from pathlib import Path

from data_utils.metadata.database import DatabaseMeta, DRIVER
from data_utils.build_metadata import get_parser, setup_logging
from data_utils.extraction.models import ExtractMeta, ExtractGraph
from data_utils.extraction.strategies import (
    StreamStrategy, TempTableStrategy, ParallelExecutor
)
from data_utils.extraction.exporters import MetadataExtractor


logger = logging.getLogger(__name__)


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
                        help='Username to use when connecting to the source '
                             'DBMS.')
    parser.add_argument('--password', metavar="password", type=str,
                        required=False,
                        help='Password to use when connecting to the source '
                             'DBMS.')
    parser.add_argument('--database', metavar="database", type=str,
                        required=True,
                        help='Database to connect to in the source DBMS.')
    parser.add_argument('--metadata', action='store_true', default=False,
                        help='Whether to export the JSON table metadata.')
    parser.add_argument('--data', action=BooleanOptionalAction, default=True,
                        help='Whether to extract data from the source DBMS.')
    parser.add_argument('--include', type=str, required=False, nargs='+',
                        help='Limit data extraction to the specified tables.')
    parser.add_argument('--read-only', action='store_true', default=False,
                        help='Run in read-only mode (no temp tables).')
    parser.add_argument('--parallel', type=int, default=4,
                        help='Number of parallel extraction jobs.')
    args = parser.parse_args(args)
    setup_logging(args.stdout, "extract.log")
    directory = Path(args.directory).absolute()

    # Create output directory
    meta_dir = Path(directory) / "metadata"
    meta_dir.mkdir(exist_ok=True)

    # Connect parameters
    db_params = {
        "dialect": args.dialect,
        "user": args.user,
        "password": args.password,
        "host": args.host,
        "database": args.database,
        "port": args.port
    }

    # Load all extraction metadata
    tasks = []
    for file in sorted(directory.glob("extract/*.json")):
        tasks.append(ExtractMeta.from_json(file))

    # Build dependency graph and sort
    graph = ExtractGraph(tasks)
    sorted_tasks = graph.sort()

    # Initialize execution strategy
    if args.read_only:
        strategy = StreamStrategy(db_params, directory)
    else:
        strategy = TempTableStrategy(db_params, directory)

    # Filtering tasks
    export_set = None
    execute_set = None
    if args.include:
        # Tasks to export
        export_set = set(args.include)
        invalid_tasks = export_set - set(graph.tasks.keys())
        if invalid_tasks:
            raise ValueError(
                "Specified table(s) not found in metadata: ",
                {', '.join(sorted(invalid_tasks))}
            )

        # Tasks to execute (export_set + dependencies)
        execute_set = set()
        queue = list(export_set)
        while queue:
            curr = queue.pop(0)
            if curr not in execute_set:
                execute_set.add(curr)
                if curr in graph.tasks:
                    queue.extend(graph.tasks[curr].dependencies())

    # Track exported tables for metadata generation
    exported_schemas = {}
    for task in sorted_tasks:
        if export_set is None or task.name in export_set:
            if task.schema not in exported_schemas:
                exported_schemas[task.schema] = []
            exported_schemas[task.schema].append(task.table)

    if args.data:
        # Create data directory if not exists
        data_dir = directory / "data"
        data_dir.mkdir(exist_ok=True)

        # Initialize and run parallel executor
        executor = ParallelExecutor(strategy, graph, max_workers=args.parallel)
        try:
            executor.execute(
                execute=execute_set,
                export=export_set,
                data_dir=data_dir
            )
        finally:
            strategy.cleanup()

    # Skip metadata generation
    if not args.metadata:
        return

    # Get target database params
    db_params = DatabaseMeta.load_env(directory)

    # Generate table metadata
    with strategy.session() as session:
        for schema, tables in exported_schemas.items():
            # Retrieve metadata from DBMS
            metadata = MetadataExtractor.get_metadata(session, schema, tables)

            for _, table in metadata.tables.items():
                # Generate table metadata
                logger.info("GENERATING TABLE METADATA: %s", table)
                MetadataExtractor.generate_table_meta(
                    meta_dir,
                    table,
                    db_params["dialect"]
                )


if __name__ == "__main__":
    main()
