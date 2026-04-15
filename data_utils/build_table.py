"""
Creates table and loads data.
"""
from pathlib import Path

from data_utils.build_metadata import get_parser
from data_utils.metadata.database import Session
from data_utils.metadata.table import TableMeta


def main(args=None):
    """Creates table and loads data."""
    # Get arguments
    parser = get_parser()
    args = parser.parse_args(args)

    # Get absolute of specified path
    meta_path = Path(args.directory).absolute()

    # Connect to database
    db_params = Session.load_env(meta_path.parents[1])
    session = Session.create(**db_params)

    # Stage and flush to database
    table = TableMeta.from_json(meta_path, session)
    table.create(session)
    table.stage_meta.load(session)
    session.flush()

    # Load to final schema and commit changes
    table.resolve_columns(session)
    table.create_uniques(session)
    table.load(session)
    table.enforce_fks(session)
    table.create_indexes(session)
    session.commit()

    # Close connection
    session.close()


if __name__ == "__main__":
    main()
