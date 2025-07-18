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
    conn = Session.create(**db_params)

    # Stage and flush to database
    table = TableMeta.from_json(meta_path, conn)
    table.create(conn)
    table.stage_meta.load(conn)
    conn.flush()

    # Load to final schema and commit changes
    table.enforce_constraints(conn)
    table.load(conn)
    table.create_indexes(conn)
    conn.commit()

    # Close connection
    conn.close()


if __name__ == "__main__":
    main()
