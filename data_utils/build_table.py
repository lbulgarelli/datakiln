"""
Creates table and loads data.
"""
from pathlib import Path

from data_utils.build_metadata import get_parser
from data_utils.metadata.database import load_env, connect
from data_utils.metadata.table import TableMeta


def main(args=None):
    """Creates table and loads data."""
    # Get arguments
    parser = get_parser()
    args = parser.parse_args(args)

    # Get absolute of specified path
    meta_path = Path(args.directory).absolute()

    # Connect to database
    db_params = load_env(meta_path.parents[1])
    conn = connect(**db_params)

    # Load to final schema and commit changes
    table_meta = TableMeta.stage(meta_path, conn)
    table_meta.enforce_constraints(conn)
    table_meta.load(conn)
    table_meta.create_indexes(conn)
    conn.commit()

    # Close connection
    conn.close()


if __name__ == "__main__":
    main()
