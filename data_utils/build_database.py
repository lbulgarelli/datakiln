"""
Creates table and loads data.
"""
import json
import logging
from pathlib import Path

from data_utils.build_json import get_parser
from data_utils.build_table import load_env, connect, TableMeta


logging.basicConfig(
    level=logging.INFO, datefmt="%Y-%m-%d %H:%M:%S",
    format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s")


def main(args=None):
    """Creates table and loads data."""
    # Get arguments
    parser = get_parser()
    args = parser.parse_args(args)

    # Get absolute of specified path
    directory = Path(args.directory).absolute()

    # Connect to database
    db_params = load_env(directory)
    conn = connect(**db_params)

    # Iterate through all table metadata
    for meta_path in (directory / "metadata").glob("*.json"):
        # Load metadata
        with open(meta_path, encoding="utf-8") as file:
            meta_json = json.load(file)

        # Create table
        table_meta = TableMeta(**meta_json)
        table_meta.create(conn)

        # Load data
        data_path = (directory / "data").glob(f"{meta_path.stem}.*")
        table_meta.load(conn, next(data_path))

    # Commit changes
    conn.commit()


if __name__ == "__main__":
    main()
