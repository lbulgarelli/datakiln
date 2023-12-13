"""
Creates table and loads data.
"""
import logging
from pathlib import Path

from sqlalchemy import text

from data_utils.build_json import get_parser
from data_utils.build_table import load_env, connect, build


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
    for meta_path in sorted((directory / "metadata").glob("*.json")):
        # Build and flush to database
        build(meta_path, conn)
        conn.flush()

    # Run views
    for file_name in sorted((directory / "views").glob("*.sql")):
        logging.info("Script '%s': RUNNING", file_name.stem)

        # Load file
        with open(file_name, "r", encoding="utf-8") as file:
            sql = text(file.read())

        # Execute query
        conn.execute(sql)

        # Flush to database
        conn.flush()

    # Commit changes
    conn.commit()


if __name__ == "__main__":
    main()
