"""
Generate metadata from data file.
"""
import json
from pathlib import Path
from itertools import starmap
from argparse import ArgumentParser

import pandas as pd
from slugify import slugify


SUPPORTED_EXTENSIONS = {
    ".csv": pd.read_csv,
    ".xlsx": pd.read_excel
}


def dtype_to_psql(dtype):
    """Define PostgreSQL type from pandas inferred type.

    :param dtype: Type inferred by pandas.
    :type dtype: type

    :return: PostgreSQL data type.
    :rtype: str
    """
    if dtype == object:
        return "TEXT"
    if dtype == int:
        return "INTEGER"
    if dtype == float:
        return "DOUBLE PRECISION"
    if dtype == bool:
        return "BOOLEAN"
    return None


def get_parser():
    """Generates default parser.

    :return: Argument parser.
    :rtype: argparse.ArgumentParser
    """
    parser = ArgumentParser()
    parser.add_argument("directory", metavar="directory", type=str,
                        help="""Path of the directory with the dataset
                        metadata.""")

    return parser


def dtype_to_meta(column, dtype):
    """Generates metadata for column from tuple.

    :param column: Column name as it is in data file.
    :type column: str

    :param dtype: Type inferred by pandas.
    :type dtype: type

    :return: Column metadata.
    :rtype: dict
    """
    return {
        "name": column,
        "rename": slugify(column, separator="_"),
        "dtype": {
            "postgresql": dtype_to_psql(dtype)
        }
    }


def main(args=None):
    """Generates metadata from data file."""
    # Get arguments
    parser = get_parser()
    args = parser.parse_args(args)

    # Get absolute path for specified directory
    directory = Path(args.directory).absolute()

    # Ensure metadata directory exists
    meta_dir = directory / "metadata"
    meta_dir.mkdir(exist_ok=True)

    # Generate metadata for each data file
    for data_path in directory.glob("data/*.*"):
        # Skip if the extension is not supported
        extension = data_path.suffix.lower()
        if extension not in SUPPORTED_EXTENSIONS:
            continue

        # Read file
        data = SUPPORTED_EXTENSIONS[extension](data_path.absolute())

        # Remove trailing white spaces from data
        data = data.map(lambda x: x.strip() if isinstance(x, str) else x)

        # Generate table name from file
        table_name = slugify(data_path.stem, separator="_")

        # Generate metadata
        meta = {
            "name": table_name,
            "schema": "public",
            "columns": list(starmap(dtype_to_meta, data.dtypes.items())),
            "constraints": {
                "primary_key": [""]
            }
        }

        # Write metadata to file
        meta_path = meta_dir / f"{table_name}.json"
        with open(meta_path, "w", encoding="utf-8") as file:
            json.dump(meta, file, indent=4)


if __name__ == "__main__":
    main()
