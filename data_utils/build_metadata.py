"""
Generate metadata from data file.
"""
import json
import logging
from pathlib import Path
from itertools import starmap
from functools import partial
from argparse import ArgumentParser

import pandas as pd
from slugify import slugify


logging.basicConfig(
    level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S',
    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s')

SUPPORTED_EXTENSIONS = {
    ".csv": partial(pd.read_csv, engine="python"),
    ".xlsx": partial(pd.read_excel, sheet_name=None)
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


def generate_table_meta(meta_dir, table_name, data):
    """Generates metadata for column from tuple.

    :param meta_dir: Medatada directory.
    :type meta_dir: Path

    :param table_name: Name of the table.
    :type table_name: str

    :param data: DataFrame containing sample data for the table.
    :type data: pandas.DataFrame

    :return: None
    """
    # Ensure table name is database friendly
    table_name = slugify(table_name, separator="_")

    # Remove trailing white spaces from data
    data = data.map(lambda x: x.strip() if isinstance(x, str) else x)

    # Generate metadata
    meta = {
        "name": table_name,
        "schema": "public",
        "encoding": "utf-8",
        "columns": list(starmap(dtype_to_meta, data.dtypes.items())),
        "constraints": {
            "primary_key": ["meta_row_id"]
        }
    }

    # Write metadata to file
    meta_path = meta_dir / f"{table_name}.json"
    with open(meta_path, "w", encoding="utf-8") as file:
        json.dump(meta, file, indent=4)


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
        logging.info("GENERATING METADATA: %s", data_path)

        # Read file
        data = SUPPORTED_EXTENSIONS[extension](data_path.absolute())

        # Excel files can have more than one sheet
        # so we handle the data as a dictionary
        if isinstance(data, pd.DataFrame):
            data = {data_path.stem: data}
        # Only consider tables with header row
        # when multiple tables are returned
        elif isinstance(data, dict):
            data = {
                name: df
                for name, df in data.items()
                if not df.columns.str.startswith("Unnamed:").all()
            }

            # If only one table is left, use file name
            if len(data) == 1:
                data = {data_path.stem: list(data.values())[0]}

        # Generate metadata
        for table_name, df in data.items():
            generate_table_meta(meta_dir, table_name, df)


if __name__ == "__main__":
    main()
