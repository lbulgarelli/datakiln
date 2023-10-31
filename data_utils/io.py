"""
Helpers to read multiple data file extensions.
"""
import warnings
from io import BytesIO, TextIOWrapper

import pandas as pd


def open_excel(path, encoding):
    """
    Excel file reader.

    :param data_path: Path to file.
    :type data_path: str

    :param encoding: encoding of the data file.
    :type encoding: str

    :return: File buffer.
    :rtype: TextIOWrapper
    """
    buffer = TextIOWrapper(BytesIO(), encoding=encoding)
    with warnings.catch_warnings(record=True):
        warnings.simplefilter("always")
        buffer.writelines(pd.read_excel(path).to_csv(index=False))
    buffer.seek(0)
    return buffer
