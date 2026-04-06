"""
Helpers to read multiple data file extensions.
"""
import warnings
from io import BytesIO, TextIOWrapper, IOBase

import pandas as pd


class NullStripper(IOBase):
    """
    Remove NULL characters
    """
    def __init__(self, target):
        self.target = target

    def read(self, size=-1):
        """
        Read
        """
        # Read a chunk from the original buffer (e.g., StringIO)
        chunk = self.target.read(size)
        if chunk is None:
            return None

        # Remove the null bytes from the chunk and return it
        return chunk.replace('\x00', '')

    def readable(self):
        return True


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
        buffer.writelines(
            pd.read_excel(path, dtype=object).to_csv(index=False))
    buffer.seek(0)
    return buffer
