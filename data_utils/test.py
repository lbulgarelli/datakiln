"""
Test
"""

import os
import json
from pathlib import Path

import pytest

from data_utils.build_table import load_env, connect, TableMeta

JSON_PATH = Path("metadata")
DATA_PATH = Path("data")


@pytest.fixture(scope="class")
def test_session():
    """Get database session"""
    # Yield session
    db_params = load_env(os.getcwd())
    session = connect(**db_params)
    yield session

    # Do not save test changes
    session.rollback()


class TestBuild():
    # pylint: disable=too-few-public-methods
    """
    Test loading functions
    """
    tables_meta = []

    def test_load_meta(self):
        """Test loading table json"""
        for meta_path in JSON_PATH.glob("*.json"):
            with open(meta_path, encoding="utf-8") as file:
                meta_json = json.load(file)
            table_meta = TableMeta(**meta_json)
            self.tables_meta.append(table_meta)
