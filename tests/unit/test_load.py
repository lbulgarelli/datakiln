"""
Test
"""

import json
from pathlib import Path

import pandas as pd
from sqlalchemy import inspect, text

from data_utils.test import test_session
from data_utils.build_table import TableMeta, stage


TEST_JSON_PATH = Path("tests") / "metadata"
TEST_DATA_PATH = Path("tests") / "data"
TEST_FILES = ["test", "foreign_key"]


class TestBuild():
    """
    Test loading functions
    """
    tables_meta = []

    def test_load_meta(self):
        """Test loading table json"""
        for test_file in TEST_FILES:
            meta_path = TEST_JSON_PATH / f"{test_file}.json"
            with open(meta_path, encoding="utf-8") as file:
                meta_json = json.load(file)
            table_meta = TableMeta(**meta_json)
            self.tables_meta.append(table_meta)

        assert (
            self.tables_meta[0].columns[0].name == "test_id" and
            self.tables_meta[1].columns[0].name == "foreign_id"
        )

    def test_create(self, test_session):
        """Test create database tables"""
        insp = inspect(test_session.get_bind())

        for table_meta in self.tables_meta:
            table_meta.create(test_session)
            stage_meta = table_meta.stage_meta
            assert (
                insp.has_table(table_meta.name, table_meta.schema) and
                insp.has_table(table_meta.name, stage_meta.schema)
            )

    def test_stage(self, test_session):
        """Test loading data to staging"""
        for table_meta in self.tables_meta:
            data_path = TEST_DATA_PATH / f"{table_meta.name}.csv"
            stage_meta = table_meta.stage_meta
            stage_meta.load(test_session, data_path.absolute())

            # Get counts from csv
            data = pd.read_csv(data_path)
            count = len(data.index)

            assert test_session.execute(text(
                f"SELECT * FROM {stage_meta.schema}.{stage_meta.name}"
            )).rowcount == count

    def test_load(self, test_session):
        """Test loading to final table"""
        for table_meta in self.tables_meta:
            data_path = TEST_DATA_PATH / f"{table_meta.name}.csv"
            table_meta.load(test_session)

            # Get counts from csv
            data = pd.read_csv(data_path)
            count = len(data.index)

            assert test_session.execute(text(
                f"SELECT * FROM {table_meta.schema}.{table_meta.name}"
            )).rowcount == count
