"""
Test
"""

import os
import json
from pathlib import Path

import pytest
from sqlalchemy import inspect

from data_utils.build_database import execute_directory
from data_utils.build_table import TableMeta
from data_utils.database import load_env, connect

DIRECTORY = Path()


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
    tables_meta = {}
    dependencies_meta = []

    # Retrieve files to ignore
    ignore = []
    if (ignore_file := DIRECTORY / ".dataignore").exists():
        with ignore_file.open("r", encoding="utf-8", errors="ignore") as file:
            ignore = file.read().splitlines()

    def test_load_meta(self):
        """Test loading table json"""
        for meta_path in DIRECTORY.glob("**/metadata/*.json"):
            if str(meta_path.relative_to(DIRECTORY)) in self.ignore:
                continue
            with open(meta_path, encoding="utf-8") as file:
                meta_json = json.load(file)
            table_meta = TableMeta(**meta_json)
            if meta_path.parents[1] == DIRECTORY:
                self.tables_meta[meta_path] = table_meta
            else:
                self.dependencies_meta.append(table_meta)

    def test_create_table(self, test_session):  # pylint: disable=redefined-outer-name
        """Test create database tables"""
        insp = inspect(test_session.get_bind())

        for table_meta in list(self.tables_meta.values()) + self.dependencies_meta:
            table_meta.create(test_session)
            stage_meta = table_meta.stage_meta
            assert (
                insp.has_table(table_meta.name, table_meta.schema) and
                insp.has_table(table_meta.name, stage_meta.schema)
            )
            table_meta.enforce_constraints(test_session)

    def test_data(self):
        """Test if data exists"""
        data_dir = DIRECTORY / "data"
        no_data = []
        for meta_path, table_meta in self.tables_meta.items():
            file_pattern = f"{meta_path.stem}.*.dvc"
            dir_pattern = f"{meta_path.stem}.dvc"
            files = list(data_dir.glob(file_pattern)) + list(data_dir.glob(dir_pattern))
            if len(files) == 0:
                no_data.append(table_meta.name)
        assert len(no_data) == 0, no_data

    def test_derive(self, test_session):  # pylint: disable=redefined-outer-name
        """Test create derived views"""
        execute_directory(DIRECTORY / "derive", test_session, self.ignore)
