"""
Test
"""

import os
import json
from pathlib import Path

import pytest
from sqlalchemy import inspect

from data_utils.build_database import execute_directory
from data_utils.build_table import load_env, connect, TableMeta

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
    tables_meta = []

    # Retrieve files to ignore
    ignore = []
    if (ignore_file := DIRECTORY / ".dataignore").exists():
        with ignore_file.open("r", encoding="utf-8", errors="ignore") as file:
            ignore = file.read().splitlines()

    def test_load_meta(self):
        """Test loading table json"""
        for meta_path in (DIRECTORY / "metadata").glob("*.json"):
            if meta_path in self.ignore:
                continue
            with open(meta_path, encoding="utf-8") as file:
                meta_json = json.load(file)
            table_meta = TableMeta(**meta_json)
            self.tables_meta.append(table_meta)
     
    def test_create_table(self, test_session):
        """Test create database tables"""
        insp = inspect(test_session.get_bind())

        for table_meta in self.tables_meta:
            table_meta.create(test_session)
            stage_meta = table_meta.stage_meta
            assert (
                insp.has_table(table_meta.name, table_meta.schema) and
                insp.has_table(table_meta.name, stage_meta.schema)
            )
     
    def test_create_table(self, test_session):
        """Test create database tables"""
        for table_meta in self.tables_meta:
            table_meta.create(test_session)
            table_meta.enforce_constraints(test_session)

    def test_derive(self, test_session):
        """Test create derived views"""
        execute_directory(DIRECTORY / "derive", test_session, self.ignore)