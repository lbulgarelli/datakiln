"""
Test
"""

import os
from pathlib import Path

import pytest
from sqlalchemy import inspect

from data_utils.metadata.database import DatabaseMeta, Session

DIRECTORY = Path()


@pytest.fixture(scope="class")
def test_session():
    """Get database session"""
    # Yield session
    db_params = DatabaseMeta.load_env(os.getcwd())
    db_params["database"] = "__test"
    session = Session.create(**db_params)
    yield session

    # Do not save test changes
    session.rollback()


class TestBuild():
    # pylint: disable=too-few-public-methods
    """
    Test loading functions
    """
    database = DatabaseMeta(DIRECTORY, "__test")

    dependencies = []
    for dependency in sorted(DIRECTORY.rglob("dependencies/data-*"), reverse=True):
        if str(dependency.parents[-2]) == ".git":
            continue
        dependencies.append(DatabaseMeta(dependency, "__test"))

    def test_database(self):
        """Test loading table json"""
        # Check if there are any metadata to load
        assert isinstance(self.database.tables, list)

    def test_dependencies(self):
        """Test loading dependencies"""
        for database in self.dependencies:
            assert isinstance(database.tables, list)

    def test_create_table(self, test_session):  # pylint: disable=redefined-outer-name # noqa: E501
        """Test create database tables"""
        insp = inspect(test_session.connection())

        tables = self.database.tables
        for dependency in self.dependencies:
            tables[0:0] = dependency.tables

        for table in tables:
            table.create(test_session)
            stage = table.stage_meta
            assert (
                insp.has_table(table.name, table.schema) and
                insp.has_table(stage.name, stage.schema)
            )
            table.enforce_pk(test_session)

    def test_transform(self, test_session):  # pylint: disable=redefined-outer-name # noqa: E501
        """Test transforming data"""
        self.database.execute_directory(
            "transform", test_session, max_statements=0)

    def test_constraints(self, test_session):  # pylint: disable=redefined-outer-name # noqa: E501
        """Test enforcing contraints"""
        for table in self.database.tables:
            table.enforce_constraints(test_session)
            table.create_uniques(test_session)
            table.create_indexes(test_session)

    def test_derive(self, test_session):  # pylint: disable=redefined-outer-name # noqa: E501
        """Test create derived views"""
        for dependency in self.dependencies:
            dependency.execute_directory("derive", test_session)

        self.database.execute_directory("derive", test_session)
