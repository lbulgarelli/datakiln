"""
Test
"""

from pathlib import Path
from shutil import copytree

import pytest
import pandas as pd
from sqlalchemy import inspect
from dvc.testing.fixtures import make_tmp_dir  # pylint: disable=unused-import # noqa: E501 F401

from data_utils.test import test_session  # pylint: disable=unused-import # noqa: E501 F401
from data_utils.metadata.database import DatabaseMeta


TEST_DIR = Path("tests").absolute()


@pytest.fixture(scope="class")
def database(tmp_path_factory, make_tmp_dir):  # pylint: disable=redefined-outer-name # noqa: E501 F811
    """Test creating DVC directory"""
    # Copy files over to temporary directory
    test_repo = tmp_path_factory.mktemp("data-test")
    copytree(TEST_DIR, test_repo, dirs_exist_ok=True)

    # Commit data to dvc
    data_repo = make_tmp_dir(test_repo / "data", dvc=True).dvc
    for file in test_repo.glob("**/*.csv"):
        data_repo.add(file)
        data_repo.commit()

    yield DatabaseMeta(test_repo, "__test")


class TestBuild():
    """
    Test loading functions
    """

    def test_load_meta(self, database):  # pylint: disable=redefined-outer-name # noqa: E501
        """Test loading table json"""
        assert (
            database.tables[0].columns[0].name == "test_id" and
            database.tables[1].columns[0].name == "foreign_id"
        )

    def test_create(self, database, test_session):  # pylint: disable=redefined-outer-name # noqa: E501 F811
        """Test create database tables"""
        insp = inspect(test_session.connection())

        for table in database.tables:
            table.create(test_session)
            stage_meta = table.stage_meta
            assert (
                insp.has_table(table.name, table.schema) and
                insp.has_table(stage_meta.name, stage_meta.schema)
            )

    def test_stage(self, database, test_session):  # pylint: disable=redefined-outer-name # noqa: E501 F811
        """Test loading data to staging"""
        for table in database.tables:
            # Create table
            table.create(test_session)

            # Stage data
            stage_meta = table.stage_meta
            stage_meta.load(test_session)
            test_session.flush()

            # Get counts from csv
            data_path = TEST_DIR / "data" / table.artifact['name']
            data = pd.read_csv(data_path)
            count = len(data.index)

            assert test_session.execute(
                f"SELECT * FROM {stage_meta.schema}.{stage_meta.name}"
            ).rowcount == count

    def test_load(self, database, test_session):  # pylint: disable=redefined-outer-name # noqa: E501 F811
        """Test loading to final table"""
        for table in database.tables:
            # Load data
            table.enforce_constraints(test_session)
            table.load(test_session)
            test_session.flush()

            # Get counts from csv
            data_path = TEST_DIR / "data" / table.artifact['name']
            data = pd.read_csv(data_path)
            count = len(data.index)

            assert test_session.execute(
                f"SELECT * FROM {table.schema}.{table.name}"
            ).rowcount == count
