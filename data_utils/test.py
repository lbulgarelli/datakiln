import os

import pytest

from data_utils.build_table import load_env, connect


@pytest.fixture(scope="class")
def test_session():
    """Get database session"""
    # Yield session
    db_params = load_env(os.getcwd())
    session = connect(**db_params)
    yield session

    # Do not save test changes
    session.rollback()
