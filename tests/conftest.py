"""Shared Spark and filesystem fixtures."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from src.common import get_spark_session


@pytest.fixture(scope="session")
def spark():
    """Create a lightweight Spark session for transformation unit tests."""
    session = get_spark_session(
        "lakehouse-unit-tests",
        master="local[2]",
        with_delta=False,
    )
    yield session
    session.stop()


@pytest.fixture(scope="session")
def delta_spark():
    """Create a Delta-enabled session for explicitly selected integration tests."""
    session = get_spark_session("lakehouse-delta-tests", master="local[2]")
    yield session
    session.stop()


def pytest_collection_modifyitems(config, items):
    """Skip Delta integration tests unless the caller explicitly enables them."""
    if os.getenv("RUN_DELTA_TESTS") == "1":
        return
    skip = pytest.mark.skip(reason="set RUN_DELTA_TESTS=1 to run Delta integration tests")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip)


@pytest.fixture
def tmp_warehouse(tmp_path: Path) -> str:
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    return str(warehouse)


@pytest.fixture
def tmp_data(tmp_path: Path) -> str:
    data = tmp_path / "data"
    data.mkdir()
    return str(data)
