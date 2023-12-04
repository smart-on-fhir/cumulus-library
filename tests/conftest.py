import tempfile

from pathlib import Path
from unittest import mock


import pytest

from cumulus_library.cli import StudyBuilder
from cumulus_library.databases import create_db_backend


@pytest.fixture
def mock_db():
    """Provides a DuckDatabaseBackend for local testing"""
    data_dir = f"{Path(__file__).parent}/test_data/duckdb_data"
    with tempfile.TemporaryDirectory() as tmpdir:
        db = create_db_backend(
            {
                "db_type": "duckdb",
                "schema_name": f"{tmpdir}/duck.db",
                "load_ndjson_dir": data_dir,
            }
        )
        yield db


@pytest.fixture
def mock_db_core():
    """Provides a DuckDatabaseBackend with the core study ranfor local testing"""
    data_dir = f"{Path(__file__).parent}/test_data/duckdb_data"
    with tempfile.TemporaryDirectory() as tmpdir:
        db = create_db_backend(
            {
                "db_type": "duckdb",
                "schema_name": f"{tmpdir}/duck.db",
                "load_ndjson_dir": data_dir,
            }
        )
        builder = StudyBuilder(db)
        builder.clean_and_build_study(
            f"{Path(__file__).parent.parent}/cumulus_library/studies/core"
        )
        yield db
