import tempfile

from pathlib import Path
from unittest import mock


import pytest

from cumulus_library.cli import StudyBuilder
from cumulus_library.databases import create_db_backend

MOCK_DATA_DIR = f"{Path(__file__).parent}/test_data/duckdb_data"


def duckdb_args(args, tmp_path):
    """Convenience function for adding duckdb args to a CLI mock"""
    if args[0] == "build":
        return args + [
            "--db-type",
            "duckdb",
            "--load-ndjson-dir",
            f"{MOCK_DATA_DIR}",
            "--database",
            f"{tmp_path}/duck.db",
        ]
    elif args[0] == "export":
        return args + [
            "--db-type",
            "duckdb",
            "--database",
            f"{tmp_path}/duck.db",
            f"{tmp_path}/counts",
        ]
    return args + ["--db-type", "duckdb", "--database", f"{tmp_path}/duck.db"]


@pytest.fixture
def mock_db():
    """Provides a DuckDatabaseBackend for local testing"""
    with tempfile.TemporaryDirectory() as tmpdir:
        db = create_db_backend(
            {
                "db_type": "duckdb",
                "schema_name": f"{tmpdir}/duck.db",
                "load_ndjson_dir": MOCK_DATA_DIR,
            }
        )
        yield db


@pytest.fixture
def mock_db_core(mock_db):
    """Provides a DuckDatabaseBackend with the core study ran for local testing"""
    builder = StudyBuilder(mock_db)
    builder.clean_and_build_study(
        f"{Path(__file__).parent.parent}/cumulus_library/studies/core"
    )
    yield mock_db
