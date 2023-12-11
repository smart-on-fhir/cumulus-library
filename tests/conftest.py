"""pytest mocks and testing utility classes/methods"""

import tempfile

from enum import IntEnum
from pathlib import Path


import pytest

from cumulus_library.cli import StudyBuilder
from cumulus_library.databases import create_db_backend, DatabaseCursor

MOCK_DATA_DIR = f"{Path(__file__).parent}/test_data/duckdb_data"


class ResourceTableIdPos(IntEnum):
    """Primary ID column per FHIR resource table used in counts"""

    CONDITION = 7
    DOCUMENTREFERENCE = 11
    ENCOUNTER = 20
    OBSERVATION = 14
    PATIENT = 4


def modify_resource_column(
    cursor: DatabaseCursor, table: str, col: str, replacement_val
):
    """Allows for modifying a single table col for edge case detection

    TODO: if anything more complex than this is required for unit testing, either
    create a seperate test dir directory, or consider a generator using the
    existing test data as a source.
    """
    df = cursor.execute(f"SELECT * from {table}").df()
    df[col] = df.apply(lambda x: replacement_val, axis=1)
    cursor.register(f"{table}_{col}_df", df)
    cursor.execute(f"DROP VIEW {table}")
    cursor.execute(f"CREATE VIEW {table} AS SELECT * from {table}_{col}_df")


def duckdb_args(args: list, tmp_path):
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
def mock_db_core(mock_db):  # pylint: disable=redefined-outer-name
    """Provides a DuckDatabaseBackend with the core study ran for local testing"""
    builder = StudyBuilder(mock_db)
    builder.clean_and_build_study(
        f"{Path(__file__).parent.parent}/cumulus_library/studies/core"
    )
    yield mock_db
