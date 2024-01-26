"""pytest mocks and testing utility classes/methods"""

import copy
import json
import os
import tempfile

from enum import IntEnum
from pathlib import Path

import pandas
import pytest
import numpy

from cumulus_library.cli import StudyBuilder
from cumulus_library.databases import create_db_backend, DatabaseCursor

MOCK_DATA_DIR = f"{Path(__file__).parent}/test_data/duckdb_data"
ID_PATHS = {
    "condition": [["id"], ["encounter", "reference"], ["subject", "reference"]],
    "documentreference": [
        ["id"],
        ["subject", "reference"],
        ["context", "encounter", "reference"],
    ],
    "encounter": [["id"], ["subject", "reference"]],
    "medicationrequest": [
        ["id"],
        ["encounter", "reference"],
        ["subject", "reference"],
        ["reasonReference", "reference"],
    ],
    "observation": [["id"], ["encounter", "reference"]],
    "patient": [["id"]],
}


class ResourceTableIdPos(IntEnum):
    """Primary ID column per FHIR resource table used in counts"""

    CONDITION = 7
    DOCUMENTREFERENCE = 1
    ENCOUNTER = 20
    MEDICATION = 1
    MEDICATIONREQUEST = 1
    OBSERVATION = 1
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


def duckdb_args(args: list, tmp_path, stats=False):
    """Convenience function for adding duckdb args to a CLI mock"""
    if stats:
        ndjson_data_generator(Path(MOCK_DATA_DIR), Path(f"{tmp_path}/stats_db"), 20)
        target = f"{tmp_path}/stats_db"
    else:
        target = f"{MOCK_DATA_DIR}"

    if args[0] == "build":
        return args + [
            "--db-type",
            "duckdb",
            "--load-ndjson-dir",
            target,
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


def ndjson_data_generator(source_dir: Path, target_dir: Path, iterations: int):
    """Uses the test data as a template to create large datasets

    Rather than a complex find/replace operation, we're just appending ints cast
    as strings to any FHIR resource ID we find. If you're doing something that
    relies on exact length of resource IDs, consider a different approach."""

    def update_nested_obj(id_path, obj, i):
        """Recursively update an object val, a thing that should be a pandas feature"""
        if len(id_path) == 1:
            # if we get a float it's a NumPy nan, so we should just let it go
            if isinstance(obj, float):
                pass
            elif isinstance(obj, list):
                obj[0][id_path[0]] = obj[0][id_path[0]] + str(i)
            else:
                obj[id_path[0]] = obj[id_path[0]] + str(i)
        else:
            if isinstance(obj, list):
                obj[0][id_path[0]] = update_nested_obj(
                    id_path[1:], obj[0][id_path[0]], i
                )
            else:
                obj[id_path[0]] = update_nested_obj(id_path[1:], obj[id_path[0]], i)

        return obj

    for key in ID_PATHS:
        for filepath in [f for f in Path(source_dir / key).iterdir()]:
            ref_df = pandas.read_json(filepath, lines=True)
            output_df = pandas.DataFrame()
            for i in range(0, iterations):
                df = ref_df.copy(deep=True)
                for id_path in ID_PATHS[key]:
                    if len(id_path) == 1:
                        if id_path[0] in df.columns:
                            df[id_path[0]] = df[id_path[0]] + str(i)
                    else:
                        if id_path[0] in df.columns:
                            # panda's deep copy is not recursive, so we have to do it
                            # again for nested objects
                            df[id_path[0]] = df[id_path[0]].map(
                                lambda x: update_nested_obj(
                                    id_path[1:], copy.deepcopy(x), i
                                )
                            )
                output_df = pandas.concat([output_df, df])
            # workaround for pandas/null/boolean casting issues
            for null_bool_col in ["multipleBirthBoolean"]:
                if null_bool_col in output_df.columns:
                    output_df[null_bool_col] = output_df[null_bool_col].replace(
                        {0.0: False}
                    )
            output_df = output_df.replace({numpy.nan: None})

            write_path = Path(str(target_dir) + f"/{key}/{filepath.name}")
            write_path.parent.mkdir(parents=True, exist_ok=True)
            # pandas.to_json() fails due to the datamodel complexity, so we'll manually
            # coerce to ndjson
            out_dict = output_df.to_dict(orient="records")
            with open(write_path, "w", encoding="UTF-8") as f:
                for row in out_dict:
                    f.write(json.dumps(row, default=str) + "\n")


@pytest.fixture
def mock_db(tmp_path):
    """Provides a DuckDatabaseBackend for local testing"""
    db = create_db_backend(
        {
            "db_type": "duckdb",
            "schema_name": f"{tmp_path}/duck.db",
            "load_ndjson_dir": MOCK_DATA_DIR,
        }
    )
    yield db


@pytest.fixture
def mock_db_core(tmp_path, mock_db):  # pylint: disable=redefined-outer-name
    """Provides a DuckDatabaseBackend with the core study ran for local testing"""
    builder = StudyBuilder(mock_db, data_path=f"{tmp_path}/data_path")
    builder.clean_and_build_study(
        f"{Path(__file__).parent.parent}/cumulus_library/studies/core", stats_build=True
    )
    yield mock_db


@pytest.fixture
def mock_db_stats(tmp_path):
    """Provides a DuckDatabaseBackend with a larger dataset for sampling stats"""
    ndjson_data_generator(Path(MOCK_DATA_DIR), f"{tmp_path}/mock_data", 20)
    db = create_db_backend(
        {
            "db_type": "duckdb",
            "schema_name": f"{tmp_path}cumulus.duckdb",
            "load_ndjson_dir": f"{tmp_path}/mock_data",
        }
    )
    builder = StudyBuilder(db, data_path=f"{tmp_path}/data_path")
    builder.clean_and_build_study(
        f"{Path(__file__).parent.parent}/cumulus_library/studies/core", stats_build=True
    )
    yield db
