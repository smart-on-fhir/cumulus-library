"""pytest mocks and testing utility classes/methods"""

import copy
import json
from pathlib import Path

import numpy
import pandas
import pytest
from rich import console, table

from cumulus_library.cli import StudyRunner
from cumulus_library.databases import create_db_backend

# Useful constants

TESTS_ROOT = Path(__file__).parent
LIBRARY_ROOT = TESTS_ROOT.parent / "cumulus_library"
MOCK_DATA_DIR = f"{TESTS_ROOT}/test_data/duckdb_data"
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

# Utility functions


def get_sorted_table_data(cursor, table):
    num_cols = cursor.execute(
        f"SELECT count(*) FROM information_schema.columns WHERE table_name='{table}'"
    ).fetchone()[0]
    if num_cols == 0:
        return [], []
    data = cursor.execute(
        f"SELECT * FROM '{table}' ORDER BY " f"{','.join(map(str, range(1,num_cols)))}"
    ).fetchall()
    return data, cursor.description


def duckdb_args(args: list, tmp_path, stats=False):
    """Convenience function for adding duckdb args to a CLI mock"""
    if stats:
        ndjson_data_generator(Path(MOCK_DATA_DIR), Path(f"{tmp_path}/stats_db"), 20)
        target = f"{tmp_path}/stats_db"
    else:
        target = f"{MOCK_DATA_DIR}"

    if args[0] == "build":
        return [
            *args,
            "--db-type",
            "duckdb",
            "--load-ndjson-dir",
            target,
            "--database",
            f"{tmp_path}/duck.db",
        ]
    elif args[0] == "export":
        return [
            *args,
            "--db-type",
            "duckdb",
            "--database",
            f"{tmp_path}/duck.db",
            f"{tmp_path}/export",
        ]
    return [*args, "--db-type", "duckdb", "--database", f"{tmp_path}/duck.db"]


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
                                lambda x, i=i, id_path=id_path: update_nested_obj(
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


# Debugging aids


def debug_table_schema(cursor, table):
    table_schema = cursor.execute(
        "select column_name, data_type from information_schema.columns "
        f"where table_name='{table}'"
    ).fetchall()
    for line in table_schema:
        print(line)


def debug_table_head(cursor, table, rows=3, cols="*"):
    if isinstance(cols, list):
        cols = ",".join(cols)
    table_schema = cursor.execute(f"select {cols} from {table} limit {rows}").fetchall()
    col_names = []
    for field in cursor.description:
        col_names.append(field[0])
    for line in table_schema:
        print(line)
    print()


def debug_diff_tables(cols, found, ref, pos=0):
    cols = cols if len(cols) > 0 else []
    found = found[pos] if len(found) > pos else []
    ref = ref[pos] if len(ref) > pos else []
    max_size = max(len(found), len(ref))
    diff_table = table.Table(title=f"Row {pos} delta")
    diff_table.add_column("DB Column")
    diff_table.add_column("Found in DB")
    diff_table.add_column("Reference")
    for i in range(0, max_size):
        diff_table.add_row(
            cols[i][0] if cols and i < len(cols) else "**None**",
            str(found[i]) if i < len(found) else "**None**",
            str(ref[i]) if i < len(ref) else "**None**",
        )
    output = console.Console()
    output.print(diff_table)


# Database fixtures


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
    builder = StudyRunner(mock_db, data_path=f"{tmp_path}/data_path")
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
    builder = StudyRunner(db, data_path=f"{tmp_path}/data_path")
    builder.clean_and_build_study(
        f"{Path(__file__).parent.parent}/cumulus_library/studies/core", stats_build=True
    )
    yield db
