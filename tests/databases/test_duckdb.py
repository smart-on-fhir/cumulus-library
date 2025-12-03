"""tests for duckdb backend support"""

import glob
import json
import os
import tempfile
from datetime import datetime
from pathlib import Path

import pytest

from cumulus_library import cli, databases
from cumulus_library.template_sql import base_templates


def test_duckdb_core_build_and_export(tmp_path):
    data_dir = f"{Path(__file__).parents[1]}/test_data/duckdb_data"
    cli.main(
        [
            "build",
            "--target=core",
            "--db-type=duckdb",
            f"--database={tmp_path}/duck.db",
            f"--load-ndjson-dir={data_dir}",
        ]
    )
    cli.main(
        [
            "export",
            "--target=core",
            "--db-type=duckdb",
            f"--database={tmp_path}/duck.db",
            f"{tmp_path}/counts",
        ]
    )

    # Now check each csv file - we'll assume the parquets are alright
    csv_files = glob.glob(f"{tmp_path}/counts/core/*.csv")
    for csv_file in csv_files:
        basename = Path(csv_file).name
        with open(csv_file, encoding="utf8") as f:
            generated = f.read().strip()
        with open(f"{data_dir}/expected_export/core/{basename}", encoding="utf8") as f:
            expected = f.read().strip()
        assert generated == expected, basename


@pytest.mark.parametrize(
    "timestamp,expected",
    [
        ("2021", datetime(2021, 1, 1)),
        ("2019-10", datetime(2019, 10, 1)),
        ("1923-01-23", datetime(1923, 1, 23)),
        (
            "2023-01-16T07:55:25-05:00",
            datetime(2023, 1, 16, 12, 55, 25),
        ),
    ],
)
def test_duckdb_from_iso8601_timestamp(timestamp, expected):
    db = databases.DuckDatabaseBackend(":memory:")
    db.connect()
    parsed = db.cursor().execute(f"select from_iso8601_timestamp('{timestamp}')").fetchone()[0]
    assert parsed == expected


def test_duckdb_load_ndjson_dir(tmp_path):
    filenames = {
        "blarg.ndjson": True,
        "blarg.nope": False,
        "subdir/deeply/nested/blarg.ndjson": True,
        "patient/blarg.ndjson": True,
        "patient/blarg.meta": False,
    }
    os.mkdir(f"{tmp_path}/patient")
    os.makedirs(f"{tmp_path}/subdir/deeply/nested")
    for index, (filename, valid) in enumerate(filenames.items()):
        with open(f"{tmp_path}/{filename}", "w", encoding="utf8") as f:
            row_id = f"Good{index}" if valid else f"Bad{index}"
            f.write(f'{{"id":"{row_id}", "resourceType": "Patient"}}\n')

    db, _ = databases.create_db_backend(
        {
            "db_type": "duckdb",
            "database": ":memory:",
            "load_ndjson_dir": tmp_path,
        }
    )

    expected_good_count = len({f for f, v in filenames.items() if v})
    found_ids = {row[0] for row in db.cursor().execute("select id from patient").fetchall()}
    found_good = {row_id for row_id in found_ids if row_id.startswith("Good")}
    found_bad = found_ids - found_good
    assert len(found_good) == expected_good_count
    assert len(found_bad) == 0


def test_duckdb_table_schema():
    """Verify we can detect schemas correctly, even for nested camel case fields"""
    db = databases.DuckDatabaseBackend(":memory:")
    db.connect()

    with tempfile.TemporaryDirectory() as tmpdir:
        os.mkdir(f"{tmpdir}/observation")
        with open(f"{tmpdir}/observation/test.ndjson", "w", encoding="utf8") as ndjson:
            json.dump(
                {
                    "resourceType": "Observation",
                    "id": "test",
                    "component": [
                        {
                            "dataAbsentReason": {"text": "Dunno"},
                            "valuePeriod": {"id": "X"},
                        }
                    ],
                    "valueBoolean": False,
                },
                ndjson,
            )

        db.insert_tables(databases.get_ndjson_files(tmpdir))

        # Look for a mix of camel-cased and lower-cased fields. Both should work.
        target_schema = {
            "bodySite": [],
            "CoMpOnEnT": {
                "dataabsentreason": [],
                "valuePeriod": ["id"],
                "valueQuantity": [],
            },
            "not_a_real_field": [],
            "valueboolean": [],
        }

        # Query database for what exists right now as a schema
        query = base_templates.get_column_datatype_query(
            # Use a mixed-case table name
            db.schema_name,
            "Observation",
            list(target_schema.keys()),
        )
        actual_schema = db.cursor().execute(query).fetchall()

        # Validate that schema against what we were looking for
        validated_schema = db.parser().validate_table_schema(target_schema, actual_schema)
        # Note the all mixed-case results.
        # These are guaranteed to be the same case as the expected/target schema.
        expected_schema = {
            "bodySite": True,  # real toplevel fields are guaranteed to be in schema
            "CoMpOnEnT": {
                "dataabsentreason": True,
                "valuePeriod": {"id": True},
                "valueQuantity": False,
            },
            "not_a_real_field": False,
            "valueboolean": True,
        }
        assert validated_schema == expected_schema
