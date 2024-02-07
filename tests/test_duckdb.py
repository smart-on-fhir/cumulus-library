""" tests for duckdb backend support """

import glob
import os
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

import pytest

from cumulus_library import cli, databases


@mock.patch.dict(
    os.environ,
    clear=True,
)
def test_duckdb_core_build_and_export():
    data_dir = f"{Path(__file__).parent}/test_data/duckdb_data"

    with tempfile.TemporaryDirectory() as tmpdir:
        cli.main(
            [
                "build",
                "--target=core",
                "--db-type=duckdb",
                f"--database={tmpdir}/duck.db",
                f"--load-ndjson-dir={data_dir}",
            ]
        )
        cli.main(
            [
                "export",
                "--target=core",
                "--db-type=duckdb",
                f"--database={tmpdir}/duck.db",
                f"{tmpdir}/counts",
            ]
        )

        # Now check each csv file - we'll assume the parquest are alright
        csv_files = glob.glob(f"{tmpdir}/counts/core/*.csv")
        for csv_file in csv_files:
            basename = Path(csv_file).name
            with open(csv_file, encoding="utf8") as f:
                generated = f.read().strip()
            with open(
                f"{data_dir}/expected_export/core/{basename}", encoding="utf8"
            ) as f:
                expected = f.read().strip()
            assert generated == expected, basename


@pytest.mark.parametrize(
    "timestamp,expected",
    [
        ("2021", datetime(2021, 1, 1, tzinfo=timezone.utc)),
        ("2019-10", datetime(2019, 10, 1, tzinfo=timezone.utc)),
        ("1923-01-23", datetime(1923, 1, 23, tzinfo=timezone.utc)),
        (
            "2023-01-16T07:55:25-05:00",
            datetime(2023, 1, 16, 7, 55, 25, tzinfo=timezone(timedelta(hours=-5))),
        ),
    ],
)
def test_duckdb_from_iso8601_timestamp(timestamp, expected):
    db = databases.DuckDatabaseBackend(":memory:")
    parsed = (
        db.cursor()
        .execute(f"select from_iso8601_timestamp('{timestamp}')")
        .fetchone()[0]
    )
    assert parsed, expected
