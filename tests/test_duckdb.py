""" tests for duckdb backend support """

import glob
import os
import tempfile
from pathlib import Path

from cumulus_library import cli


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
