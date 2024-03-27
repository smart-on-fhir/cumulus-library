"""unit tests for completion tracking support in the core study"""

import datetime
import json
from pathlib import Path

import duckdb
import pytest
import toml

from cumulus_library import cli
from cumulus_library.studies.core.core_templates import core_templates
from cumulus_library.databases import DatabaseBackend, DatabaseCursor, create_db_backend

from tests import conftest


class LocalTestbed:
    def __init__(self, path: Path):
        self.path = path
        self.indices: dict[str, int] = {}

        # TODO: fix the core SQL to check for extensions in the schema before querying them.
        #  In the meantime, we can just ensure those fields exist, ready to be queried.
        self.add(
            "patient",
            {
                "id": "FakeA",
                "extension": [
                    {
                        "url": "",
                        "extension": [
                            {
                                "url": "",
                                "valueCoding": {
                                    "code": "",
                                    "display": "",
                                },
                            }
                        ],
                    }
                ],
            },
        )

    def add(self, table: str, obj: dict) -> None:
        index = self.indices.get(table, -1) + 1
        self.indices[table] = index

        table_dir = self.path / table
        table_dir.mkdir()

        with open(table_dir / f"{index}.ndjson", "w", encoding="utf8") as f:
            json.dump(obj, f)

    def build_core(self) -> duckdb.DuckDBPyConnection:
        db = create_db_backend(
            {
                "db_type": "duckdb",
                "schema_name": f"{self.path}/core.db",
                "load_ndjson_dir": str(self.path),
            }
        )
        builder = cli.StudyRunner(db, data_path=str(self.path))
        builder.clean_and_build_study(
            Path(__file__).parent.parent / "cumulus_library/studies/core",
            stats_build=False,
        )
        return duckdb.connect(f"{self.path}/core.db")


def test_completion_cases(tmp_path):
    testbed = LocalTestbed(tmp_path)
    testbed.add("encounter", {"id": "A"})
    con = testbed.build_core()
    encounters = con.sql("SELECT * FROM core__encounter").fetchall()
    assert {e["id"] for e in encounters} == {"A"}
