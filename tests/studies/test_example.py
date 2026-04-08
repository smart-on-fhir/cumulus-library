import os
from unittest import mock

import time_machine

from cumulus_library import (
    cli,
    databases,
)
from tests import conftest


@mock.patch.dict(
    os.environ,
    clear=True,
)
@time_machine.travel("2024-01-01T00:00:00Z", tick=False)
def test_example_study_succeeds(tmp_path):
    build_args = conftest.duckdb_args(["build", "-t", "core"], tmp_path)
    cli.main(cli_args=build_args)
    build_args = conftest.duckdb_args(["build", "-t", "example"], tmp_path)
    cli.main(cli_args=build_args)
    build_args = conftest.duckdb_args(["build", "-t", "example", "--stage", "analysis"], tmp_path)
    cli.main(cli_args=build_args)
    db = databases.DuckDatabaseBackend(f"{tmp_path}/duck.db")
    db.connect()
    found_tables = db.connection.execute(
        "SELECT table_schema,table_name FROM information_schema.tables "
        "WHERE 'example' IN table_name"
    ).fetchall()
    # TODO: Add bronchitis data to validate joins
    assert len(found_tables) == 17
