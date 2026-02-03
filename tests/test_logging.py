import os
from contextlib import nullcontext as does_not_raise
from datetime import datetime
from unittest import mock

import duckdb
import pyathena
import pytest
import time_machine

from cumulus_library import (
    __version__,
    base_utils,
    databases,
    enums,
    errors,
    log_utils,
    study_manifest,
)
from cumulus_library.template_sql import base_templates, sql_utils


@time_machine.travel("2024-01-01T00:00:00Z")
@pytest.mark.parametrize(
    "schema,study,status,message,expects,raises",
    [
        (
            "main",
            "study_valid",
            enums.LogStatuses.STARTED,
            None,
            ("study_valid", __version__, "started", datetime(2024, 1, 1, 0, 0), None),
            does_not_raise(),
        ),
        (
            "main",
            "study_valid",
            "info",
            "status",
            ("study_valid", __version__, "info", datetime(2024, 1, 1, 0, 0), "status"),
            does_not_raise(),
        ),
        (
            "main",
            "study_dedicated_schema",
            "error",
            None,
            (
                "study_dedicated_schema",
                __version__,
                "error",
                datetime(2024, 1, 1, 0, 0),
                None,
            ),
            does_not_raise(),
        ),
        (
            "main",
            "study_valid",
            "invalid_type",
            None,
            None,
            pytest.raises(errors.CumulusLibraryError),
        ),
    ],
)
def test_transactions(mock_db, schema, study, status, message, expects, raises):
    with raises:
        cursor = mock_db.cursor()
        table = sql_utils.TransactionsTable()
        config = base_utils.StudyConfig(db=mock_db, schema="main")
        manifest = study_manifest.StudyManifest(f"./tests/test_data/{study}/")
        if manifest.get_dedicated_schema():
            schema = manifest.get_dedicated_schema()
            table_name = table.name
            cursor.execute(f"create schema {schema}")
        else:
            table_name = f"{manifest.get_study_prefix()}__{table.name}"
        query = base_templates.get_ctas_empty_query(
            schema_name=schema,
            table_name=table_name,
            table_cols=table.columns,
            table_cols_types=table.column_types,
        )
        cursor.execute(query)
        log_utils.log_transaction(
            config=config,
            manifest=manifest,
            status=status,
            message=message,
        )
        log = cursor.execute(f"select * from {schema}.{table_name}").fetchone()
        assert log == expects


def test_migrate_transactions_duckdb(mock_db_config):
    query = base_templates.get_ctas_empty_query(
        "main",
        "study_valid__lib_transactions",
        ["study_name", "library_version", "status", "event_time"],
        ["varchar", "varchar", "varchar", "timestamp"],
    )
    mock_db_config.db.cursor().execute(query)
    manifest = study_manifest.StudyManifest("./tests/test_data/study_valid/")
    with does_not_raise():
        log_utils.log_transaction(
            config=mock_db_config,
            manifest=manifest,
            status="debug",
            message="message",
        )
        cols = (
            mock_db_config.db.cursor()
            .execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name ='study_valid__lib_transactions' "
                "AND table_schema ='main'"
            )
            .fetchall()
        )
        assert len(cols) == 5
        assert ("message",) in cols


@mock.patch.dict(
    os.environ,
    clear=True,
)
@mock.patch("pyathena.connect")
def test_migrate_transactions_athena(mock_pyathena):
    mock_fetchall = mock.MagicMock()
    mock_fetchall.fetchall.side_effect = [
        [("event_time",), ("library_version",), ("status",), ("study_name",)],
        [
            ("event_time",),
            ("library_version",),
            ("status",),
            ("study_name",),
            ("message",),
        ],
    ]
    mock_pyathena.return_value.cursor.return_value.execute.side_effect = [
        pyathena.OperationalError,
        mock_fetchall,
        None,
        None,
    ]

    db = databases.AthenaDatabaseBackend(
        region="test",
        work_group="test",
        profile="test",
        schema_name="test",
    )
    db.connect()
    manifest = study_manifest.StudyManifest("./tests/test_data/study_valid/")
    config = base_utils.StudyConfig(schema="test", db=db)
    log_utils.log_transaction(
        config=config,
        manifest=manifest,
        status="debug",
        message="message",
    )
    expected = "ALTER TABLE test.study_valid__lib_transactions ADD COLUMNS(message string)"
    call_args = mock_pyathena.return_value.cursor.return_value.execute.call_args_list
    assert expected == call_args[2][0][0]


def test_statistics_failure_gets_raised(mock_db_config):
    mock_db_config.db.cursor = mock.MagicMock()
    mock_db_config.db.cursor.return_value.execute.side_effect = duckdb.OperationalError
    with pytest.raises(duckdb.OperationalError):
        log_utils.log_statistics(
            config=mock_db_config,
            manifest=study_manifest.StudyManifest(),
            table_type="test1",
            table_name="test2",
            view_name="test3",
        )


@time_machine.travel("2024-01-01T00:00:00Z")
@pytest.mark.parametrize(
    "schema,study,table_type,table_name,view_type,expects,raises",
    [
        (
            "main",
            "study_valid",
            "psm",
            "psm123",
            "psmview",
            (
                "study_valid",
                __version__,
                "psm",
                "study_valid__lib_statistics",
                "psmview",
                datetime(2024, 1, 1, 0, 0),
            ),
            does_not_raise(),
        ),
        (
            "main",
            "study_dedicated_schema",
            "psm",
            "psm123",
            "psmview",
            (
                "study_dedicated_schema",
                __version__,
                "psm",
                "lib_statistics",
                "psmview",
                datetime(2024, 1, 1, 0, 0),
            ),
            does_not_raise(),
        ),
    ],
)
def test_statistics(mock_db, schema, study, table_type, table_name, view_type, raises, expects):
    with raises:
        cursor = mock_db.cursor()
        table = sql_utils.StatisticsTable()
        manifest = study_manifest.StudyManifest(f"./tests/test_data/{study}/")
        config = base_utils.StudyConfig(db=mock_db, schema="main")
        if manifest.get_dedicated_schema():
            schema = manifest.get_dedicated_schema()
            table_name = table.name
            cursor.execute(f"create schema {schema}")
        else:
            table_name = f"{manifest.get_study_prefix()}__{table.name}"
        query = base_templates.get_ctas_empty_query(
            schema_name=schema,
            table_name=table_name,
            table_cols=table.columns,
            table_cols_types=table.column_types,
        )
        cursor.execute(query)
        log_utils.log_statistics(
            config=config,
            manifest=manifest,
            table_type=table_type,
            table_name=table_name,
            view_name=view_type,
        )
        log = cursor.execute(f"select * from {schema}.{table_name}").fetchone()
        assert log == expects
