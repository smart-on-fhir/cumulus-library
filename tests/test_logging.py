from contextlib import nullcontext as does_not_raise
from datetime import datetime

import pytest
from freezegun import freeze_time

from cumulus_library import __version__, enums, errors, log_utils, study_parser
from cumulus_library.template_sql import base_templates, sql_utils


@freeze_time("2024-01-01")
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
        manifest = study_parser.StudyManifestParser(f"./tests/test_data/{study}/")
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
            cursor=cursor,
            schema=schema,
            manifest=manifest,
            status=status,
            message=message,
        )
        log = cursor.execute(f"select * from {schema}.{table_name}").fetchone()
        assert log == expects


@freeze_time("2024-01-01")
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
def test_statistics(
    mock_db, schema, study, table_type, table_name, view_type, raises, expects
):
    with raises:
        cursor = mock_db.cursor()
        table = sql_utils.StatisticsTable()
        manifest = study_parser.StudyManifestParser(f"./tests/test_data/{study}/")
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
            cursor=cursor,
            schema=schema,
            manifest=manifest,
            table_type=table_type,
            table_name=table_name,
            view_name=view_type,
        )
        log = cursor.execute(f"select * from {schema}.{table_name}").fetchone()
        assert log == expects
