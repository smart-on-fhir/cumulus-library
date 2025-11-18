import contextlib
import io
import pathlib
from contextlib import nullcontext as does_not_raise
from unittest import mock

import pytest
from freezegun import freeze_time

from cumulus_library import base_utils, enums, errors, log_utils, study_manifest
from cumulus_library.actions import (
    builder,
)
from cumulus_library.template_sql import base_templates, sql_utils


@pytest.mark.parametrize(
    "study_path,stats",
    [
        ("./tests/test_data/study_valid/", False),
        ("./tests/test_data/psm/", True),
    ],
)
def test_run_protected_table_builder(mock_db_config, study_path, stats):
    manifest = study_manifest.StudyManifest(study_path)
    builder.run_protected_table_builder(config=mock_db_config, manifest=manifest)
    tables = (
        mock_db_config.db.cursor()
        .execute("SELECT distinct(table_name) FROM information_schema.tables ")
        .fetchall()
    )
    assert (f"{manifest.get_study_prefix()}__{enums.ProtectedTables.TRANSACTIONS.value}",) in tables
    if stats:
        assert (
            f"{manifest.get_study_prefix()}__{enums.ProtectedTables.STATISTICS.value}",
        ) in tables
    else:
        assert (
            f"{manifest.get_study_prefix()}__{enums.ProtectedTables.STATISTICS.value}",
        ) not in tables


@pytest.mark.parametrize(
    "study_path,verbose,expects,raises",
    [
        (
            "./tests/test_data/study_python_valid/",
            True,
            ("study_python_valid__table",),
            does_not_raise(),
        ),
        (
            "./tests/test_data/study_python_valid_parallel/",
            False,
            ("study_python_valid_parallel__table",),
            does_not_raise(),
        ),
        (
            "./tests/test_data/study_python_no_subclass/",
            True,
            (),
            pytest.raises(errors.StudyManifestParsingError),
        ),
        (
            "./tests/test_data/study_python_local_template/",
            False,
            ("study_python_local_template__table_duckdb_foo",),
            does_not_raise(),
        ),
    ],
)
def test_table_builder(mock_db_config, study_path, verbose, expects, raises):
    with raises:
        manifest = study_manifest.StudyManifest(pathlib.Path(study_path))
        builder.build_study(config=mock_db_config, manifest=manifest, data_path=None, prepare=False)
        tables = (
            mock_db_config.db.cursor()
            .execute("SELECT distinct(table_name) FROM information_schema.tables ")
            .fetchall()
        )
        assert expects in tables


@pytest.mark.parametrize(
    "study_path,verbose,expects,raises",
    [
        (
            "./tests/test_data/study_valid/",
            True,
            ("study_valid__table",),
            does_not_raise(),
        ),
        (
            "./tests/test_data/study_valid_parallel/",
            False,
            ("study_valid_parallel__table",),
            does_not_raise(),
        ),
        (
            "./tests/test_data/study_invalid_bad_query/",
            None,
            ("study_valid__table",),
            pytest.raises(SystemExit),
        ),
        (
            "./tests/test_data/study_wrong_prefix/",
            None,
            [],
            pytest.raises(SystemExit),
        ),
        (
            "./tests/test_data/study_invalid_no_dunder/",
            True,
            (),
            pytest.raises(SystemExit),
        ),
        (
            "./tests/test_data/study_invalid_two_dunder/",
            True,
            (),
            pytest.raises(SystemExit),
        ),
        (
            "./tests/test_data/study_invalid_reserved_word/",
            True,
            (),
            pytest.raises(SystemExit),
        ),
    ],
)
def test_build_study(mock_db_config, study_path, verbose, expects, raises):
    with raises:
        manifest = study_manifest.StudyManifest(study_path)
        table = sql_utils.TransactionsTable()
        query = base_templates.get_ctas_empty_query(
            schema_name="main",
            table_name=f"{manifest.get_study_prefix()}__lib_transactions",
            table_cols=table.columns,
            table_cols_types=table.column_types,
        )
        mock_db_config.db.cursor().execute(query)
        builder.build_study(config=mock_db_config, manifest=manifest, data_path=None, prepare=False)
        tables = (
            mock_db_config.db.cursor()
            .execute("SELECT distinct(table_name) FROM information_schema.tables ")
            .fetchall()
        )
        assert expects in tables


@freeze_time("2024-01-01")
@pytest.mark.parametrize(
    "study_path,stats,previous,expects,raises",
    [
        (
            "./tests/test_data/psm/",
            False,
            False,
            ("psm_test__psm_encounter_covariate_2024_01_01T00_00_00_00_00",),
            does_not_raise(),
        ),
        (
            "./tests/test_data/psm/",
            False,
            True,
            ("psm_test__psm_encounter_covariate_2024_01_01T00_00_00_00_00",),
            does_not_raise(),
        ),
        (
            "./tests/test_data/psm/",
            True,
            False,
            ("psm_test__psm_encounter_covariate_2024_01_01T00_00_00_00_00",),
            does_not_raise(),
        ),
    ],
)
def test_run_psm_statistics_builders(
    tmp_path, mock_db_stats_config, study_path, stats, previous, expects, raises
):
    with raises:
        manifest = study_manifest.StudyManifest(pathlib.Path(study_path), data_path=tmp_path)
        mock_db_stats_config.stats_build = stats
        builder.run_protected_table_builder(
            config=mock_db_stats_config,
            manifest=manifest,
        )
        if previous:
            log_utils.log_statistics(
                config=mock_db_stats_config,
                manifest=manifest,
                table_type="psm",
                table_name="psm_test__psm_encounter_2023_06_15",
                view_name="psm_test__psm_encounter_covariate",
            )
        builder.build_study(
            config=mock_db_stats_config, manifest=manifest, prepare=False, data_path=None
        )
        tables = (
            mock_db_stats_config.db.cursor()
            .execute("SELECT distinct(table_name) FROM information_schema.tables")
            .fetchall()
        )
        if previous:
            assert expects not in tables
        else:
            assert expects in tables


@mock.patch("cumulus_library.builders.valueset_builder.ValuesetBuilder.execute_queries")
def test_invoke_valueset_builder(mock_builder, mock_db_config, tmp_path):
    manifest = study_manifest.StudyManifest(
        pathlib.Path(__file__).parents[1] / "test_data/valueset", data_path=tmp_path
    )
    builder.run_protected_table_builder(
        config=mock_db_config,
        manifest=manifest,
    )
    config = base_utils.StudyConfig(
        db=mock_db_config.db, schema=mock_db_config.schema, stats_build=True
    )
    builder.build_study(config=config, manifest=manifest, prepare=False, data_path=None)
    assert mock_builder.is_called()


def test_builder_init_error(mock_db_config):
    manifest = study_manifest.StudyManifest(
        pathlib.Path(__file__).parents[1] / "test_data/study_valid"
    )
    builder.run_protected_table_builder(config=mock_db_config, manifest=manifest)
    console_output = io.StringIO()
    with pytest.raises(SystemExit):
        with contextlib.redirect_stdout(console_output):
            builder._query_error(
                mock_db_config, manifest, "mock query", "mock_file.txt", "Catalog Error"
            )
    assert "https://docs.smarthealthit.org/" in console_output.getvalue()
