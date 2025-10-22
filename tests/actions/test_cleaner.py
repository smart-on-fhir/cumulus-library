import builtins
from contextlib import nullcontext as does_not_raise
from unittest import mock

import pytest

from cumulus_library import enums, errors, study_manifest
from cumulus_library.actions import (
    builder,
    cleaner,
)


@pytest.mark.parametrize(
    "verbose,prefix,confirm,stats,target,raises",
    [
        (True, None, None, False, "study_valid__table", does_not_raise()),
        (False, None, None, False, "study_valid__table", does_not_raise()),
        (None, None, None, False, "study_valid__table", does_not_raise()),
        (None, None, None, False, "study_valid__etl_table", does_not_raise()),
        (None, None, None, False, "study_valid__nlp_table", does_not_raise()),
        (None, None, None, False, "study_valid__lib_table", does_not_raise()),
        (None, None, None, False, "study_valid__lib", does_not_raise()),
        (None, "foo", "y", False, "foo_table", does_not_raise()),
        (None, "foo", "n", False, "foo_table", pytest.raises(SystemExit)),
        (True, None, "y", True, "study_valid__table", does_not_raise()),
        (
            True,
            None,
            "n",
            True,
            "study_valid__table",
            pytest.raises(SystemExit),
        ),
    ],
)
def test_clean_study(mock_db_config, verbose, prefix, confirm, stats, target, raises):
    with raises:
        mock_db_config.stats_clean = stats
        protected_strs = [x.value for x in enums.ProtectedTableKeywords]
        with mock.patch.object(builtins, "input", lambda _: confirm):
            manifest = study_manifest.StudyManifest("./tests/test_data/study_valid/")
            builder.run_protected_table_builder(
                config=mock_db_config,
                manifest=manifest,
            )

            # We're mocking stats tables since creating them programmatically
            # is very slow and we're trying a lot of conditions
            mock_db_config.db.cursor().execute(
                f"CREATE TABLE {manifest.get_study_prefix()}__"
                f"{enums.ProtectedTables.STATISTICS.value} "
                "AS SELECT 'study_valid' as study_name, "
                "'study_valid__123' AS table_name"
            )
            mock_db_config.db.cursor().execute("CREATE TABLE study_valid__123 (test int)")
            mock_db_config.db.cursor().execute(
                "CREATE VIEW study_valid__456 AS SELECT * FROM study_valid__123"
            )

            if target is not None:
                mock_db_config.db.cursor().execute(f"CREATE TABLE {target} (test int);")
            cleaner.clean_study(config=mock_db_config, manifest=manifest, prefix=prefix)
            remaining_tables = (
                mock_db_config.db.cursor()
                .execute("select distinct(table_name) from information_schema.tables")
                .fetchall()
            )

            if any(x in target for x in protected_strs):
                assert (target,) in remaining_tables
            else:
                assert (target,) not in remaining_tables
            assert (
                f"{manifest.get_study_prefix()}__{enums.ProtectedTables.TRANSACTIONS.value}",
            ) in remaining_tables
            if stats:
                assert (
                    f"{manifest.get_study_prefix()}__{enums.ProtectedTables.STATISTICS.value}",
                ) not in remaining_tables
                assert ("study_valid__123",) not in remaining_tables
            else:
                assert (
                    f"{manifest.get_study_prefix()}__{enums.ProtectedTables.STATISTICS.value}",
                ) in remaining_tables
                assert ("study_valid__123",) in remaining_tables
            if not prefix:
                assert ("study_valid__456",) not in remaining_tables


def test_clean_dedicated_schema(mock_db_config):
    with mock.patch.object(builtins, "input", lambda _: False):
        mock_db_config.schema = "dedicated"
        manifest = study_manifest.StudyManifest("./tests/test_data/study_dedicated_schema/")
        mock_db_config.db.cursor().execute("CREATE SCHEMA dedicated")
        builder.run_protected_table_builder(
            config=mock_db_config,
            manifest=manifest,
        )
        mock_db_config.db.cursor().execute("CREATE TABLE dedicated.table_1 (test int)")
        mock_db_config.db.cursor().execute(
            "CREATE VIEW dedicated.view_2 AS SELECT * FROM dedicated.table_1"
        )
        cleaner.clean_study(config=mock_db_config, manifest=manifest)
        remaining_tables = (
            mock_db_config.db.cursor()
            .execute("select distinct(table_name) from information_schema.tables")
            .fetchall()
        )
        assert (f"{enums.ProtectedTables.TRANSACTIONS.value}",) in remaining_tables
        assert ("table_1",) not in remaining_tables
        assert ("view_2",) not in remaining_tables


def test_clean_throws_error_on_missing_params(mock_db_config):
    with pytest.raises(errors.CumulusLibraryError):
        cleaner.clean_study(config=mock_db_config, manifest=None)
