""" tests for the cli interface to studies """
import builtins
import glob
import os
import sysconfig

from contextlib import nullcontext as does_not_raise
from pathlib import Path, PosixPath
from unittest import mock

import pytest
import requests
import requests_mock
import toml

from cumulus_library import cli
from cumulus_library.databases import DuckDatabaseBackend
from tests.conftest import duckdb_args


class MockVocabBsv:
    """mock class for patching test BSVs for the vocab study"""

    builtin_open = open

    def open(self, *args, **kwargs):
        if str(args[0]).endswith(".bsv"):
            print(args)
            args = (
                PosixPath(
                    f"./tests/test_data/mock_bsvs/{str(args[0]).rsplit('/', maxsplit=1)[-1]}"
                ),
                "r",
            )
        return self.builtin_open(*args, **kwargs)


@mock.patch.dict(
    os.environ,
    clear=True,
)
def test_cli_invalid_study(tmp_path):
    with pytest.raises(SystemExit):
        args = duckdb_args(["build", "-t", "foo"], tmp_path)
        cli.main(cli_args=args)


@mock.patch.dict(
    os.environ,
    clear=True,
)
@pytest.mark.parametrize(
    "args",
    [
        ([]),
        (["-t", "all"]),
    ],
)
def test_cli_early_exit(args):
    with pytest.raises(SystemExit):
        cli.main(cli_args=args)


@mock.patch.dict(
    os.environ,
    clear=True,
)
@mock.patch("sysconfig.get_path")
@mock.patch("json.load")
@pytest.mark.parametrize(
    "args,raises,expected",
    [
        (
            [
                "build",
                "-t",
                "core",
            ],
            does_not_raise(),
            "core__condition",
        ),
        (
            ["build", "-t", "study_python_valid"],
            does_not_raise(),
            "study_python_valid__table",
        ),
        (["build", "-t", "wrong"], pytest.raises(SystemExit), None),
        (
            [
                "build",
                "-t",
                "study_valid",
                "-s",
                f"{Path(__file__).resolve().parents[0]}/test_data/study_valid",
                "--database",
                "test",
            ],
            does_not_raise(),
            "study_valid__table",
        ),
    ],
)
def test_cli_path_mapping(
    mock_load_json, mock_path, tmp_path, args, raises, expected
):  # pylint: disable=unused-argument
    with raises:
        mock_path.return_value = f"{Path(__file__).resolve().parents[0]}" "/test_data/"
        mock_load_json.return_value = {
            "__desc__": "",
            "allowlist": {
                "study_python_valid": "study_python_valid",
            },
        }
        args = duckdb_args(args, tmp_path)
        cli.main(cli_args=args)
        db = DuckDatabaseBackend(f"{tmp_path}/duck.db")
        assert (expected,) in db.cursor().execute("show tables").fetchall()


@mock.patch.dict(
    os.environ,
    clear=True,
)
@mock.patch("sysconfig.get_path")
def test_count_builder_mapping(mock_path, tmp_path):
    mock_path.return_value = f"{Path(__file__).resolve().parents[0]}" "/test_data/"
    with does_not_raise():
        args = duckdb_args(
            [
                "build",
                "-t",
                "study_python_counts_valid",
                "-s",
                "./tests/test_data",
                "--database",
                "test",
            ],
            tmp_path,
        )
        cli.main(cli_args=args)
        db = DuckDatabaseBackend(f"{tmp_path}/duck.db")
        assert [
            ("study_python_counts_valid__lib_transactions",),
            ("study_python_counts_valid__table1",),
            ("study_python_counts_valid__table2",),
        ] == db.cursor().execute("show tables").fetchall()


@mock.patch.dict(
    os.environ,
    clear=True,
)
@mock.patch("sysconfig.get_path")
@pytest.mark.parametrize(
    "args,expected",
    [
        (
            [
                "clean",
                "-t",
                "core",
            ],
            "core__",
        ),
        (
            [
                "clean",
                "--prefix",
                "-t",
                "foo",
            ],
            "foo",
        ),
        (
            [
                "clean",
                "-t",
                "core",
                "--statistics",
            ],
            "core__",
        ),
    ],
)
def test_clean(mock_path, tmp_path, args, expected):  # pylint: disable=unused-argument
    mock_path.return_value = f"{Path(__file__).resolve().parents[0]}" "/test_data/"
    cli.main(
        cli_args=duckdb_args(["build", "-t", "core", "--database", "test"], tmp_path)
    )
    with does_not_raise():
        with mock.patch.object(builtins, "input", lambda _: "y"):
            cli.main(
                cli_args=args
                + ["--db-type", "duckdb", "--database", f"{tmp_path}/duck.db"]
            )
            db = DuckDatabaseBackend(f"{tmp_path}/duck.db")
            for table in db.cursor().execute("show tables").fetchall():
                assert expected not in table


@mock.patch("builtins.open", MockVocabBsv().open)
@mock.patch.dict(
    os.environ,
    clear=True,
)
@pytest.mark.parametrize(
    "build_args,export_args,expected_tables",
    [
        (["build", "-t", "core"], ["export", "-t", "core"], 38),
        (
            [  # checking that a study is loaded from a child directory of a user-defined path
                "build",
                "-t",
                "study_valid",
                "-s",
                "tests/test_data/",
            ],
            ["export", "-t", "study_valid", "-s", "tests/test_data/"],
            2,
        ),
        (["build", "-t", "vocab"], None, 3),
        (
            [  # checking that a study is loaded from the directory of a user-defined path.
                # we're also validating that the CLI accpes the statistics keyword, though
                "build",
                "-t",
                "study_valid",
                "-s",
                "tests/test_data/study_valid/",
                "--statistics",
            ],
            ["export", "-t", "study_valid", "-s", "tests/test_data/study_valid/"],
            2,
        ),
    ],
)
def test_cli_executes_queries(tmp_path, build_args, export_args, expected_tables):
    with does_not_raise():
        build_args = duckdb_args(build_args, tmp_path)
        cli.main(cli_args=build_args)
        if export_args is not None:
            export_args = duckdb_args(export_args, tmp_path)
            cli.main(cli_args=export_args)

        db = DuckDatabaseBackend(f"{tmp_path}/duck.db")
        found_tables = db.cursor().execute("show tables").fetchall()
        assert len(found_tables) == expected_tables
        for table in found_tables:
            # If a table was created by this run, check it has the study prefix
            if "__" in table[0]:
                assert build_args[2] in table[0]

        if export_args is not None:
            # Expected length if not specifying a study dir
            if len(build_args) == 9:
                manifest_dir = cli.get_study_dict([])[build_args[2]]
            else:
                manifest_dir = cli.get_study_dict(
                    [cli.get_abs_posix_path(build_args[4])]
                )[build_args[2]]

            with open(f"{manifest_dir}/manifest.toml", encoding="UTF-8") as file:
                config = toml.load(file)
            csv_files = glob.glob(f"{tmp_path}/counts/{build_args[2]}/*.csv")
            for export_table in config["export_config"]["export_list"]:
                assert any(export_table in x for x in csv_files)


@mock.patch.dict(
    os.environ,
    clear=True,
)
def test_cli_stats_rebuild(tmp_path):
    """Validates statistics build behavior

    Since this is a little obtuse - we are checking:
    - that stats builds run at all
    - that a results table is created on the first run
    - that a results table is :not: created on the second run
    - that a results table is created when we explicitly ask for one with a CLI flag
    """

    cli.main(
        cli_args=duckdb_args(
            ["build", "-t", "core", "--database", "test"], tmp_path, stats=True
        )
    )
    arg_list = [
        "build",
        "-s",
        "./tests/test_data",
        "-t",
        "psm",
        "--db-type",
        "duckdb",
        "--database",
        f"{tmp_path}/duck.db",
    ]
    cli.main(cli_args=arg_list + [f"{tmp_path}/export"])
    cli.main(cli_args=arg_list + [f"{tmp_path}/export"])
    cli.main(cli_args=arg_list + [f"{tmp_path}/export", "--statistics"])
    db = DuckDatabaseBackend(f"{tmp_path}/duck.db")
    expected = (
        db.cursor()
        .execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_name LIKE 'psm_test__psm_encounter_covariate_%'"
        )
        .fetchall()
    )
    assert len(expected) == 2


@mock.patch.dict(
    os.environ,
    clear=True,
)
def test_cli_creates_study(tmp_path):
    cli.main(cli_args=["create", f"{tmp_path}/studydir/"])
    with open(
        "./cumulus_library/studies/template/manifest.toml", encoding="UTF-8"
    ) as file:
        source = toml.load(file)
    with open(f"{tmp_path}/studydir/manifest.toml", encoding="UTF-8") as file:
        target = toml.load(file)
    assert source == target


@mock.patch.dict(
    os.environ,
    clear=True,
)
@mock.patch("pathlib.Path.glob")
@pytest.mark.parametrize(
    "args,status,login_error,raises",
    [
        (["upload"], 204, False, pytest.raises(SystemExit)),
        (["upload", "--user", "user", "--id", "id"], 204, False, does_not_raise()),
        (
            ["upload", "--user", "user", "--id", "id"],
            500,
            False,
            pytest.raises(requests.exceptions.RequestException),
        ),
        (
            ["upload", "--user", "baduser", "--id", "badid"],
            204,
            True,
            pytest.raises(requests.exceptions.RequestException),
        ),
        (
            ["upload", "--user", "user", "--id", "id", "./foo"],
            204,
            False,
            does_not_raise(),
        ),
    ],
)
def test_cli_upload_studies(mock_glob, args, status, login_error, raises):
    mock_glob.side_effect = [
        [Path(__file__)],
        [Path(str(Path(__file__).parent) + "/test_data/count_synthea_patient.parquet")],
    ]
    with raises:
        with requests_mock.Mocker() as r:
            if login_error:
                r.post("https://aggregator.smartcumulus.org/upload/", status_code=401)
            else:
                r.post(
                    "https://aggregator.smartcumulus.org/upload/",
                    json={"url": "https://presigned.url.org", "fields": {"a": "b"}},
                )
            r.post("https://presigned.url.org", status_code=status)
            cli.main(cli_args=args)


@pytest.mark.parametrize(
    "args,calls",
    [
        (["upload", "--user", "user", "--id", "id", "./foo"], 2),
        (["upload", "--user", "user", "--id", "id", "./foo", "-t", "test_data"], 1),
        (["upload", "--user", "user", "--id", "id", "./foo", "-t", "not_found"], 0),
    ],
)
@mock.patch.dict(
    os.environ,
    clear=True,
)
@mock.patch("pathlib.Path.glob")
@mock.patch("cumulus_library.upload.upload_data")
def test_cli_upload_filter(mock_upload_data, mock_glob, args, calls):
    mock_glob.side_effect = [
        [
            Path(
                str(Path(__file__).parent) + "/test_data/count_synthea_patient.parquet"
            ),
            Path(
                str(Path(__file__).parent) + "/other_data/count_synthea_patient.parquet"
            ),
        ],
    ]
    cli.main(cli_args=args)
    if len(mock_upload_data.call_args_list) == 1:
        target = args[args.index("-t") + 1]
        # filepath is in the third argument position in the upload data arg list
        assert target in str(mock_upload_data.call_args[0][2])
    assert mock_upload_data.call_count == calls
