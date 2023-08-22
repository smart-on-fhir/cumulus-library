""" tests for the cli interface to studies """
import os
import sysconfig

from contextlib import nullcontext as does_not_raise
from pathlib import Path
from unittest import mock

import pytest
import requests
import requests_mock

from cumulus_library import cli


@mock.patch("pyathena.connect")
def test_cli_invalid_study(mock_connect):  # pylint: disable=unused-argument
    with pytest.raises(SystemExit):
        cli.main(cli_args=["build", "-t", "foo"])


@mock.patch("pyathena.connect")
@pytest.mark.parametrize(
    "args",
    [
        ([]),
        (["-t", "all"]),
    ],
)
def test_cli_no_reads_or_writes(mock_connect, args):  # pylint: disable=unused-argument
    with pytest.raises(SystemExit):
        cli.main(cli_args=args)


@mock.patch("pyathena.connect")
@mock.patch("sysconfig.get_path")
@mock.patch("json.load")
@pytest.mark.parametrize(
    "args,raises",
    [
        (["build", "-t", "core", "--database", "test"], does_not_raise()),
        (["build", "-t", "study_python_valid", "--database", "test"], does_not_raise()),
        (["build", "-t", "wrong", "--database", "test"], pytest.raises(SystemExit)),
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
        ),
    ],
)
def test_cli_path_mapping(
    mock_load_json, mock_path, mock_connect, args, raises
):  # pylint: disable=unused-argument
    with raises:
        mock_path.return_value = f"{Path(__file__).resolve().parents[0]}" "/test_data/"
        mock_load_json.return_value = {
            "__desc__": "",
            "allowlist": {
                "study_python_valid": "study_python_valid",
            },
        }
        sysconfig.get_path("purelib")
        builder = cli.main(cli_args=args)
        builder.cursor.execute.assert_called()


@mock.patch("pyathena.connect")
def test_count_builder_mapping(mock_connect):  # pylint: disable=unused-argument
    with does_not_raise():
        builder = cli.main(
            cli_args=[
                "build",
                "-t",
                "study_python_counts_valid",
                "-s" "./tests/test_data",
            ]
        )
        builder.cursor.execute.assert_called()


@mock.patch("pyathena.connect")
@pytest.mark.parametrize(
    "args,cursor_calls,pandas_cursor_calls",
    [
        (["build", "-t", "vocab", "--database", "test"], 344, 0),
        (["build", "-t", "core", "--database", "test"], 47, 0),
        (["export", "-t", "core", "--database", "test"], 1, 10),
        (
            [
                "build",
                "-t",
                "study_valid",
                "-s",
                "tests/test_data/",
                "--database",
                "test",
            ],
            4,
            0,
        ),
        (
            [
                "build",
                "-t",
                "study_valid",
                "-s",
                "tests/test_data/study_valid/",
                "--database",
                "test",
            ],
            4,
            0,
        ),
        (
            ["build", "-t", "core", "-s", "tests/test_data/", "--database", "test"],
            47,
            0,
        ),
        (
            [
                "export",
                "-t",
                "core",
                "--database",
                "test",
                "cumulus_library/data_export",
            ],
            1,
            10,
        ),
    ],
)
def test_cli_executes_queries(mock_connect, args, cursor_calls, pandas_cursor_calls):
    mock_connect.side_effect = [mock.MagicMock(), mock.MagicMock()]
    builder = cli.main(cli_args=args)
    assert builder.cursor.execute.call_count == cursor_calls
    assert builder.pandas_cursor.execute.call_count == pandas_cursor_calls


@mock.patch("pathlib.PosixPath.mkdir")
@mock.patch("pathlib.PosixPath.write_bytes")
@pytest.mark.parametrize(
    "args,raises",
    [
        (["create"], does_not_raise()),
        (["create", "/tmp/foo"], does_not_raise()),
        (["create", "./test_data"], does_not_raise()),
        (["create", "./test_data/fakedir"], does_not_raise()),
    ],
)
def test_cli_creates_studies(
    mock_mkdir, mock_write, args, raises
):  # pylint: disable=unused-argument
    with raises:
        cli.main(cli_args=args)
        assert mock_write.called


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
        [Path(str(Path(__file__)) + "/test_data/count_synthea_patient.parquet")],
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
