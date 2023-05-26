""" tests for the cli interface to studies """
import json
import pytest
import sysconfig

from contextlib import nullcontext as does_not_raise
from pathlib import Path
from unittest import mock

from cumulus_library import cli


@mock.patch("pyathena.connect")
def test_cli_invalid_study(mock_connect):  # pylint: disable=unused-argument
    with pytest.raises(SystemExit):
        cli.main(cli_args=["-t", "foo"])


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
        builder = cli.main(cli_args=args)
        builder.cursor.execute.assert_called_once()


@mock.patch("pyathena.connect")
@mock.patch("sysconfig.get_path")
@mock.patch("json.load")
@pytest.mark.parametrize(
    "args,raises",
    [
        (["-b", "-t", "core", "--database", "test"], does_not_raise()),
        (["-b", "-t", "study_python_valid", "--database", "test"], does_not_raise()),
        (["-b", "-t", "wrong", "--database", "test"], pytest.raises(SystemExit)),
        (
            [
                "-b",
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
            "study_python_valid": "study_python_valid",
        }
        sysconfig.get_path("purelib")
        builder = cli.main(cli_args=args)
        builder.cursor.execute.assert_called()


@mock.patch("pyathena.connect")
@pytest.mark.parametrize(
    "args,cursor_calls,pandas_cursor_calls",
    [
        (["-t", "vocab", "-b", "--database", "test"], 119, 0),
        (["-t", "core", "-b", "--database", "test"], 21, 0),
        (["-t", "core", "-e", "--database", "test"], 1, 6),
        (["-t", "core", "-e", "-b", "--database", "test"], 21, 6),
        (
            ["-t", "study_valid", "-b", "-s", "tests/test_data/", "--database", "test"],
            4,
            0,
        ),
        (
            [
                "-t",
                "study_valid",
                "-b",
                "-s",
                "tests/test_data/study_valid/",
                "--database",
                "test",
            ],
            4,
            0,
        ),
        (["-t", "core", "-b", "-s", "tests/test_data/", "--database", "test"], 21, 0),
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
        (["-c"], does_not_raise()),
        (["-c", "-p"], pytest.raises(SystemExit)),
        (["-c", "-p", "/tmp/foo"], does_not_raise()),
        (["-c", "-p", "./test_data"], does_not_raise()),
        (["-c", "-p", "./test_data/fakedir"], does_not_raise()),
    ],
)
def test_cli_creates_studies(mock_mkdir, mock_write, args, raises):
    with raises:
        builder = cli.main(cli_args=args)
        assert mock_write.called
