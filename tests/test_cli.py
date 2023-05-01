import pytest

from unittest import mock

import botocore

from cumulus_library import cli

"""
def test_cli_invalid_study():
    with pytest.raises(SystemExit):
        builder = cli.main(cli_args=["-t", "foo"])
"""


@mock.patch("pyathena.connect")
@pytest.mark.parametrize(
    "args",
    [
        ([]),
        (["-t", "all"]),
    ],
)
def test_cli_no_reads_or_writes(mock_connect, args):
    builder = cli.main(cli_args=args)
    builder.cursor.execute.assert_called_once()


@mock.patch("pyathena.connect")
@pytest.mark.parametrize(
    "args,cursor_calls,pandas_cursor_calls",
    [
        (["-t", "all", "-b", "-d", "test"], 139, 0),
        (["-t", "vocab", "-b", "-d", "test"], 119, 0),
        (["-t", "core", "-b", "-d", "test"], 21, 0),
        (["-t", "all", "-e", "-d", "test"], 1, 7),
        (["-t", "core", "-e", "-d", "test"], 1, 6),
        (["-t", "core", "-e", "-b", "-d", "test"], 21, 6),
    ],
)
def test_cli_executes_queries(mock_connect, args, cursor_calls, pandas_cursor_calls):
    mock_connect.side_effect = [mock.MagicMock(), mock.MagicMock()]
    builder = cli.main(cli_args=args)
    assert builder.cursor.execute.call_count == cursor_calls
    assert builder.pandas_cursor.execute.call_count == pandas_cursor_calls
