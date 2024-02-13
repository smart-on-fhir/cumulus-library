""" Collection of small commonly used utility functions """

import datetime
import json
import os
import shutil
import zipfile
from contextlib import contextmanager

from rich import progress


def filepath(filename: str) -> str:
    return os.path.join(os.path.dirname(__file__), filename)


def load_text(path: str) -> str:
    with open(path, encoding="UTF-8") as fp:
        return fp.read()


def load_json(path: str) -> dict:
    with open(path, encoding="UTF-8") as fp:
        return json.load(fp)


def parse_sql(sql_text: str) -> list[str]:
    commands = []

    for statement in sql_text.split(";"):
        parsed = []
        for line in statement.splitlines():
            if not line.strip().startswith("--"):
                parsed.append(line)
        commands.append("\n".join(parsed))
    return filter_strip(commands)


def filter_strip(commands) -> list[str]:
    return list(filter(None, [c.strip() for c in commands]))


def list_coding(code_display: dict, system=None) -> list[dict]:
    as_list = []
    for code, display in code_display.items():
        if system:
            item = {"code": code, "display": display, "system": system}
        else:
            item = {"code": code, "display": display}
        as_list.append(item)
    return as_list


@contextmanager
def query_console_output(
    verbose: bool, query: str, progress_bar: progress.Progress, task: progress.Task
):
    """Convenience context manager for handling console output"""
    if verbose:
        print()
        print(query)
    yield
    if not verbose:
        progress_bar.advance(task)


def get_progress_bar(**kwargs) -> progress.Progress:
    # The default columns don't change to elapsed time when finished.
    return progress.Progress(
        progress.TextColumn("[progress.description]{task.description}"),
        progress.BarColumn(),
        progress.TaskProgressColumn(),
        progress.TimeElapsedColumn(),
        **kwargs,
    )


def get_utc_datetime() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)


def get_tablename_safe_iso_timestamp() -> str:
    """formats a timestamp to remove sql unallowed characters in table names"""
    iso_timestamp = get_utc_datetime().isoformat()
    safe_timestamp = iso_timestamp.replace(":", "_").replace("-", "_").replace("+", "_")
    return safe_timestamp


def zip_dir(read_path, write_path, archive_name):
    """Moves a directory to an archive"""
    file_list = [file for file in read_path.glob("**/*") if file.is_file()]
    timestamp = get_utc_datetime().isoformat().replace("+00:00", "Z")
    with zipfile.ZipFile(
        f"{write_path}/{archive_name}_{timestamp}.zip",
        "w",
        zipfile.ZIP_DEFLATED,
    ) as f:
        for file in file_list:
            f.write(file, file.relative_to(read_path))
            file.unlink()
        shutil.rmtree(read_path)
