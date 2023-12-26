""" Collection of small commonly used utility functions """
import datetime
import os
import json
from typing import List
from rich import progress


def filepath(filename: str) -> str:
    return os.path.join(os.path.dirname(__file__), filename)


def load_text(path: str) -> str:
    with open(path, "r", encoding="UTF-8") as fp:
        return fp.read()


def load_json(path: str) -> dict:
    with open(path, "r", encoding="UTF-8") as fp:
        return json.load(fp)


def parse_sql(sql_text: str) -> List[str]:
    commands = []

    for statement in sql_text.split(";"):
        parsed = []
        for line in statement.splitlines():
            if not line.strip().startswith("--"):
                parsed.append(line)
        commands.append("\n".join(parsed))
    return filter_strip(commands)


def filter_strip(commands) -> List[str]:
    return list(filter(None, [c.strip() for c in commands]))


def list_coding(code_display: dict, system=None) -> List[dict]:
    as_list = []
    for code, display in code_display.items():
        if system:
            item = {"code": code, "display": display, "system": system}
        else:
            item = {"code": code, "display": display}
        as_list.append(item)
    return as_list


def query_console_output(
    verbose: bool, query: str, progress_bar: progress.Progress, task: progress.Task
):
    """Convenience function for determining output type"""
    if verbose:
        print()
        print(query)
    else:
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
