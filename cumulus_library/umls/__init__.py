import os
from typing import List
from cumulus_library.helper import load_text, parse_sql


def relpath(filename: str) -> str:
    return os.path.join(os.path.dirname(__file__), filename)


def list_sql(file_sql: str) -> List[str]:
    return parse_sql(load_text(relpath(file_sql)))
