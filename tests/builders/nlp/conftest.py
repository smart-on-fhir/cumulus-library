"""Fixtures shared by the NLP builder tests."""

from collections.abc import Iterator

import pytest

from cumulus_library import note_utils
from tests.nlp_utils import add_dxr


@pytest.fixture(autouse=True)
def _autouse_cache_dir(mock_cache_dir):
    """
    Keeps NLP's on-disk cache inside tmp_path for every NLP test.
    Delegates to the shared ``mock_cache_dir`` fixture defined in tests/conftest.py.
    """
    yield


@pytest.fixture
def note_source(tmp_path) -> Iterator[note_utils.NoteSource]:
    """Just make a sample note source with a row - contents not important"""
    with open(f"{tmp_path}/dxr.ndjson", "w", encoding="utf8") as f:
        add_dxr("hello", "hello world", f)
    yield note_utils.NoteSource([tmp_path])
