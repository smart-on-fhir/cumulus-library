"""Local caching of NLP results, to avoid future calls"""

import hashlib
import threading
from collections.abc import Callable
from typing import TypeVar

import cumulus_fhir_support as cfs

Obj = TypeVar("Obj")

# Writes have to be serialized. Every FsPath write made while NLP workers are running must
# take this lock - see the writers in driver.py as well.
FS_WRITE_LOCK = threading.Lock()


def _cache_metadata_path(cache_dir: cfs.FsPath, namespace: str, filename: str) -> cfs.FsPath:
    return cache_dir.joinpath(f"nlp-cache/{namespace}/{filename}")


def cache_metadata_write(cache_dir: cfs.FsPath, namespace: str, content: dict) -> None:
    path = _cache_metadata_path(cache_dir, namespace, "metadata.json")
    with FS_WRITE_LOCK:
        path.parent.makedirs()
        path.write_json(content, indent=2)


def cache_metadata_read(cache_dir: cfs.FsPath, namespace: str) -> dict:
    path = _cache_metadata_path(cache_dir, namespace, "metadata.json")
    return path.read_json(default={})


def _cache_path(cache_dir: cfs.FsPath, namespace: str, checksum: str) -> cfs.FsPath:
    return cache_dir.joinpath(f"nlp-cache/{namespace}/{checksum[0:4]}/sha256-{checksum}.cache")


def cache_checksum(note_text: str) -> str:
    return hashlib.sha256(note_text.encode("utf8"), usedforsecurity=False).hexdigest()


def cache_write(cache_dir: cfs.FsPath, namespace: str, checksum: str, content: str) -> None:
    path = _cache_path(cache_dir, namespace, checksum)
    with FS_WRITE_LOCK:
        path.parent.makedirs()
        path.write_text(content)


def cache_read(cache_dir: cfs.FsPath, namespace: str, checksum: str) -> str | None:
    path = _cache_path(cache_dir, namespace, checksum)
    return path.read_text(default=None)


def cache_wrapper(
    cache_dir: cfs.FsPath,
    namespace: str,
    checksum: str,
    from_file: Callable[[str], Obj],
    to_file: Callable[[Obj], str],
    method: Callable,
    *args,
    **kwargs,
) -> tuple[Obj, bool]:
    """Looks up an NLP result in the cache first, falling back to calling the NLP method.

    Returns the result along with whether it came from the cache, so callers can tell a note
    that cost money from one that was free.
    """
    result = cache_read(cache_dir, namespace, checksum)

    if result is None:
        result = method(*args, **kwargs)
        cache_write(cache_dir, namespace, checksum, to_file(result))
        return result, False

    return from_file(result), True
