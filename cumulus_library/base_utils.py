"""Collection of small commonly used utility functions"""

import dataclasses
import datetime
import pathlib
import shutil
import zipfile
from contextlib import contextmanager

import rich
from rich import progress

from cumulus_library import databases, study_manifest


@dataclasses.dataclass
class StudyConfig:
    """Class for containing study-centric parameters

    The intent of this class is that if you want something passed through to the
    prepare_queries section of a study, this is the place it should go. If you're
    doing something above that level, consider explicit arguments instead. This should
    be an interface aimed at a study author.

    :param db: a databaseBackend object for a specific target database
    :param schema: the database schema specified at the command line
    :keyword drop_table: when creating tables, if one already exists in the db,
        drop it
    :keyword force_upload: If the study downloads data from an external resource,
        force it to skip any cached data when running
    :keyword verbose: if True, print raw queries to the cli instead of progress bars
    :keyword stats_build: If the study runs a stats builder, force regeneration of
        any sampling or other stochastic techniques
    :keyword umls_key: An API for the UMLS service, used for downloading vocabularies
    :keyword umls: A UMLS API key
    :keyword options: a dictionary for any study-specific CLI arguments
    """

    db: databases.DatabaseBackend
    schema: str
    drop_table: bool = False
    force_upload: bool = False
    verbose: bool = False
    stats_build: bool = False
    stats_clean: bool = False
    umls_key: str | None = None
    options: dict | None = None


def get_schema(config: StudyConfig, manifest: study_manifest.StudyManifest):
    if dedicated := manifest.get_dedicated_schema():
        config.db.create_schema(dedicated)
        return dedicated
    return config.schema


def load_text(path: str) -> str:
    with open(path, encoding="UTF-8") as fp:
        return fp.read()


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


@contextmanager
def query_console_output(
    verbose: bool, query: str, progress_bar: progress.Progress, task: progress.Task
):
    """Convenience context manager for handling console output"""
    if verbose:
        rich.print()
        rich.print(query)
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
    return datetime.datetime.now(datetime.UTC).replace(microsecond=0)


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


def update_query_if_schema_specified(query: str, manifest: study_manifest.StudyManifest):
    if manifest and manifest.get_dedicated_schema():
        # External queries in athena require a schema to be specified already, so
        # rather than splitting and ending up with a table name like
        # `schema`.`schema.table`, we just remove the `schema__` chunk from the
        # table name.
        # TODO: Move this to be a database defined function.
        if "CREATE EXTERNAL" in query:
            query = query.replace(
                f"{manifest.get_study_prefix()}__",
                "",
            )
        else:
            query = query.replace(
                f"{manifest.get_study_prefix()}__",
                f"{manifest.get_dedicated_schema()}.",
            )
    return query


def unzip_file(file_path: pathlib.Path, write_path: pathlib.Path):
    """Expands a zip archive"""
    with zipfile.ZipFile(file_path, mode="r") as z:
        for file in z.namelist():
            z.extract(file, write_path)
