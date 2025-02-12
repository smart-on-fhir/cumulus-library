import pathlib
import tempfile
import zipfile

import pandas

from cumulus_library import base_utils, errors
from cumulus_library.actions import cleaner
from cumulus_library.template_sql import base_templates


def _create_table_from_parquet(archive, file, study_name, config):
    try:
        parquet_path = pathlib.Path(archive.extract(file), path=tempfile.TemporaryFile())
        # While convenient to access, this exposes us to panda's type system,
        # which is messy - this could be optionally be replaced by pyarrow if it
        # becomes problematic.
        table_types = pandas.read_parquet(parquet_path).dtypes
        remote_types = config.db.col_parquet_types_from_pandas(table_types.values)
        s3_path = config.db.upload_file(
            file=parquet_path,
            study=study_name,
            topic=parquet_path.stem,
            force_upload=True,
            remote_filename=parquet_path.name,
        )
        query = base_templates.get_ctas_from_parquet_query(
            schema_name=config.schema,
            table_name=parquet_path.stem.replace(".", "_"),
            local_location=f"{parquet_path.parent}/*.parquet",
            remote_location=s3_path,
            table_cols=list(table_types.index),
            remote_table_cols_types=remote_types,
        )
        config.db.cursor().execute(query)
    finally:
        parquet_path.unlink()


def import_archive(config: base_utils.StudyConfig, *, archive_path: pathlib.Path):
    """Creates a study in the database from a previous export

    :param config: a StudyConfig object
    :keyword archive_path: the location of the archive to import data from

    """

    # Ensure we've got something that looks like a valid database export
    if not archive_path.exists():
        raise errors.StudyImportError(f"File {archive_path} not found.")
    try:
        archive = zipfile.ZipFile(archive_path)
        files = archive.namelist()
        files = [file for file in files if file.endswith(".parquet")]
    except zipfile.BadZipFile as e:
        raise errors.StudyImportError(f"File {archive_path} is not a valid archive.") from e
    if not any("__" in file for file in files):
        raise errors.StudyImportError(f"File {archive_path} contains non-study parquet files.")
    study_name = files[0].split("__")[0]
    for file in files[1:]:
        if file.split("__")[0] != study_name:
            raise errors.StudyImportError(
                f"File {archive_path} contains data from more than one study."
            )

    # Clean and rebuild from the provided archive
    cleaner.clean_study(config=config, manifest=None, prefix=study_name)
    with base_utils.get_progress_bar(disable=config.verbose) as progress:
        task = progress.add_task(
            f"Recreating {study_name} from archive...",
            total=len(files),
            visible=not config.verbose,
        )
        for file in files:
            _create_table_from_parquet(archive, file, study_name, config)
            progress.advance(task)
