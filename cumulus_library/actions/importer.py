import pathlib
import tempfile
import zipfile

import pandas

from cumulus_library import base_utils, errors
from cumulus_library.actions import cleaner
from cumulus_library.template_sql import base_templates


def create_table_from_parquet(archive, file, study_name, db, schema):
    try:
        parquet_path = pathlib.Path(
            archive.extract(file), path=tempfile.TemporaryFile()
        )
        # While convenient to access, this exposes us to panda's type system,
        # which is messy - this could be optionally be replaced by pyarrow if it
        # becomes problematic.
        table_types = pandas.read_parquet(parquet_path).dtypes
        remote_types = db.col_parquet_types_from_pandas(table_types.values)
        s3_path = db.upload_file(
            file=parquet_path,
            study=study_name,
            topic=parquet_path.stem,
            force_upload=True,
            remote_filename=parquet_path.name,
        )
        query = base_templates.get_ctas_from_parquet_query(
            schema_name=schema,
            table_name=parquet_path.stem.replace(".", "_"),
            local_location=parquet_path.parent,
            remote_location=s3_path,
            table_cols=list(table_types.index),
            remote_table_cols_types=remote_types,
        )
        db.cursor().execute(query)
    finally:
        parquet_path.unlink()


def import_archive(
    config: base_utils.StudyConfig, archive_path: pathlib.Path, args: dict
):
    """Creates a study in the database from a previous export"""

    # Ensure we've got something that looks like a valid database export
    if not archive_path.exists():
        raise errors.StudyImportError(f"File {archive_path} not found.")
    try:
        archive = zipfile.ZipFile(archive_path)
        files = archive.namelist()
        files = [file for file in files if file.endswith(".parquet")]
    except zipfile.BadZipFile as e:
        raise errors.StudyImportError(
            f"File {archive_path} is not a valid archive."
        ) from e
    if len(files) == 0:
        raise errors.StudyImportError(
            f"File {archive_path} does not contain any tables."
        )
    if not any("__" in file for file in files):
        raise errors.StudyImportError(
            f"File {archive_path} contains non-study parquet files."
        )
    study_name = files[0].split("__")[0]
    for file in files[1:]:
        if file.split("__")[0] != study_name:
            raise errors.StudyImportError(
                f"File {archive_path} contains data from more than one study."
            )

    # Clean and rebuild from the provided archive
    cleaner.clean_study(
        manifest_parser=None,
        cursor=config.db.cursor(),
        schema_name=args["schema_name"],
        verbose=args["verbose"],
        prefix=study_name,
    )
    with base_utils.get_progress_bar(disable=args["verbose"]) as progress:
        task = progress.add_task(
            f"Recreating {study_name} from archive...",
            total=len(files),
            visible=not args["verbose"],
        )
        for file in files:
            create_table_from_parquet(
                archive, file, study_name, config.db, args["schema_name"]
            )
            progress.advance(task)
