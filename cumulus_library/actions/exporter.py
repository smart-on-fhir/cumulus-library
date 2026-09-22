import contextlib
import pathlib
import tempfile

import cumulus_fhir_support as cfs
import pandas
import rich
from rich.progress import track

from cumulus_library import base_utils, study_manifest
from cumulus_library.template_sql import base_templates

# Database exporting functions


def reset_counts_exports(
    manifest: study_manifest.StudyManifest,
    data_path: cfs.FsPath,
) -> None:
    """
    Removes exports associated with this study from the ../data_export directory.
    """
    path = data_path.joinpath(manifest.get_study_prefix())
    if path.exists():
        # we're just going to remove the count exports - stats exports in
        # subdirectories are left alone by this call
        for file in path.ls(include_dirs=False):
            file.rm()


def export_study(
    config: base_utils.StudyConfig,
    manifest: study_manifest.StudyManifest,
    *,
    data_path: cfs.FsPath,
    archive: bool,
    chunksize: int = 1000000,
) -> None:
    """Exports csvs/parquet extracts of tables listed in export_list
    :param config: a StudyConfig object
    :param manifest: a StudyManifest object
    :keyword data_path: the path to save data - a local path or an S3 URL
    :keyword archive: If true, get all study data and zip with timestamp
    :keyword chunksize: number of rows to export in a single transaction
    """
    skipped_tables = []
    reset_counts_exports(manifest, data_path)
    manifest.materialize_counts_builder_exports()
    if manifest.get_dedicated_schema():
        prefix = f"{manifest.get_dedicated_schema()}."
    else:
        prefix = f"{manifest.get_study_prefix()}__"
    if archive:
        table_query = base_templates.get_show_tables(config.schema, prefix)
        result = config.db.cursor().execute(table_query).fetchall()
        table_list = manifest.get_export_table_list(config.stage)
        for row in result:
            if row[0] not in table_list:
                table_list.append(study_manifest.ManifestExport(name=row[0], export_type="archive"))
    else:
        table_list = manifest.get_export_table_list(config.stage)

    # If the data_path is a local directory, we can just leave the
    # files. Otherwise, if the final goal is upload to S3, we should use
    # a temporary directory.
    if data_path.is_local:
        export_directory = contextlib.nullcontext(str(data_path))
    else:
        export_directory = tempfile.TemporaryDirectory(
            prefix=f"cumulus-export-{manifest.get_study_prefix()}"
        )

    with export_directory as work_directory:
        working_path = pathlib.Path(work_directory)
        path = working_path / manifest.get_study_prefix()
        path.mkdir(parents=True, exist_ok=True)
        for table in track(
            table_list,
            description=f"Exporting {manifest.get_study_prefix()} data...",
        ):
            table.name = base_utils.update_query_if_schema_specified(table.name, manifest)
            file_name = f"{table.name}.{table.export_type}.parquet"
            if config.db.export_table_as_parquet(table.name, file_name, path):
                parquet_path = path / file_name

                df = pandas.read_parquet(parquet_path)
                df = df.sort_values(
                    by=list(df.columns), ascending=False, ignore_index=True, na_position="first"
                )
                df.to_parquet(parquet_path)
                df.to_csv(
                    (parquet_path).with_suffix(".csv"),
                    index=False,
                )
            else:
                skipped_tables.append(table.name)

        if len(skipped_tables) > 0:
            rich.print("The following tables were empty and were not exported:")
            for table in skipped_tables:
                rich.print(f"  - {table}")
        manifest.write_manifest(path)
        base_utils.zip_dir(
            path, working_path, manifest.get_study_prefix(), archive_csvs=archive, zip_subdirs=False
        )

        if not data_path.is_local:
            cfs.FsPath(work_directory).copy(data_path)
            rich.print(f"Exported files to {data_path}")
