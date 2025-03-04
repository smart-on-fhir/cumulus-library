import pathlib

import pandas
import rich
from rich.progress import track

from cumulus_library import base_utils, study_manifest
from cumulus_library.template_sql import base_templates

# Database exporting functions


def reset_counts_exports(
    manifest: study_manifest.StudyManifest,
) -> None:
    """
    Removes exports associated with this study from the ../data_export directory.
    """
    path = pathlib.Path(f"{manifest.data_path}/{manifest.get_study_prefix()}")
    if path.exists():
        # we're just going to remove the count exports - stats exports in
        # subdirectories are left alone by this call
        for file in path.glob("*.*"):
            file.unlink()


def export_study(
    config: base_utils.StudyConfig,
    manifest: study_manifest.StudyManifest,
    *,
    data_path: pathlib.Path,
    archive: bool,
    chunksize: int = 1000000,
) -> None:
    """Exports csvs/parquet extracts of tables listed in export_list
    :param config: a StudyConfig object
    :param manifest: a StudyManifest object
    :keyword data_path: the path to the place on disk to save data
    :keyword archive: If true, get all study data and zip with timestamp
    :keyword chunksize: number of rows to export in a single transaction
    """

    skipped_tables = []
    reset_counts_exports(manifest)
    if manifest.get_dedicated_schema():
        prefix = f"{manifest.get_dedicated_schema()}."
    else:
        prefix = f"{manifest.get_study_prefix()}__"
    if archive:
        table_query = base_templates.get_show_tables(config.schema, prefix)
        result = config.db.cursor().execute(table_query).fetchall()
        table_list = manifest.get_export_table_list()
        for row in result:
            if row[0] not in table_list:
                table_list.append(study_manifest.ManifestExport(name=row[0], export_type="archive"))
    else:
        table_list = manifest.get_export_table_list()
    path = pathlib.Path(f"{data_path}/{manifest.get_study_prefix()}/")
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
    if archive:
        base_utils.zip_dir(path, data_path, manifest.get_study_prefix())
