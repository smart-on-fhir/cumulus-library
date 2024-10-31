import pathlib

import pyarrow
from pyarrow import csv, parquet
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


def _write_chunk(writer, chunk, arrow_schema):
    writer.write(
        pyarrow.Table.from_pandas(
            chunk.sort_values(by=list(chunk.columns), ascending=False, na_position="first"),
            preserve_index=False,
            schema=arrow_schema,
        )
    )


def export_study(
    config: base_utils.StudyConfig,
    manifest: study_manifest.StudyManifest,
    *,
    data_path: pathlib.Path,
    archive: bool,
    chunksize: int = 1000000,
) -> list:
    """Exports csvs/parquet extracts of tables listed in export_list
    :param config: a StudyConfig object
    :param manifest: a StudyManifest object
    :keyword data_path: the path to the place on disk to save data
    :keyword archive: If true, get all study data and zip with timestamp
    :keyword chunksize: number of rows to export in a single transaction
    :returns: a list of queries, (only for unit tests)
    """
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
    queries = []
    path = pathlib.Path(f"{data_path}/{manifest.get_study_prefix()}/")
    for table in track(
        table_list,
        description=f"Exporting {manifest.get_study_prefix()} data...",
    ):
        query = f"SELECT * FROM {table.name}"  # noqa: S608
        query = base_utils.update_query_if_schema_specified(query, manifest)
        dataframe_chunks, db_schema = config.db.execute_as_pandas(query, chunksize=chunksize)
        path.mkdir(parents=True, exist_ok=True)
        arrow_schema = pyarrow.schema(config.db.col_pyarrow_types_from_sql(db_schema))
        with parquet.ParquetWriter(
            f"{path}/{table.name}.{table.export_type}.parquet", arrow_schema
        ) as p_writer:
            with csv.CSVWriter(
                f"{path}/{table.name}.{table.export_type}.csv",
                arrow_schema,
                write_options=csv.WriteOptions(
                    # Note that this quoting style is not exactly csv.QUOTE_MINIMAL
                    # https://github.com/apache/arrow/issues/42032
                    quoting_style="needed"
                ),
            ) as c_writer:
                for chunk in dataframe_chunks:
                    _write_chunk(p_writer, chunk, arrow_schema)  # pragma: no cover
                    _write_chunk(c_writer, chunk, arrow_schema)  # pragma: no cover
        queries.append(query)
    if archive:
        base_utils.zip_dir(path, data_path, manifest.get_study_prefix())
    return queries
