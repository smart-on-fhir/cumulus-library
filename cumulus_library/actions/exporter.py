import pathlib

import pyarrow
from pyarrow import csv, parquet
from rich.progress import track

from cumulus_library import base_utils, databases, study_parser
from cumulus_library.template_sql import base_templates

# Database exporting functions


def reset_counts_exports(
    manifest_parser: study_parser.StudyManifestParser,
) -> None:
    """
    Removes exports associated with this study from the ../data_export directory.
    """
    path = pathlib.Path(
        f"{manifest_parser.data_path}/{manifest_parser.get_study_prefix()}"
    )
    if path.exists():
        # we're just going to remove the count exports - stats exports in
        # subdirectories are left alone by this call
        for file in path.glob("*.*"):
            file.unlink()


def _write_chunk(writer, chunk, schema):
    writer.write(
        pyarrow.Table.from_pandas(
            chunk.sort_values(
                by=list(chunk.columns), ascending=False, na_position="first"
            ),
            preserve_index=False,
            schema=schema,
        )
    )


def export_study(
    manifest_parser: study_parser.StudyManifestParser,
    db: databases.DatabaseBackend,
    schema_name: str,
    data_path: pathlib.Path,
    archive: bool,
    chunksize: int = 1000000,
) -> list:
    """Exports csvs/parquet extracts of tables listed in export_list
    :param db: A database backend
    :param schema_name: the schema/database to target
    :param data_path: the path to the place on disk to save data
    :param archive: If true, get all study data and zip with timestamp
    :returns: a list of queries, (only for unit tests)
    """
    reset_counts_exports(manifest_parser)
    if manifest_parser.get_dedicated_schema():
        prefix = f"{manifest_parser.get_dedicated_schema()}."
    else:
        prefix = f"{manifest_parser.get_study_prefix()}__"
    if archive:
        table_query = base_templates.get_show_tables(schema_name, prefix)
        result = db.cursor().execute(table_query).fetchall()
        table_list = [row[0] for row in result]
    else:
        table_list = manifest_parser.get_export_table_list()

    queries = []
    path = pathlib.Path(f"{data_path}/{manifest_parser.get_study_prefix()}/")
    for table in track(
        table_list,
        description=f"Exporting {manifest_parser.get_study_prefix()} data...",
    ):
        query = f"SELECT * FROM {table}"
        query = base_utils.update_query_if_schema_specified(query, manifest_parser)
        dataframe_chunks, db_schema = db.execute_as_pandas(query, chunksize=chunksize)
        path.mkdir(parents=True, exist_ok=True)
        schema = pyarrow.schema(db.col_pyarrow_types_from_sql(db_schema))
        with parquet.ParquetWriter(f"{path}/{table}.parquet", schema) as p_writer:
            with csv.CSVWriter(
                f"{path}/{table}.csv",
                schema,
                write_options=csv.WriteOptions(
                    # Note that this quoting style is not exactly csv.QUOTE_MINIMAL
                    # https://github.com/apache/arrow/issues/42032
                    quoting_style="needed"
                ),
            ) as c_writer:
                for chunk in dataframe_chunks:
                    _write_chunk(p_writer, chunk, schema)  # pragma: no cover
                    _write_chunk(c_writer, chunk, schema)  # pragma: no cover
        queries.append(query)
    if archive:
        base_utils.zip_dir(path, data_path, manifest_parser.get_study_prefix())
    return queries
