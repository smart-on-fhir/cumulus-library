import csv
import pathlib

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


def export_study(
    manifest_parser: study_parser.StudyManifestParser,
    db: databases.DatabaseBackend,
    schema_name: str,
    data_path: pathlib.Path,
    archive: bool,
) -> list:
    """Exports csvs/parquet extracts of tables listed in export_list
    :param db: A database backend
    :param schema_name: the schema/database to target
    :param data_path: the path to the place on disk to save data
    :param archive: If true, get all study data and zip with timestamp
    :returns: a list of queries, (only for unit tests)
    """
    reset_counts_exports(manifest_parser)
    if archive:
        table_query = base_templates.get_show_tables(
            schema_name, f"{manifest_parser.get_study_prefix()}__"
        )
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
        dataframe = db.execute_as_pandas(query)
        path.mkdir(parents=True, exist_ok=True)
        dataframe = dataframe.sort_values(
            by=list(dataframe.columns), ascending=False, na_position="first"
        )
        dataframe.to_csv(f"{path}/{table}.csv", index=False, quoting=csv.QUOTE_MINIMAL)
        dataframe.to_parquet(f"{path}/{table}.parquet", index=False)
        queries.append(queries)
    if archive:
        base_utils.zip_dir(path, data_path, manifest_parser.get_study_prefix())
    return queries
