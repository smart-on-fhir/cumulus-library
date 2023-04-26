from pathlib import Path
from typing import List

import toml

from pyathena.cursor import Cursor
from rich.progress import Progress, track

from cumulus_library.helper import query_console_output, load_text, parse_sql
from cumulus_library.template_sql.templates import (
    get_show_tables,
    get_show_views,
    get_drop_view_table,
)


class StudyManifestParsingError(Exception):
    pass


class StudyManifestParser:
    study_path = None
    study_config = None

    def __init__(self, study_path: [Path, None] = None):
        if study_path is not None:
            self.load_study_manifest(study_path)

    ### toml parsing helper functions
    def load_study_manifest(self, study_path: Path):
        try:
            with open(f"{study_path}/manifest.toml") as file:
                self.study_config = toml.load(file)
            self.study_path = study_path
        except FileNotFoundError:
            raise StudyManifestParsingError(f"No manifest found at {study_path}")

    def get_study_prefix(self):
        if prefix_list := self.study_config["study_prefix"]:
            return prefix_list
        raise StudyManifestParsingError(
            f"No prefixed defined in manifest in {study_path}"
        )

    def get_sql_file_list(self):
        if sql_file_list := self.study_config["sql_config"]["file_names"]:
            return sql_file_list
        raise StudyManifestParsingError(
            f"No sql_file_list defined in manifest in {study_path}"
        )

    def get_export_table_list(self):
        if export_table_list := self.study_config["export_config"]["export_list"]:
            return export_table_list
        raise StudyManifestParsingError(
            f"No export list defined in manifest in {study_path}"
        )

    def reset_export_dir(self):
        """
        Removes existing exports from a study's local data dir
        """
        project_path = Path(__file__).resolve().parents[1]
        path = Path(f"{str(project_path)}/data_export/{self.get_study_prefix()}/")
        if path.exists():
            for file in path.glob("*"):
                file.unlink()

    # Study sql clean call
    def clean_study(self, cursor: Cursor, schema_name: str, verbose: bool = False):
        prefix = self.get_study_prefix()
        view_sql = get_show_views(schema_name, prefix)
        table_sql = get_show_tables(schema_name, prefix)
        view_table_list = []
        for item in [[view_sql, "VIEW"], [table_sql, "TABLE"]]:
            cursor.execute(item[0])
            for row in cursor:
                # this check handles athena reporting views as also being tables,
                # so we don't waste time dropping things that don't exist
                if item[1] == "TABLE":
                    if not any(row[0] in item for item in view_table_list):
                        view_table_list.append([row[0], item[1]])
                else:
                    view_table_list.append([row[0], item[1]])
        if verbose:
            self._build_drop_queries(cursor, verbose, view_table_list, None, None)
        else:
            with Progress() as progress:
                task = progress.add_task(
                    f"Removing {self.get_study_prefix()} study artifacts...",
                    total=len(view_table_list),
                )
                self._build_drop_queries(
                    cursor, verbose, view_table_list, progress, task
                )

    def _build_drop_queries(
        self, cursor: Cursor, verbose: bool, view_table_list: List, progress, task
    ):
        """Constructs queries and posts to athena."""
        for view_table in view_table_list:
            drop_view_table = get_drop_view_table(
                name=view_table[0], view_or_table=view_table[1]
            )
            cursor.execute(drop_view_table)
            query_console_output(verbose, drop_view_table, progress, task)

    # study sql build_tables

    def build_study(self, cursor: Cursor, schema_name: str, verbose: bool = False):
        queries = []
        for file in self.get_sql_file_list():
            for query in parse_sql(load_text(f"{self.study_path}/{file}")):
                queries.append([query, file])
        if verbose:
            self._execute_make_queries(cursor, verbose, queries, None, None)
        else:
            with Progress() as progress:
                task = progress.add_task(
                    f"Creating {self.get_study_prefix()} study in db...",
                    total=len(queries),
                )
                self._execute_make_queries(
                    cursor,
                    verbose,
                    queries,
                    progress,
                    task,
                )

    def _execute_make_queries(
        self, cursor: Cursor, verbose: bool, queries: list, progress, task
    ):
        for query in queries:
            try:
                cursor.execute(query[0])
                query_console_output(verbose, query, progress, task)
            except Exception as e:
                print(query[1])
                print(e)

    # study exporting tables

    def export_study(self, cursor: Cursor):
        self.reset_export_dir()
        for table in track(
            self.get_export_table_list(),
            description=f"Exporting {self.get_study_prefix()} counts...",
        ):
            dataframe = cursor.execute(f"select * from {table}").as_pandas()
            project_path = Path(__file__).resolve().parents[1]
            path = Path(f"{str(project_path)}/data_export/{self.get_study_prefix()}/")
            path.mkdir(parents=True, exist_ok=True)
            dataframe.to_csv(f"{path}/{table}.csv", index=False)
            dataframe.to_parquet(f"{path}/{table}.parquet", index=False)
