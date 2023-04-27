""" Contains classes for loading study data based on manifest.toml files """
import sys

from pathlib import Path
from typing import List, Union

import toml

from rich.progress import Progress, TaskID, track

from cumulus_library.helper import query_console_output, load_text, parse_sql
from cumulus_library.template_sql.templates import (
    get_show_tables,
    get_show_views,
    get_drop_view_table,
)


class StudyManifestParsingError(Exception):
    """Basic error for StudyManifestParser"""

    pass


class StudyManifestParser:
    """Handles loading of study data from manifest files.

    The goal of this class is to make it so that a researcher can contribute a study
    definition without touching the main python infrastructure. It provides
    mechanisms for IDing studies/files of interest, and for executing queries, but
    specifically it should never be in charge of instantiation a cursor itself -
    this will help to future proof against other database implementations in the
    future, assuming those DBs have a PEP-249 cursor available (and this is why we
    are hinting generic objects for cursors).

    """

    _study_path = None
    _study_config = None

    def __init__(self, study_path: Union[Path, None] = None):
        """Instantiates a StudyManifestParser.

        :param study_path: A pathlib Path object, optional
        """
        if study_path is not None:
            self.load_study_manifest(study_path)

    def __repr__(self):
        return str(self._study_config)

    ### toml parsing helper functions
    def load_study_manifest(self, study_path: Path) -> None:
        """Reads in a config object from a directory containing a manifest.toml

        :param study_path: A pathlib.Path object pointing to a study directory
        :raises StudyManifestParsingError: the manifest.toml is malformed or missing.
        """
        try:
            with open(f"{study_path}/manifest.toml") as file:
                config = toml.load(file)
                if (
                    "study_prefix" not in config.keys()
                    or config["study_prefix"] == None
                    or type(config["study_prefix"]) != str
                ):
                    raise StudyManifestParsingError(
                        f"Invalid prefix in manifest at {study_path}"
                    )
                self._study_config = config
            self._study_path = study_path
        except FileNotFoundError:
            raise StudyManifestParsingError(
                f"Missing or invalid manifest found at {study_path}"
            )

    def get_study_prefix(self) -> [str, None]:
        """Reads the name of a study prefix from the in-memory study config

        :returns: A string of the prefix in the manifest, or None if not found
        """
        try:
            if prefix := self._study_config["study_prefix"]:
                return prefix
        except:
            return None

    def get_sql_file_list(self) -> [list, None]:
        """Reads the contents of the sql_config array from the manifest

        :returns: An array of sql files from the manifest, or None if not found.
        """
        try:
            if sql_file_list := self._study_config["sql_config"]["file_names"]:
                return sql_file_list
        except:
            return None

    def get_export_table_list(self) -> [list, None]:
        """Reads the contents of the export_list array from the manifest

        :returns: An array of tables to export from the manifest, or None if not found.
        """
        try:
            if export_table_list := self._study_config["export_config"]["export_list"]:
                for table in export_table_list:
                    if f"{self.get_study_prefix()}__" not in table:
                        raise StudyManifestParsingError(
                            f"{table} in export list does not start with prefix "
                            f"{self.get_study_prefix()}__ - check your manifest file."
                        )
                return export_table_list
        except KeyError:
            return None

    def reset_export_dir(self) -> None:
        """
        Removes exports associated with this study from the ../data_export directory.
        """
        project_path = Path(__file__).resolve().parents[1]
        path = Path(f"{str(project_path)}/data_export/{self.get_study_prefix()}/")
        if path.exists():
            for file in path.glob("*"):
                file.unlink()

    # SQL related functions
    def clean_study(
        self, cursor: object, schema_name: str, verbose: bool = False
    ) -> List:
        """Removes tables beginning with the study prefix from the database schema

        :param cursor: A PEP-249 compatible curosr object
        :param schema_name: The name of the schema containing the study tables
        :verbose: toggle from progress bar to query output, optional
        :returns: list of dropped tables (for unit testing only)

        TODO: If we need to support additional databases, we may need to investigate
        additional ways to get a list of table prefixes
        """
        if schema_name is None:
            raise ValueError("No schema name provided")
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
            self._execute_drop_queries(cursor, verbose, view_table_list, None, None)
        else:
            with Progress() as progress:
                task = progress.add_task(
                    f"Removing {self.get_study_prefix()} study artifacts...",
                    total=len(view_table_list),
                )
                self._execute_drop_queries(
                    cursor, verbose, view_table_list, progress, task
                )
        return view_table_list

    def _execute_drop_queries(
        self,
        cursor: object,
        verbose: bool,
        view_table_list: List,
        progress: Progress,
        task: TaskID,
    ) -> None:
        """Handler for executing drop view/table queries and displaying console output.

        :param cursor: A PEP-249 compatible curosr object
        :param verbose: toggle from progress bar to query output
        :param view_table_list: a list of views and tables beginning with the study prefix
        :param progress: a rich progress bar renderer
        :param task: a TaskID for a given progress bar
        """
        for view_table in view_table_list:
            drop_view_table = get_drop_view_table(
                name=view_table[0], view_or_table=view_table[1]
            )
            cursor.execute(drop_view_table)
            query_console_output(verbose, drop_view_table, progress, task)

    def build_study(self, cursor: object, verbose: bool = False) -> List:
        """Creates tables in the schema by iterating through the sql_config.file_names

        :param cursor: A PEP-249 compatible curosr object
        :verbose: toggle from progress bar to query output, optional
        :returns: loaded queries (for unit testing only)
        """
        queries = []
        for file in self.get_sql_file_list():
            for query in parse_sql(load_text(f"{self._study_path}/{file}")):
                queries.append([query, file])
        if verbose:
            self._execute_build_queries(cursor, verbose, queries, None, None)
        else:
            with Progress() as progress:
                task = progress.add_task(
                    f"Creating {self.get_study_prefix()} study in db...",
                    total=len(queries),
                )
                self._execute_build_queries(
                    cursor,
                    verbose,
                    queries,
                    progress,
                    task,
                )
        return queries

    def _execute_build_queries(
        self,
        cursor: object,
        verbose: bool,
        queries: list,
        progress: Progress,
        task: TaskID,
    ) -> None:
        """Handler for executing create table queries and displaying console output.

        :param cursor: A PEP-249 compatible curosr object
        :param verbose: toggle from progress bar to query output
        :param queries: a list of queries read from files in sql_config.file_names
        :param progress: a rich progress bar renderer
        :param task: a TaskID for a given progress bar
        """
        for query in queries:
            if f"{self.get_study_prefix()}__" not in query[0]:
                print(f"An error occured executing the following query in {query[1]}:")
                print("--------")
                print(query[0])
                print("--------")
                print(
                    "This query does not contain the study prefix. All tables should ",
                    "start with a string like `study_prefix__`.",
                )
                sys.exit(1)
            try:
                cursor.execute(query[0])
                query_console_output(verbose, query[0], progress, task)
            except StudyManifestParsingError as e:
                print(f"An error occured executing the following query in {query[1]}:")
                print("--------")
                print(query[0])
                print("--------")
                print(
                    "You can debug issues with this query using `sqlfluff lint`, "
                    "or by executing the query directly against the database."
                )
                sys.exit(1)

    # Database exporting functions

    def export_study(self, cursor: object) -> List:
        """Exports csvs/parquet extracts of tables listed in export_list

        :param cursor: A PEP-249 compatible curosr object
        :returns: list of executed queries (for unit testing only)

        TODO: If we need to support additional databases, we may need to investigate
        additional ways to convert the query to a pandas object
        """
        self.reset_export_dir()
        queries = []
        for table in track(
            self.get_export_table_list(),
            description=f"Exporting {self.get_study_prefix()} counts...",
        ):
            query = f"select * from {table}"
            dataframe = cursor.execute(f"select * from {table}").as_pandas()
            project_path = Path(__file__).resolve().parents[1]
            path = Path(f"{str(project_path)}/data_export/{self.get_study_prefix()}/")
            path.mkdir(parents=True, exist_ok=True)
            dataframe.to_csv(f"{path}/{table}.csv", index=False)
            dataframe.to_parquet(f"{path}/{table}.parquet", index=False)
            queries.append(query)
        return queries
