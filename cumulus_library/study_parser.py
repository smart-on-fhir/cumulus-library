""" Contains classes for loading study data based on manifest.toml files """
import inspect
import importlib.util
import sys

from pathlib import Path, PosixPath
from typing import List, Optional

import toml

from rich.progress import Progress, TaskID, track

from cumulus_library.base_table_builder import BaseTableBuilder
from cumulus_library.errors import StudyManifestParsingError
from cumulus_library.helper import (
    query_console_output,
    load_text,
    parse_sql,
    get_progress_bar,
)
from cumulus_library.template_sql.templates import (
    get_show_tables,
    get_show_views,
    get_drop_view_table,
)

StrList = List[str]

RESERVED_TABLE_KEYWORDS = ["etl", "nlp", "lib"]


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
    _study_config = {}

    def __init__(self, study_path: Optional[Path] = None):
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
            with open(f"{study_path}/manifest.toml", encoding="UTF-8") as file:
                config = toml.load(file)
                if not config.get("study_prefix") or not isinstance(
                    config["study_prefix"], str
                ):
                    raise StudyManifestParsingError(
                        f"Invalid prefix in manifest at {study_path}"
                    )
                self._study_config = config
            self._study_path = study_path
        except FileNotFoundError:
            raise StudyManifestParsingError(  # pylint: disable=raise-missing-from
                f"Missing or invalid manifest found at {study_path}"
            )

    def get_study_prefix(self) -> Optional[str]:
        """Reads the name of a study prefix from the in-memory study config

        :returns: A string of the prefix in the manifest, or None if not found
        """
        return self._study_config.get("study_prefix")

    def get_sql_file_list(self) -> Optional[StrList]:
        """Reads the contents of the sql_config array from the manifest

        :returns: An array of sql files from the manifest, or None if not found.
        """
        sql_config = self._study_config.get("sql_config", {})
        return sql_config.get("file_names", [])

    def get_table_builder_file_list(self) -> Optional[StrList]:
        """Reads the contents of the python_config array from the manifest

        :returns: An array of sql files from the manifest, or None if not found.
        """
        sql_config = self._study_config.get("table_builder_config", {})
        return sql_config.get("file_names", [])

    def get_export_table_list(self) -> Optional[StrList]:
        """Reads the contents of the export_list array from the manifest

        :returns: An array of tables to export from the manifest, or None if not found.
        """
        export_config = self._study_config.get("export_config", {})
        export_table_list = export_config.get("export_list", [])
        for table in export_table_list:
            if not table.startswith(f"{self.get_study_prefix()}__"):
                raise StudyManifestParsingError(
                    f"{table} in export list does not start with prefix "
                    f"{self.get_study_prefix()}__ - check your manifest file."
                )
        return export_table_list

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
        self,
        cursor: object,
        schema_name: str,
        verbose: bool = False,
        prefix: str = None,
    ) -> List:
        """Removes tables beginning with the study prefix from the database schema

        :param cursor: A PEP-249 compatible cursor object
        :param schema_name: The name of the schema containing the study tables
        :verbose: toggle from progress bar to query output, optional
        :returns: list of dropped tables (for unit testing only)

        TODO: If we need to support additional databases, we may need to investigate
        additional ways to get a list of table prefixes
        """
        if not schema_name:
            raise ValueError("No database provided")
        if not prefix:
            prefix = self.get_study_prefix()
        view_sql = get_show_views(schema_name, prefix)
        table_sql = get_show_tables(schema_name, prefix)
        view_table_list = []
        for query_and_type in [[view_sql, "VIEW"], [table_sql, "TABLE"]]:
            cursor.execute(query_and_type[0])
            for db_row_tuple in cursor:
                # this check handles athena reporting views as also being tables,
                # so we don't waste time dropping things that don't exist
                if query_and_type[1] == "TABLE":
                    if not any(
                        db_row_tuple[0] in query_and_type
                        for query_and_type in view_table_list
                    ):
                        view_table_list.append([db_row_tuple[0], query_and_type[1]])
                else:
                    view_table_list.append([db_row_tuple[0], query_and_type[1]])
        if not view_table_list:
            return view_table_list

        # We'll do a pass to see if any of these tables were created outside of a
        # study builder, and remove them from the list.
        for view_table in view_table_list.copy():
            if any(
                view_table[0].startswith(f"{self.get_study_prefix()}__{word}_")
                for word in RESERVED_TABLE_KEYWORDS
            ):
                view_table_list.remove(view_table)

        # We want to only show a progress bar if we are :not: printing SQL lines
        with get_progress_bar(disable=verbose) as progress:
            task = progress.add_task(
                f"Removing {self.get_study_prefix()} study artifacts...",
                total=len(view_table_list),
                visible=not verbose,
            )
            self._execute_drop_queries(cursor, verbose, view_table_list, progress, task)
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

        :param cursor: A PEP-249 compatible cursor object
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

    def _load_and_execute_builder(
        self, filename, cursor, schema, verbose, drop_table=False
    ) -> None:
        """Loads a table builder from a file.

        Since we have to support arbitrary user-defined python files here, we
        jump through some importlib hoops to import the module directly from
        a source file defined in the manifest.

        As with eating an ortolan, you may wish to cover your head with a cloth.
        Per an article on the subject: "Tradition dictates that this is to shield
        – from God’s eyes – the shame of such a decadent and disgraceful act."

        """
        spec = importlib.util.spec_from_file_location(
            "table_builder", f"{self._study_path}/{filename}"
        )
        table_builder_module = importlib.util.module_from_spec(spec)
        sys.modules["table_builder"] = table_builder_module
        spec.loader.exec_module(table_builder_module)

        # We're going to find all subclasses of BaseTableBuild in this file.
        # Since BaseTableBuilder itself is a valid subclass of BaseTableBuilder,
        # we'll detect and skip it. If we don't find exactly one subclass,
        # we'll punt.
        table_builder_subclasses = []
        for _, cls_obj in inspect.getmembers(table_builder_module, inspect.isclass):
            if issubclass(cls_obj, BaseTableBuilder) and cls_obj != BaseTableBuilder:
                table_builder_subclasses.append(cls_obj)
        if len(table_builder_subclasses) != 1:
            raise StudyManifestParsingError(
                f"Error loading {self._study_path}{filename}\n"
                "Custom builders must extend the BaseTableBuilder "
                "class exactly once per module."
            )

        # We'll get the subclass, initialize it, run the executor code, and then
        # remove it so it doesn't interfere with the next python module to
        # execute, since the subclass would otherwise hang around.
        table_builder_class = table_builder_subclasses[0]
        table_builder = table_builder_class()
        table_builder.execute_queries(cursor, schema, verbose, drop_table)

        # After runnning the executor code, we'll remove
        # remove it so it doesn't interfere with the next python module to
        # execute, since the subclass would otherwise hang around.
        del sys.modules[table_builder_module.__name__]
        del table_builder_module

    def run_table_builder(
        self, cursor: object, schema: str, verbose: bool = False
    ) -> None:
        """Loads modules from a manifest and executes code via BaseTableBuilder

        :param cursor: A PEP-249 compatible cursor object
        :param schema: The name of the schema to write tables to
        :param verbose: toggle from progress bar to query output
        """
        for file in self.get_table_builder_file_list():
            self._load_and_execute_builder(file, cursor, schema, verbose)

    def run_single_table_builder(
        self, cursor: object, schema: str, name: str, verbose: bool = False
    ):
        """targets a single table builder to run"""
        if not name.endswith(".py"):
            name = f"{name}.py"
        self._load_and_execute_builder(name, cursor, schema, verbose, drop_table=True)

    def build_study(self, cursor: object, verbose: bool = False) -> List:
        """Creates tables in the schema by iterating through the sql_config.file_names

        :param cursor: A PEP-249 compatible cursor object
        :param schema: The name of the schema to write tables to
        :param verbose: toggle from progress bar to query output, optional
        :returns: loaded queries (for unit testing only)
        """
        queries = []
        for file in self.get_sql_file_list():
            for query in parse_sql(load_text(f"{self._study_path}/{file}")):
                queries.append([query, file])
        if len(queries) == 0:
            return []
        # We want to only show a progress bar if we are :not: printing SQL lines
        with get_progress_bar(disable=verbose) as progress:
            task = progress.add_task(
                f"Creating {self.get_study_prefix()} study in db...",
                total=len(queries),
                visible=not verbose,
            )
            self._execute_build_queries(
                cursor,
                verbose,
                queries,
                progress,
                task,
            )
        return queries

    def _query_error(self, query_and_filename: List, exit_message: str) -> None:
        print(
            f"An error occured executing the following query in {query_and_filename[1]}:",
            file=sys.stderr,
        )
        print("--------", file=sys.stderr)
        print(query_and_filename[0], file=sys.stderr)
        print("--------", file=sys.stderr)
        sys.exit(exit_message)

    def _execute_build_queries(
        self,
        cursor: object,
        verbose: bool,
        queries: list,
        progress: Progress,
        task: TaskID,
    ) -> None:
        """Handler for executing create table queries and displaying console output.

        :param cursor: A PEP-249 compatible cursor object
        :param verbose: toggle from progress bar to query output
        :param queries: a list of queries read from files in sql_config.file_names
        :param progress: a rich progress bar renderer
        :param task: a TaskID for a given progress bar
        """
        for query in queries:
            create_line = query[0].split("\n")[0]
            if f" {self.get_study_prefix()}__" not in create_line:
                self._query_error(
                    query,
                    "This query does not contain the study prefix. All tables should "
                    f"start with a string like `{self.get_study_prefix()}__`, and it "
                    "should be in the first line of the query.",
                )
            if any(
                f" {self.get_study_prefix()}__{word}_" in create_line
                for word in RESERVED_TABLE_KEYWORDS
            ):
                self._query_error(
                    query,
                    "This query contains a table name which contains a reserved word "
                    "immediately after the study prefix. Please rename this table so "
                    "that is does not begin with one of these special words "
                    "immediately after the double undescore.\n"
                    f"Reserved words: {str(RESERVED_TABLE_KEYWORDS)}",
                )
            if create_line.count("__") > 1:
                self._query_error(
                    query,
                    "This query contains a table name with more than one '__' in it. "
                    "Double underscores are reserved for special use cases. Please "
                    "rename this table so the only double undercore is after the "
                    f"study prefix, e.g. `{self.get_study_prefix()}__`",
                )
            if f"{self.get_study_prefix()}__" not in query[0].split("\n")[0]:
                self._query_error(
                    query,
                    "This query does not contain the study prefix. All tables should "
                    "start with a string like `study_prefix__`.",
                )
            try:
                cursor.execute(query[0])
                query_console_output(verbose, query[0], progress, task)
            except Exception as e:  # pylint: disable=broad-exception-caught
                self._query_error(
                    query,
                    "You can debug issues with this query using `sqlfluff lint`, "
                    "or by executing the query directly against the database.\n"
                    f"Error: {e}",
                )

    # Database exporting functions

    def export_study(self, cursor: object, data_path: PosixPath) -> List:
        """Exports csvs/parquet extracts of tables listed in export_list

        :param cursor: A PEP-249 compatible cursor object
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
            dataframe = cursor.execute(query).as_pandas()
            path = Path(f"{str(data_path)}/{self.get_study_prefix()}/")
            path.mkdir(parents=True, exist_ok=True)
            dataframe.to_csv(f"{path}/{table}.csv", index=False)
            dataframe.to_parquet(f"{path}/{table}.parquet", index=False)
            queries.append(query)
        return queries
