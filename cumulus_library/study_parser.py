""" Contains classes for loading study data based on manifest.toml files """
import inspect
import importlib.util
import sys

from datetime import datetime
from pathlib import Path, PosixPath
from typing import List, Optional

import toml

from rich.progress import Progress, TaskID, track

from cumulus_library import __version__
from cumulus_library.base_table_builder import BaseTableBuilder
from cumulus_library.databases import DatabaseBackend, DatabaseCursor
from cumulus_library.enums import PROTECTED_TABLE_KEYWORDS, PROTECTED_TABLES
from cumulus_library.errors import StudyManifestParsingError
from cumulus_library.helper import (
    query_console_output,
    load_text,
    parse_sql,
    get_progress_bar,
)
from cumulus_library.protected_table_builder import ProtectedTableBuilder
from cumulus_library.statistics.psm import PsmBuilder
from cumulus_library.template_sql.templates import (
    get_show_tables,
    get_show_views,
    get_drop_view_table,
    get_insert_into_query,
)

StrList = List[str]


class StudyManifestParser:
    """Handles loading of study data from manifest files.

    The goal of this class is to make it so that a researcher can contribute a study
    definition without touching the main python infrastructure. It provides
    mechanisms for IDing studies/files of interest, and for executing queries, but
    specifically it should never be in charge of instantiation a cursor itself -
    this will help to future proof against other database implementations in the
    future.
    """

    _study_path = None
    _study_config = {}

    def __init__(
        self, study_path: Optional[Path] = None, data_path: Optional[Path] = None
    ):
        """Instantiates a StudyManifestParser.

        :param study_path: A pathlib Path object, optional
        """
        if study_path is not None:
            self.load_study_manifest(study_path)
        self.data_path = data_path

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

    def get_sql_file_list(self, continue_from: str = None) -> Optional[StrList]:
        """Reads the contents of the sql_config array from the manifest

        :returns: An array of sql files from the manifest, or None if not found.
        """
        sql_config = self._study_config.get("sql_config", {})
        sql_files = sql_config.get("file_names", [])
        if continue_from:
            for pos, file in enumerate(sql_files):
                if continue_from.replace(".sql", "") == file.replace(".sql", ""):
                    sql_files = sql_files[pos:]
                    break
            else:
                sys.exit(f"No tables matching '{continue_from}' found")
        return sql_files

    def get_table_builder_file_list(self) -> Optional[StrList]:
        """Reads the contents of the table_builder_config array from the manifest

        :returns: An array of sql files from the manifest, or None if not found.
        """
        sql_config = self._study_config.get("table_builder_config", {})
        return sql_config.get("file_names", [])

    def get_counts_builder_file_list(self) -> Optional[StrList]:
        """Reads the contents of the counts_builder_config array from the manifest

        :returns: An array of sql files from the manifest, or None if not found.
        """
        sql_config = self._study_config.get("counts_builder_config", {})
        return sql_config.get("file_names", [])

    def get_statistics_file_list(self) -> Optional[StrList]:
        """Reads the contents of the statistics_config array from the manifest

        :returns: An array of statistics toml files from the manifest, or None if not found.
        """
        stats_config = self._study_config.get("statistics_config", {})
        return stats_config.get("file_names", [])

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

    def reset_data_dir(self) -> None:
        """
        Removes exports associated with this study from the ../data_export directory.
        """
        print(self.data_path)
        print(type(self.data_path))
        path = Path(f"{self.data_path}/{self.get_study_prefix()}")
        if path.exists():
            # we're just going to remove the count files - exports related to stats
            # that aren't uploaded to the aggregator are left alone.
            for file in path.glob("*.*"):
                file.unlink()

    # SQL related functions
    def clean_study(
        self,
        cursor: DatabaseCursor,
        schema_name: str,
        stats_clean: bool = False,
        verbose: bool = False,
        prefix: str = None,
    ) -> List:
        """Removes tables beginning with the study prefix from the database schema

        :param cursor: A PEP-249 compatible cursor object
        :param schema_name: The name of the schema containing the study tables
        :verbose: toggle from progress bar to query output, optional
        :returns: list of dropped tables (for unit testing only)
        :prefix: override prefix discovery with the provided prefix
        """
        if not schema_name:
            raise ValueError("No database provided")
        if not prefix:
            drop_prefix = f"{self.get_study_prefix()}__"
            display_prefix = self.get_study_prefix()
        else:
            drop_prefix = prefix
            display_prefix = drop_prefix

        if stats_clean:
            confirm = input(
                "This will remove all historical stats tables beginning in the "
                f"{display_prefix} study - are you sure? (y/N)"
            )
            if confirm is None or confirm.lower() not in ("y", "yes"):
                sys.exit("Table cleaning aborted")

        view_sql = get_show_views(schema_name, drop_prefix)
        table_sql = get_show_tables(schema_name, drop_prefix)
        view_table_list = []
        for query_and_type in [[view_sql, "VIEW"], [table_sql, "TABLE"]]:
            tuple_list = cursor.execute(query_and_type[0]).fetchall()
            if (
                f"{drop_prefix}{PROTECTED_TABLES.STATISTICS.value}",
            ) in tuple_list and not stats_clean:
                protected_list = cursor.execute(
                    f"""SELECT {(query_and_type[1]).lower()}_name 
                    FROM {drop_prefix}{PROTECTED_TABLES.STATISTICS.value}
                    WHERE study_name = '{display_prefix}'"""
                ).fetchall()
                print(protected_list)
                for protected_tuple in protected_list:
                    if protected_tuple in tuple_list:
                        tuple_list.remove(protected_tuple)
            for db_row_tuple in tuple_list:
                # this check handles athena reporting views as also being tables,
                # so we don't waste time dropping things that don't exist
                if query_and_type[1] == "TABLE":
                    if not any(
                        db_row_tuple[0] in iter_q_and_t
                        for iter_q_and_t in view_table_list
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
                (
                    (f"_{word.value}_") in view_table[0]
                    or view_table[0].endswith(word.value)
                )
                for word in PROTECTED_TABLE_KEYWORDS
            ):
                view_table_list.remove(view_table)

        if prefix:
            print("The following views/tables were selected by prefix:")
            for view_table in view_table_list:
                print(f"  {view_table[0]}")
            confirm = input("Remove these tables? (y/N)")
            if confirm.lower() not in ("y", "yes"):
                sys.exit("Table cleaning aborted")
        # We want to only show a progress bar if we are :not: printing SQL lines
        with get_progress_bar(disable=verbose) as progress:
            task = progress.add_task(
                f"Removing {display_prefix} study artifacts...",
                total=len(view_table_list),
                visible=not verbose,
            )
            self._execute_drop_queries(
                cursor,
                verbose,
                view_table_list,
                progress,
                task,
            )
        if stats_clean:
            drop_query = get_drop_view_table(
                f"{drop_prefix}{PROTECTED_TABLES.STATISTICS.value}", "TABLE"
            )
            cursor.execute(drop_query)

        return view_table_list

    def _execute_drop_queries(
        self,
        cursor: DatabaseCursor,
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
        # we'll detect and skip it. If we don't find any subclasses,
        # we'll punt.
        table_builder_subclasses = []
        for _, cls_obj in inspect.getmembers(table_builder_module, inspect.isclass):
            if issubclass(cls_obj, BaseTableBuilder) and cls_obj != BaseTableBuilder:
                table_builder_subclasses.append(cls_obj)

        if len(table_builder_subclasses) == 0:
            raise StudyManifestParsingError(
                f"Error loading {self._study_path}{filename}\n"
                "Custom builders must extend the BaseTableBuilder class."
            )

        # Remove instances of intermediate classes, if present
        table_builder_subclasses = list(
            filter(lambda x: x.__name__ != "CountsBuilder", table_builder_subclasses)
        )

        # We'll get the subclass, initialize it, run the executor code, and then
        # remove it so it doesn't interfere with the next python module to
        # execute, since the subclass would otherwise hang around.
        table_builder_class = table_builder_subclasses[0]
        table_builder = table_builder_class()
        table_builder.execute_queries(cursor, schema, verbose, drop_table)

        # After running the executor code, we'll remove
        # it so it doesn't interfere with the next python module to
        # execute, since the subclass would otherwise hang around.
        del sys.modules[table_builder_module.__name__]
        del table_builder_module

    def run_protected_table_builder(
        self, cursor: DatabaseCursor, schema: str, verbose: bool = False
    ) -> None:
        """Creates protected tables for persisting selected data across runs

        :param cursor: A PEP-249 compatible cursor object
        :param schema: The name of the schema to write tables to
        :param verbose: toggle from progress bar to query output
        """
        ptb = ProtectedTableBuilder()
        ptb.execute_queries(
            cursor,
            schema,
            verbose,
            study_name=self._study_config.get("study_prefix"),
            study_stats=self._study_config.get("statistics_config"),
        )

    def run_table_builder(
        self, cursor: DatabaseCursor, schema: str, verbose: bool = False
    ) -> None:
        """Loads modules from a manifest and executes code via BaseTableBuilder

        :param cursor: A PEP-249 compatible cursor object
        :param schema: The name of the schema to write tables to
        :param verbose: toggle from progress bar to query output
        """
        for file in self.get_table_builder_file_list():
            self._load_and_execute_builder(file, cursor, schema, verbose)

    def run_counts_builders(
        self, cursor: DatabaseCursor, schema: str, verbose: bool = False
    ) -> None:
        """Loads counts modules from a manifest and executes code via BaseTableBuilder

        While a count is a form of statistics, it is treated separately from other
        statistics because it is, by design, always going to be static against a
        given dataset, where other statistical methods may use sampling techniques
        or adjustable input parameters that may need to be preserved for later review.

        :param cursor: A PEP-249 compatible cursor object
        :param schema: The name of the schema to write tables to
        :param verbose: toggle from progress bar to query output
        """
        for file in self.get_counts_builder_file_list():
            self._load_and_execute_builder(file, cursor, schema, verbose)

    def run_statistics_builders(
        self,
        cursor: DatabaseCursor,
        schema: str,
        verbose: bool = False,
        stats_build: bool = False,
    ) -> None:
        """Loads statistics modules from toml definitions and executes

        :param cursor: A PEP-249 compatible cursor object
        :param schema: The name of the schema to write tables to
        :keyword verbose: toggle from progress bar to query output
        :keyword stats_build: If true, will run statistical sampling & table generation
        :keyword data_path: A path to where stats output artifacts should be stored
        """
        if not stats_build:
            return
        for file in self.get_statistics_file_list():
            # This open is a bit redundant with the open inside of the PSM builder,
            # but we're letting it slide so that builders function similarly
            # across the board
            iso_timestamp = datetime.now().replace(microsecond=0).isoformat()
            safe_timestamp = iso_timestamp.replace(":", "_").replace("-", "_")
            toml_path = Path(f"{self._study_path}/{file}")
            with open(toml_path, encoding="UTF-8") as file:
                config = toml.load(file)
                config_type = config["config_type"]
                target_table = config["target_table"]
            if config_type == "psm":
                builder = PsmBuilder(
                    toml_path, self.data_path / f"{self.get_study_prefix()}/psm"
                )
            else:
                raise StudyManifestParsingError(
                    f"{toml_path} references an invalid statistics type {config_type}."
                )
            builder.execute_queries(
                cursor, schema, verbose, table_suffix=safe_timestamp
            )

            insert_query = get_insert_into_query(
                f"{self.get_study_prefix()}__{PROTECTED_TABLES.STATISTICS.value}",
                [
                    "study_name",
                    "library_version",
                    "table_type",
                    "table_name",
                    "view_name",
                    "created_on",
                ],
                [
                    [
                        self.get_study_prefix(),
                        __version__,
                        config_type,
                        f"{target_table}_{safe_timestamp}",
                        target_table,
                        iso_timestamp,
                    ]
                ],
            )
            cursor.execute(insert_query)

    def run_single_table_builder(
        self, cursor: DatabaseCursor, schema: str, name: str, verbose: bool = False
    ):
        """targets a single table builder to run"""
        if not name.endswith(".py"):
            name = f"{name}.py"
        self._load_and_execute_builder(name, cursor, schema, verbose, drop_table=True)

    def build_study(
        self, cursor: DatabaseCursor, verbose: bool = False, continue_from: str = None
    ) -> List:
        """Creates tables in the schema by iterating through the sql_config.file_names

        :param cursor: A PEP-249 compatible cursor object
        :param schema: The name of the schema to write tables to
        :param verbose: toggle from progress bar to query output, optional
        :returns: loaded queries (for unit testing only)
        """
        queries = []
        for file in self.get_sql_file_list(continue_from):
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
        cursor: DatabaseCursor,
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
                f" {self.get_study_prefix()}__{word.value}_" in create_line
                for word in PROTECTED_TABLE_KEYWORDS
            ):
                self._query_error(
                    query,
                    "This query contains a table name which contains a reserved word "
                    "immediately after the study prefix. Please rename this table so "
                    "that is does not begin with one of these special words "
                    "immediately after the double undescore.\n"
                    f"Reserved words: {str(word.value for word in PROTECTED_TABLE_KEYWORDS)}",
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

    def export_study(self, db: DatabaseBackend, data_path: PosixPath) -> List:
        """Exports csvs/parquet extracts of tables listed in export_list

        :param db: A database backend
        :returns: list of executed queries (for unit testing only)
        """
        self.reset_data_dir()
        queries = []
        for table in track(
            self.get_export_table_list(),
            description=f"Exporting {self.get_study_prefix()} counts...",
        ):
            query = f"select * from {table}"
            dataframe = db.execute_as_pandas(query)
            path = Path(f"{str(data_path)}/{self.get_study_prefix()}/")
            path.mkdir(parents=True, exist_ok=True)
            dataframe = dataframe.sort_values(
                by=list(dataframe.columns), ascending=False, na_position="first"
            )
            dataframe.to_csv(f"{path}/{table}.csv", index=False)
            dataframe.to_parquet(f"{path}/{table}.parquet", index=False)
            queries.append(query)
        return queries
