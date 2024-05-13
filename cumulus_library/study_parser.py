"""Contains classes for loading study data based on manifest.toml files"""

import contextlib
import csv
import importlib.util
import inspect
import pathlib
import sys
import typing

import pandas
import pytablewriter
import toml
from rich.progress import Progress, TaskID, track

from cumulus_library import (
    __version__,
    base_table_builder,
    base_utils,
    databases,
    enums,
    errors,
    protected_table_builder,
)
from cumulus_library.statistics import psm
from cumulus_library.template_sql import base_templates


@contextlib.contextmanager
def _temporary_sys_path(add: pathlib.Path) -> None:
    orig_path = list(sys.path)
    try:
        sys.path.insert(0, str(add))
        yield
    finally:
        sys.path = orig_path


class StudyManifestParser:
    """Handles loading of study data from manifest files.

    The goal of this class is to make it so that a researcher can contribute a study
    definition without touching the main python infrastructure. It provides
    mechanisms for IDing studies/files of interest, and for executing queries, but
    specifically it should never be in charge of instantiation a cursor itself -
    this will help to future proof against other database implementations in the
    future.
    """

    def __init__(
        self,
        study_path: pathlib.Path | None = None,
        data_path: pathlib.Path | None = None,
    ):
        """Instantiates a StudyManifestParser.

        :param study_path: A pathlib Path object, optional
        """
        self._study_path = None
        self._study_config = {}
        if study_path is not None:
            self.load_study_manifest(study_path)
        self.data_path = data_path

    def __repr__(self):
        return str(self._study_config)

    ### toml parsing helper functions
    def load_study_manifest(self, study_path: pathlib.Path) -> None:
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
                    raise errors.StudyManifestParsingError(
                        f"Invalid prefix in manifest at {study_path}"
                    )
                self._study_config = config
            self._study_path = study_path
        except FileNotFoundError as e:
            raise errors.StudyManifestFilesystemError(
                f"Missing or invalid manifest found at {study_path}"
            ) from e
        except toml.TomlDecodeError as e:
            # just unify the error classes for convenience of catching them
            raise errors.StudyManifestParsingError(str(e)) from e

    def get_study_prefix(self) -> str | None:
        """Reads the name of a study prefix from the in-memory study config

        :returns: A string of the prefix in the manifest, or None if not found
        """
        return self._study_config.get("study_prefix")

    def get_sql_file_list(self, continue_from: str | None = None) -> list[str] | None:
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
                raise errors.StudyManifestParsingError(
                    f"No tables matching '{continue_from}' found"
                )
        return sql_files

    def get_table_builder_file_list(self) -> list[str] | None:
        """Reads the contents of the table_builder_config array from the manifest

        :returns: An array of sql files from the manifest, or None if not found.
        """
        sql_config = self._study_config.get("table_builder_config", {})
        return sql_config.get("file_names", [])

    def get_counts_builder_file_list(self) -> list[str] | None:
        """Reads the contents of the counts_builder_config array from the manifest

        :returns: An array of sql files from the manifest, or None if not found.
        """
        sql_config = self._study_config.get("counts_builder_config", {})
        return sql_config.get("file_names", [])

    def get_statistics_file_list(self) -> list[str] | None:
        """Reads the contents of the statistics_config array from the manifest

        :returns: An array of statistics toml files from the manifest,
          or None if not found.
        """
        stats_config = self._study_config.get("statistics_config", {})
        return stats_config.get("file_names", [])

    def get_export_table_list(self) -> list[str] | None:
        """Reads the contents of the export_list array from the manifest

        :returns: An array of tables to export from the manifest, or None if not found.
        """
        export_config = self._study_config.get("export_config", {})
        export_table_list = export_config.get("export_list", [])
        for table in export_table_list:
            if not table.startswith(f"{self.get_study_prefix()}__"):
                raise errors.StudyManifestParsingError(
                    f"{table} in export list does not start with prefix "
                    f"{self.get_study_prefix()}__ - check your manifest file."
                )
        return export_table_list

    def get_all_generators(self) -> list[str]:
        """Convenience method for getting files that generate sql queries"""
        return (
            self.get_table_builder_file_list()
            + self.get_counts_builder_file_list()
            + self.get_statistics_file_list()
        )

    def reset_counts_exports(self) -> None:
        """
        Removes exports associated with this study from the ../data_export directory.
        """
        path = pathlib.Path(f"{self.data_path}/{self.get_study_prefix()}")
        if path.exists():
            # we're just going to remove the count exports - stats exports in
            # subdirectories are left alone by this call
            for file in path.glob("*.*"):
                file.unlink()

    # SQL related functions

    def get_unprotected_stats_view_table(
        self,
        cursor: databases.DatabaseCursor,
        query: str,
        artifact_type: str,
        drop_prefix: str,
        display_prefix: str,
        stats_clean: bool,
    ):
        """Gets all items from the database by type, less any protected items

        :param cursor: An object of type databases.DatabaseCursor
        :param query: A query to get the raw list of items from the db
        :param artifact_type: either 'table' or 'view'
        :param drop_prefix: The prefix requested to drop
        :param display_prefix: The expected study prefix
        :param stats_clean: A boolean indicating if stats tables are being cleaned

        :returns: a list of study tables to drop
        """
        unprotected_list = []
        db_contents = cursor.execute(query).fetchall()
        if (
            f"{drop_prefix}{enums.ProtectedTables.STATISTICS.value}",
        ) in db_contents and not stats_clean:
            protected_list = cursor.execute(
                f"""SELECT {artifact_type.lower()}_name 
                FROM {drop_prefix}{enums.ProtectedTables.STATISTICS.value}
                WHERE study_name = '{display_prefix}'"""
            ).fetchall()
            for protected_tuple in protected_list:
                if protected_tuple in db_contents:
                    db_contents.remove(protected_tuple)
        for db_row_tuple in db_contents:
            # this check handles athena reporting views as also being tables,
            # so we don't waste time dropping things that don't exist
            if artifact_type == "TABLE":
                if not any(
                    db_row_tuple[0] in iter_q_and_t for iter_q_and_t in unprotected_list
                ):
                    unprotected_list.append([db_row_tuple[0], artifact_type])
            else:
                unprotected_list.append([db_row_tuple[0], artifact_type])
        return unprotected_list

    def clean_study(
        self,
        cursor: databases.DatabaseCursor,
        schema_name: str,
        stats_clean: bool = False,
        verbose: bool = False,
        prefix: str | None = None,
    ) -> list:
        """Removes tables beginning with the study prefix from the database schema

        :param cursor: A DatabaseCursor object
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

        view_sql = base_templates.get_show_views(schema_name, drop_prefix)
        table_sql = base_templates.get_show_tables(schema_name, drop_prefix)
        for query_and_type in [[view_sql, "VIEW"], [table_sql, "TABLE"]]:
            view_table_list = self.get_unprotected_stats_view_table(
                cursor,
                query_and_type[0],
                query_and_type[1],
                drop_prefix,
                display_prefix,
                stats_clean,
            )
        if not view_table_list:
            return view_table_list

        # We'll do a pass to see if any of these tables were created outside of a
        # study builder, and remove them from the list.
        for view_table in view_table_list.copy():
            if any(
                (
                    f"__{word.value}_" in view_table[0]
                    or view_table[0].endswith(f"__{word.value}")
                )
                for word in enums.ProtectedTableKeywords
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
        with base_utils.get_progress_bar(disable=verbose) as progress:
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
        # if we're doing a stats clean, we'll also remove the table containing the
        # list of protected tables
        if stats_clean:
            drop_query = base_templates.get_drop_view_table(
                f"{drop_prefix}{enums.ProtectedTables.STATISTICS.value}", "TABLE"
            )
            cursor.execute(drop_query)

        return view_table_list

    def _execute_drop_queries(
        self,
        cursor: databases.DatabaseCursor,
        verbose: bool,
        view_table_list: list,
        progress: Progress,
        task: TaskID,
    ) -> None:
        """Handler for executing drop view/table queries and displaying console output.

        :param cursor: A DatabaseCursor object
        :param verbose: toggle from progress bar to query output
        :param view_table_list: a list of views and tables beginning with
          the study prefix
        :param progress: a rich progress bar renderer
        :param task: a TaskID for a given progress bar
        """
        for view_table in view_table_list:
            drop_view_table = base_templates.get_drop_view_table(
                name=view_table[0], view_or_table=view_table[1]
            )
            with base_utils.query_console_output(
                verbose, drop_view_table, progress, task
            ):
                cursor.execute(drop_view_table)

    def _load_and_execute_builder(
        self,
        filename: str,
        cursor: databases.DatabaseCursor,
        schema: str,
        *,
        config: base_utils.StudyConfig,
        verbose: bool = False,
        drop_table: bool = False,
        parser: databases.DatabaseParser = None,
        write_reference_sql: bool = False,
        doc_str: str | None = None,
    ) -> None:
        """Loads a table builder from a file.

        Since we have to support arbitrary user-defined python files here, we
        jump through some importlib hoops to import the module directly from
        a source file defined in the manifest.

        As with eating an ortolan, you may wish to cover your head with a cloth.
        Per an article on the subject: "Tradition dictates that this is to shield
        - from God's eyes - the shame of such a decadent and disgraceful act."

        """
        spec = importlib.util.spec_from_file_location(
            "table_builder", f"{self._study_path}/{filename}"
        )
        table_builder_module = importlib.util.module_from_spec(spec)
        sys.modules["table_builder"] = table_builder_module
        # Inject the study dir into sys.path so that builders can import
        # from surrounding utility code, even if the study isn't installed.
        # (i.e. you're working from a git checkout and do something like `-s .`)
        with _temporary_sys_path(self._study_path.parent):
            spec.loader.exec_module(table_builder_module)

        # We're going to find all subclasses of BaseTableBuild in this file.
        # Since BaseTableBuilder itself is a valid subclass of BaseTableBuilder,
        # we'll detect and skip it. If we don't find any subclasses,
        # we'll punt.
        table_builder_subclasses = []
        for _, cls_obj in inspect.getmembers(table_builder_module, inspect.isclass):
            if (
                issubclass(cls_obj, base_table_builder.BaseTableBuilder)
                and cls_obj != base_table_builder.BaseTableBuilder
            ):
                table_builder_subclasses.append(cls_obj)

        if len(table_builder_subclasses) == 0:
            raise errors.StudyManifestParsingError(
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
        if write_reference_sql:
            table_builder.prepare_queries(cursor, schema, parser=parser, config=config)
            table_builder.comment_queries(doc_str=doc_str)
            new_filename = pathlib.Path(f"{filename}").stem + ".sql"
            table_builder.write_queries(
                path=pathlib.Path(f"{self._study_path}/reference_sql/" + new_filename)
            )
        else:
            table_builder.execute_queries(
                cursor,
                schema,
                verbose=verbose,
                drop_table=drop_table,
                parser=parser,
                config=config,
            )

        # After running the executor code, we'll remove
        # it so it doesn't interfere with the next python module to
        # execute, since the subclass would otherwise hang around.
        del sys.modules[table_builder_module.__name__]
        del table_builder_module

    def run_protected_table_builder(
        self,
        cursor: databases.DatabaseCursor,
        schema: str,
        *,
        config: base_utils.StudyConfig,
        verbose: bool = False,
    ) -> None:
        """Creates protected tables for persisting selected data across runs

        :param cursor: A DatabaseCursor object
        :param schema: The name of the schema to write tables to
        :param verbose: toggle from progress bar to query output
        """
        ptb = protected_table_builder.ProtectedTableBuilder()
        ptb.execute_queries(
            cursor,
            schema,
            verbose,
            study_name=self._study_config.get("study_prefix"),
            study_stats=self._study_config.get("statistics_config"),
            config=config,
        )

    def run_table_builder(
        self,
        cursor: databases.DatabaseCursor,
        schema: str,
        *,
        config: base_utils.StudyConfig,
        verbose: bool = False,
        parser: databases.DatabaseParser = None,
    ) -> None:
        """Loads modules from a manifest and executes code via BaseTableBuilder

        :param cursor: A DatabaseCursor object
        :param schema: The name of the schema to write tables to
        :param verbose: toggle from progress bar to query output
        """
        for file in self.get_table_builder_file_list():
            self._load_and_execute_builder(
                file, cursor, schema, verbose=verbose, parser=parser, config=config
            )

    def run_counts_builders(
        self,
        cursor: databases.DatabaseCursor,
        schema: str,
        *,
        config: base_utils.StudyConfig,
        verbose: bool = False,
    ) -> None:
        """Loads counts modules from a manifest and executes code via BaseTableBuilder

        While a count is a form of statistics, it is treated separately from other
        statistics because it is, by design, always going to be static against a
        given dataset, where other statistical methods may use sampling techniques
        or adjustable input parameters that may need to be preserved for later review.

        :param cursor: A DatabaseCursor object
        :param schema: The name of the schema to write tables to
        :param verbose: toggle from progress bar to query output
        """
        for file in self.get_counts_builder_file_list():
            self._load_and_execute_builder(
                file, cursor, schema, verbose=verbose, config=config
            )

    def run_statistics_builders(
        self,
        cursor: databases.DatabaseCursor,
        schema: str,
        *,
        config: base_utils.StudyConfig,
        verbose: bool = False,
    ) -> None:
        """Loads statistics modules from toml definitions and executes

        :param cursor: A DatabaseCursor object
        :param schema: The name of the schema to write tables to
        :keyword verbose: toggle from progress bar to query output
        :keyword stats_build: If true, will run statistical sampling & table generation
        """
        if not config.stats_build:
            return
        for file in self.get_statistics_file_list():
            # This open is a bit redundant with the open inside of the PSM builder,
            # but we're letting it slide so that builders function similarly
            # across the board
            safe_timestamp = base_utils.get_tablename_safe_iso_timestamp()
            toml_path = pathlib.Path(f"{self._study_path}/{file}")
            with open(toml_path, encoding="UTF-8") as file:
                config = toml.load(file)
                config_type = config["config_type"]
                target_table = config["target_table"]
            if config_type == "psm":
                builder = psm.PsmBuilder(
                    toml_path,
                    self.data_path / f"{self.get_study_prefix()}/psm",
                    config=config,
                )
            else:
                raise errors.StudyManifestParsingError(
                    f"{toml_path} references an invalid statistics type {config_type}."
                )
            builder.execute_queries(
                cursor, schema, verbose, table_suffix=safe_timestamp, config=config
            )

            insert_query = base_templates.get_insert_into_query(
                f"{self.get_study_prefix()}__{enums.ProtectedTables.STATISTICS.value}",
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
                        base_utils.get_utc_datetime(),
                    ]
                ],
            )
            cursor.execute(insert_query)

    def run_matching_table_builder(
        self,
        cursor: databases.DatabaseCursor,
        schema: str,
        builder: str,
        *,
        config: base_utils.StudyConfig,
        verbose: bool = False,
        parser: databases.DatabaseParser = None,
    ):
        """targets all table builders matching a target string for running"""
        all_generators = self.get_all_generators()
        for file in all_generators:
            if builder and file.find(builder) == -1:
                continue
            self._load_and_execute_builder(
                file,
                cursor,
                schema,
                verbose=verbose,
                drop_table=True,
                parser=parser,
                config=config,
            )

    def run_generate_sql(
        self,
        cursor: databases.DatabaseCursor,
        schema: str,
        *,
        config: base_utils.StudyConfig,
        builder: str | None = None,
        parser: databases.DatabaseParser = None,
        verbose: bool = False,
    ) -> None:
        """Generates reference SQL from builders listed in the manifest

        :param cursor: A DatabaseCursor object
        :param schema: The name of the schema to write tables to
        :param builder: a specific builder to target
        :param verbose: toggle from progress bar to query output
        :param parser: a DB parser
        """
        all_generators = self.get_all_generators()
        doc_str = (
            "-- This sql was autogenerated as a reference example using the library\n"
            "-- CLI. Its format is tied to the specific database it was run against,\n"
            "-- and it may not be correct for all databases. Use the CLI's build \n"
            "-- option to derive the best SQL for your dataset."
        )
        for file in all_generators:
            if builder and file.find(builder) == -1:
                continue
            self._load_and_execute_builder(
                file,
                cursor,
                schema,
                parser=parser,
                write_reference_sql=True,
                doc_str=doc_str,
                verbose=verbose,
                config=config,
            )

    def run_generate_markdown(
        self,
        cursor: databases.DatabaseCursor,
        schema: str,
        parser: databases.DatabaseParser = None,
        verbose: bool = False,
    ) -> None:
        """Generates reference SQL from builders listed in the manifest

        :param cursor: A DatabaseCursor object
        :param schema: The name of the schema to write tables to
        :param verbose: toggle from progress bar to query output
        """

        query = base_templates.get_show_tables(
            schema_name=schema, prefix=f"{self.get_study_prefix()}__"
        )

        tables = [x[0] for x in cursor.execute(query).fetchall()]
        query = base_templates.get_column_datatype_query(
            schema_name=schema, table_names=tables, include_table_names=True
        )
        study_df = pandas.DataFrame(
            cursor.execute(query).fetchall(), columns=["Column", "Type", "Table"]
        )
        with open(
            self._study_path / f"{self.get_study_prefix()}_generated.md", "w"
        ) as f:
            table_list = sorted(study_df["Table"].unique())
            count_tables = [t for t in table_list if "__count_" in t]
            base_tables = [t for t in table_list if "__count_" not in t]
            if len(count_tables) > 0:
                f.write(f"## {self.get_study_prefix()} count tables\n\n")
                for table in count_tables:
                    self._write_md_table(table, study_df, f)
            if len(base_tables) > 0:
                f.write(f"## {self.get_study_prefix()} base tables\n\n")
                for table in base_tables:
                    self._write_md_table(table, study_df, f)

    def _write_md_table(self, name: str, df: pandas.DataFrame, file: typing.IO):
        table_df = df[df["Table"] == name].drop("Table", axis=1)
        table_df = table_df.assign(Description="")
        writer = pytablewriter.MarkdownTableWriter(dataframe=table_df)
        writer.table_name = f"{name}\n"
        writer.set_indent_level(2)
        writer.stream = file
        writer.write_table()
        file.write("\n\n")

    def build_study(
        self,
        cursor: databases.DatabaseCursor,
        *,
        config: base_utils.StudyConfig,
        verbose: bool = False,
        continue_from: str | None = None,
    ) -> list:
        """Creates tables in the schema by iterating through the sql_config.file_names

        :param cursor: A DatabaseCursor object
        :param schema: The name of the schema to write tables to
        :param verbose: toggle from progress bar to query output, optional
        :returns: loaded queries (for unit testing only)
        """
        queries = []
        for file in self.get_sql_file_list(continue_from):
            for query in base_utils.parse_sql(
                base_utils.load_text(f"{self._study_path}/{file}")
            ):
                queries.append([query, file])
        if len(queries) == 0:
            return []
        # We want to only show a progress bar if we are :not: printing SQL lines
        with base_utils.get_progress_bar(disable=verbose) as progress:
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
                config,
            )
        return queries

    def _query_error(self, query_and_filename: list, exit_message: str) -> None:
        print(
            "An error occured executing the following query in ",
            f"{query_and_filename[1]}:",
            file=sys.stderr,
        )
        print("--------", file=sys.stderr)
        print(query_and_filename[0], file=sys.stderr)
        print("--------", file=sys.stderr)
        raise errors.StudyManifestQueryError(exit_message)

    def _execute_build_queries(
        self,
        cursor: databases.DatabaseCursor,
        verbose: bool,
        queries: list,
        progress: Progress,
        task: TaskID,
        config: base_utils.StudyConfig,
    ) -> None:
        """Handler for executing create table queries and displaying console output.

        :param cursor: A DatabaseCursor object
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
                for word in enums.ProtectedTableKeywords
            ):
                self._query_error(
                    query,
                    "This query contains a table name which contains a reserved word "
                    "immediately after the study prefix. Please rename this table so "
                    "that is does not begin with one of these special words "
                    "immediately after the double undescore.\n Reserved words: "
                    f"{(word.value for word in enums.ProtectedTableKeywords)}",
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
                with base_utils.query_console_output(verbose, query[0], progress, task):
                    cursor.execute(query[0])
            except Exception as e:  # pylint: disable=broad-exception-caught
                self._query_error(
                    query,
                    "You can debug issues with this query using `sqlfluff lint`, "
                    "or by executing the query directly against the database.\n"
                    f"Error: {e}",
                )

    # Database exporting functions

    def export_study(
        self,
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
        self.reset_counts_exports()
        if archive:
            table_query = base_templates.get_show_tables(
                schema_name, f"{self.get_study_prefix()}__"
            )
            result = db.cursor().execute(table_query).fetchall()
            table_list = [row[0] for row in result]
        else:
            table_list = self.get_export_table_list()

        queries = []
        path = pathlib.Path(f"{data_path}/{self.get_study_prefix()}/")
        for table in track(
            table_list,
            description=f"Exporting {self.get_study_prefix()} data...",
        ):
            query = f"SELECT * FROM {table}"
            dataframe = db.execute_as_pandas(query)
            path.mkdir(parents=True, exist_ok=True)
            dataframe = dataframe.sort_values(
                by=list(dataframe.columns), ascending=False, na_position="first"
            )
            dataframe.to_csv(
                f"{path}/{table}.csv", index=False, quoting=csv.QUOTE_MINIMAL
            )
            dataframe.to_parquet(f"{path}/{table}.parquet", index=False)
            queries.append(queries)
        if archive:
            base_utils.zip_dir(path, data_path, self.get_study_prefix())
        return queries
