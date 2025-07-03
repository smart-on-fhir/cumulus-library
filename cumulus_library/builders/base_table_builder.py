"""abstract base for python-based study executors"""

import abc
import pathlib
import re
import sys
import typing

from cumulus_library import base_utils, study_manifest


class BaseTableBuilder(abc.ABC):
    """Generic base class for python table builders.

    To use a table builder, extend this class exactly once in a new module.
    See ./studies/vocab or ./studies/core for example usage.
    """

    display_text = "Building custom tables..."

    def __init__(self, manifest: study_manifest.StudyManifest | None = None):
        self.queries = []

    @abc.abstractmethod
    def prepare_queries(
        self,
        config: base_utils.StudyConfig,
        manifest: study_manifest.StudyManifest,
        *args,
        **kwargs,
    ):
        """Main entrypoint for python table builders.

        When completed, prepare_queries should populate self.queries with sql
        statements to execute. This array will the be read by execute_queries.

        :param config: A study configuration object
        :param manifest: A study manifest object
        """
        raise NotImplementedError  # pragma: no cover

    @typing.final
    def execute_queries(
        self,
        config: base_utils.StudyConfig,
        manifest: study_manifest.StudyManifest,
        *args,
        **kwargs,
    ):
        """Executes queries set up by a prepare_queries call

        :param config: A study configuration object
        :param manifest: A study manifest object
        """
        self.prepare_queries(*args, config=config, manifest=manifest, **kwargs)
        cursor = config.db.cursor()
        if config.drop_table:
            table_names = []
            for query in self.queries:
                # Get the first non-whitespace word after create table
                table_name = re.search('(?i)(?<=create table )(([a-zA-Z0-9_".-]+))', query)

                if table_name:
                    if table_name[0] == "IF":
                        # Edge case - if we're doing an empty conditional CTAS creation,
                        # we need to run a slightly different regex
                        table_name = re.search('(?i)(?<=not exists )(([a-zA-Z0-9_".-]+))', query)

                    table_name = table_name[0]
                    table_names.append(table_name)
            for table_name in table_names:
                cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        with base_utils.get_progress_bar(disable=config.verbose) as progress:
            task = progress.add_task(
                self.display_text,
                total=len(self.queries),
                visible=not config.verbose,
            )
            for query in self.queries:
                try:
                    query = base_utils.update_query_if_schema_specified(query, manifest)
                    with base_utils.query_console_output(config.verbose, query, progress, task):
                        cursor.execute(query)
                except Exception as e:  # pylint: disable=broad-exception-caught
                    sys.exit(f"An error occurred executing this query:\n----\n{query}\n----\n{e}")

        self.post_execution(config, *args, **kwargs)

    def post_execution(
        self,
        config: base_utils.StudyConfig,
        *args,
        **kwargs,
    ):
        """Hook for any additional actions to run after execute_queries"""
        pass

    def comment_queries(self, doc_str: str | None = None):
        """Convenience method for annotating outputs of template generators to disk"""
        commented_queries = ["-- noqa: disable=all"]
        if doc_str:
            commented_queries.append(doc_str)
            commented_queries.append(
                "\n-- ###########################################################\n"
            )
        for query in self.queries:
            commented_queries.append(query)
            commented_queries.append(
                "\n-- ###########################################################\n"
            )
        commented_queries.pop()
        self.queries = commented_queries

    def write_queries(self, path: pathlib.Path | None = None):
        """writes all queries constructed by prepare_queries to disk"""
        if path is None:
            path = pathlib.Path.cwd() / "output.sql"  # pragma: no cover

        path.parents[0].mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as file:
            for query in self.queries:
                file.write(query)
                file.write("\n")
