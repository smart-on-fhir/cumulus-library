"""abstract base for python-based study executors"""

import pathlib
import re
import sys
from abc import ABC, abstractmethod
from typing import final

from cumulus_library import base_utils, study_parser
from cumulus_library.databases import DatabaseCursor


class BaseTableBuilder(ABC):
    """Generic base class for python table builders.

    To use a table builder, extend this class exactly once in a new module.
    See ./studies/vocab or ./studies/core for example usage.
    """

    display_text = "Building custom tables..."

    def __init__(self):
        self.queries = []

    @abstractmethod
    def prepare_queries(self, cursor: object, schema: str, *args, **kwargs):
        """Main entrypoint for python table builders.

        When completed, prepare_queries should populate self.queries with sql
        statements to execute. This array will the be read by run queries.

        :param cursor: A PEP-249 compatible cursor
        :param schema: A schema name
        """
        raise NotImplementedError

    @final
    def execute_queries(
        self,
        cursor: DatabaseCursor,
        schema: str,
        verbose: bool,
        *args,
        drop_table: bool = False,
        manifest: study_parser.StudyManifestParser = None,
        **kwargs,
    ):
        """Executes queries set up by a prepare_queries call

        :param cursor: A PEP-249 compatible cursor
        :param schema: A schema name
        :param verbose: toggle for verbose output mode
        :param drop_table: drops any tables found in prepared_queries results
        """
        self.prepare_queries(cursor, schema, *args, manifest=manifest, **kwargs)
        if drop_table:
            table_names = []
            for query in self.queries:
                # Get the first non-whitespace word after create table
                table_name = re.search(
                    '(?i)(?<=create table )(([a-zA-Z0-9_".-]+))', query
                )

                if table_name:
                    if table_name[0] == "IF":
                        # Edge case - if we're doing an empty conditional CTAS creation,
                        # we need to run a slightly different regex
                        table_name = re.search(
                            '(?i)(?<=not exists )(([a-zA-Z0-9_".-]+))', query
                        )

                    table_name = table_name[0]
                    # TODO: this may not be required? reinvestigate
                    # if it contains a schema, remove it (usually it won't, but some
                    # CTAS forms may)
                    if "." in table_name:
                        table_name = table_name.split(".")[1].replace('"', "")
                    table_names.append(table_name)
            for table_name in table_names:
                cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        with base_utils.get_progress_bar(disable=verbose) as progress:
            task = progress.add_task(
                self.display_text,
                total=len(self.queries),
                visible=not verbose,
            )
            for query in self.queries:
                try:
                    query = base_utils.update_query_if_schema_specified(query, manifest)
                    with base_utils.query_console_output(
                        verbose, query, progress, task
                    ):
                        cursor.execute(query)
                except Exception as e:  # pylint: disable=broad-exception-caught
                    sys.exit(e)

        self.post_execution(cursor, schema, verbose, drop_table, *args, **kwargs)

    def post_execution(  # noqa: B027 - this looks like, but is not, an abstract method
        self,
        cursor: DatabaseCursor,
        schema: str,
        verbose: bool,
        *args,
        drop_table: bool = False,
        **kwargs,
    ):
        """Hook for any additional actions to run after execute_queries"""
        pass

    def comment_queries(self, doc_str=None):
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
            path = pathlib.Path.cwd() / "output.sql"

        path.parents[0].mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as file:
            for query in self.queries:
                file.write(query)
                file.write("\n")
