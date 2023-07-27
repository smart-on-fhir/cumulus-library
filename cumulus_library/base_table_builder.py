""" abstract base for python-based study executors """
import re

from abc import ABC, abstractmethod
from pathlib import Path
from typing import final

from cumulus_library.helper import get_progress_bar, query_console_output


class BaseTableBuilder(ABC):
    """Generic base class for python table builders.

    To use a table builder, extend this class exactly once in a new module.
    See ./studies/vocab or ./studies/core for example usage.
    """

    display_text = "Building custom tables..."

    def __init__(self):
        self.queries = []

    @abstractmethod
    def prepare_queries(self, cursor: object, schema: str):
        """Main entrypoint for python table builders.

        When completed, prepare_queries should populate self.queries with sql
        statements to execute. This array will the be read by run queries.

        :param cursor: A PEP-249 compatible cursor
        :param schema: A schema name
        :param verbose: toggle for verbose output mode
        """
        raise NotImplementedError

    @final
    def execute_queries(
        self, cursor: object, schema: str, verbose: bool, drop_table: bool = False
    ):
        """Executes queries set up by a prepare_queries call

        :param cursor: A PEP-249 compatible cursor
        :param schema: A schema name
        :param verbose: toggle for verbose output mode
        :param drop_table: drops any tables found in prepared_queries results
        """
        self.prepare_queries(cursor, schema)
        if drop_table:
            table_names = []
            for query in self.queries:
                # Get the first non-whitespace word after create table
                table_name = re.search(
                    '(?i)(?<=create table )(([a-zA-Z0-9_".-]+))', query
                )  # [0]
                if table_name:
                    table_name = table_name[0]
                    # if it contains a schema, remove it (usually it won't, but some CTAS
                    # forms may)
                    if "." in table_name[0]:
                        table_name = table_name.split(".")[1].replace('"', "")
                    table_names.append(table_name)
            for table_name in table_names:
                cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        with get_progress_bar(disable=verbose) as progress:
            task = progress.add_task(
                self.display_text,
                total=len(self.queries),
                visible=not verbose,
            )
            for query in self.queries:
                cursor.execute(query)
                query_console_output(verbose, self.queries, progress, task)

    def write_queries(self, filename: str = "output.sql"):
        with open(filename, "w") as file:
            for query in self.queries:
                file.write(query)
                file.write("\n")
