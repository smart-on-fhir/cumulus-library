""" abstract base for python-based study executors """
from abc import ABC, abstractmethod
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
    def execute_queries(self, cursor: object, schema: str, verbose: bool):
        """Executes queries set up by a prepare_queries call

        :param cursor: A PEP-249 compatible cursor
        :param schema: A schema name
        :param verbose: toggle for verbose output mode
        """
        self.prepare_queries(cursor, schema)
        with get_progress_bar(disable=verbose) as progress:
            task = progress.add_task(
                self.display_text,
                total=len(self.queries),
                visible=not verbose,
            )
            for query in self.queries:
                cursor.execute(query)
                query_console_output(verbose, self.queries, progress, task)
