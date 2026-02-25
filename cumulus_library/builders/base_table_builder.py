"""abstract base for python-based study executors"""

import abc
import pathlib
import sys
import typing

from cumulus_library import base_utils, study_manifest


class BaseTableBuilder(abc.ABC):
    """Generic base class for python table builders.

    To use a table builder, extend this class exactly once in a new module.
    See ./studies/vocab or ./studies/core for example usage.
    """

    display_text = "Building custom tables..."

    def __init__(
        self,
        manifest: study_manifest.StudyManifest | None = None,
        *args,
        **kwargs,
    ):
        self.queries = []
        self.parallel_allowed = True

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
        build_stage: str | None = None,
        **kwargs,
    ):
        """Executes queries set up by a prepare_queries call

        :param config: A study configuration object
        :param manifest: A study manifest object
        :keyword build_stage: The name of the currently processing stage
        """
        if self.queries == []:
            self.prepare_queries(*args, config=config, manifest=manifest, **kwargs)
        cursor = config.db.cursor()
        viewtables = base_utils.get_viewtable_names_from_queries(config, self.queries)
        if config.drop_table:
            for name, view_or_table in viewtables:
                cursor.execute(f"DROP {view_or_table} IF EXISTS {name}")

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

        self.post_execution(config, manifest, *args, **kwargs)

    def post_execution(
        self,
        config: base_utils.StudyConfig,
        manifest: study_manifest.StudyManifest,
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
