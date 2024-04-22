""" Module for generating encounter codeableConcept table"""

from cumulus_library import base_table_builder, base_utils, databases
from cumulus_library.studies.discovery import code_definitions
from cumulus_library.studies.discovery.discovery_templates import discovery_templates
from cumulus_library.template_sql import sql_utils


class CodeDetectionBuilder(base_table_builder.BaseTableBuilder):
    display_text = "Selecting unique code systems..."

    def _check_coding_against_db(self, code_source, schema, cursor, parser):
        """selects the appropriate DB query to run"""

        return sql_utils.is_field_populated(
            schema=schema,
            source_table=code_source["table_name"],
            hierarchy=code_source["column_hierarchy"],
            expected=code_source.get("expected"),
            cursor=cursor,
            parser=parser,
        )

    def _check_codes_in_fields(
        self, code_sources: list[dict], schema, cursor, parser
    ) -> dict:
        """checks if Coding/CodeableConcept fields are present and populated"""

        with base_utils.get_progress_bar() as progress:
            task = progress.add_task(
                "Discovering available coding systems...",
                total=len(code_sources),
            )
            for code_source in code_sources:
                code_source["has_data"] = self._check_coding_against_db(
                    code_source, schema, cursor, parser
                )
                progress.advance(task)
        return code_sources

    def prepare_queries(
        self,
        cursor: databases.DatabaseCursor,
        schema: str,
        parser: databases.DatabaseParser = None,
        *args,
        **kwargs,
    ):
        """Constructs queries related to condition codeableConcept

        :param cursor: A database cursor object
        :param schema: the schema/db name, matching the cursor

        """

        code_sources = []
        required_keys = {"table_name", "column_hierarchy"}
        for code_definition in code_definitions.code_list:
            if not required_keys.issubset(code_definition):
                raise KeyError(
                    "Expected table_name and column_hierarchy keys in "
                    f"{code_definition!s}"
                )
            code_source = {
                "has_data": False,
            }
            for key in code_definition.keys():
                code_source[key] = code_definition[key]
            code_sources.append(code_source)
        code_sources = self._check_codes_in_fields(code_sources, schema, cursor, parser)
        query = discovery_templates.get_code_system_pairs(
            "discovery__code_sources", code_sources
        )
        self.queries.append(query)
