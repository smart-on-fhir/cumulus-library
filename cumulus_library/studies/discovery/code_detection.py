""" Module for generating encounter codeableConcept table"""

from cumulus_library.base_table_builder import BaseTableBuilder
from cumulus_library.helper import get_progress_bar, query_console_output
from cumulus_library.template_sql.templates import get_code_system_pairs
from cumulus_library.template_sql.utils import (
    is_codeable_concept_array_populated,
    is_codeable_concept_populated,
    is_code_populated,
)

from cumulus_library.studies.discovery.code_definitions import code_list


class CodeDetectionBuilder(BaseTableBuilder):
    display_text = "Selecting unique code systems..."

    def _check_codes_in_fields(self, code_sources: list[dict], schema, cursor) -> dict:
        """checks if Coding/CodeableConcept fields are present and populated"""

        with get_progress_bar() as progress:
            task = progress.add_task(
                "Discovering available coding systems...",
                total=len(code_sources),
            )
            for code_source in code_sources:
                if code_source["is_array"]:
                    code_source["has_data"] = is_codeable_concept_array_populated(
                        schema,
                        code_source["table_name"],
                        code_source["column_name"],
                        cursor,
                        allow_partial=False,
                    )
                elif code_source["is_bare_coding"]:
                    code_source["has_data"] = is_code_populated(
                        schema,
                        code_source["table_name"],
                        code_source["column_name"],
                        cursor,
                        allow_partial=False,
                    )
                else:
                    code_source["has_data"] = is_codeable_concept_populated(
                        schema,
                        code_source["table_name"],
                        code_source["column_name"],
                        cursor,
                        allow_partial=False,
                    )
                progress.advance(task)
        return code_sources

    def prepare_queries(self, cursor: object, schema: str):
        """Constructs queries related to condition codeableConcept

        :param cursor: A database cursor object
        :param schema: the schema/db name, matching the cursor

        """

        code_sources = []
        for code_definition in code_list:
            if any(
                x not in code_definition.keys() for x in ["table_name", "column_name"]
            ):
                raise KeyError(
                    "Expected table_name and column_name keys in "
                    f"{str(code_definition)}"
                )
            code_source = {
                "is_bare_coding": False,
                "is_array": False,
                "has_data": False,
            }
            for key in code_definition.keys():
                code_source[key] = code_definition[key]
            code_sources.append(code_source)

        code_sources = self._check_codes_in_fields(code_sources, schema, cursor)
        query = get_code_system_pairs("discovery__code_sources", code_sources)
        self.queries.append(query)
