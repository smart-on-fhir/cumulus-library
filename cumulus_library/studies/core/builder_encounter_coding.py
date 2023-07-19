""" Module for generating condition codeableConcept table"""

from typing import Dict, List

from cumulus_library.base_table_builder import BaseTableBuilder
from cumulus_library.helper import get_progress_bar, query_console_output
from cumulus_library.template_sql.templates import (
    CodeableConceptConfig,
    get_codeable_concept_denormalize_query,
    get_is_table_not_empty_query,
)


class EncounterCodingBuilder(BaseTableBuilder):
    display_text = "Creating encounter type code table..."

    def _check_data_in_fields(self, code_sources: List[Dict], cursor, progress, task):
        """checks if CodableConcept fields actually have data available"""

        # TODO: consider moving to a utility library if we have another case like
        # this one
        cols_with_data = []
        for code_source in code_sources:
            query = get_is_table_not_empty_query("encounter", code_source["name"])
            cursor.execute(query)
            progress.advance(task)

            if len(cursor.fetchall()) > 0:
                if code_source["is_array"]:
                    query = get_is_table_not_empty_query(
                        "encounter",
                        "t1.row1",
                        [
                            {
                                "source_col": code_source["name"],
                                "table_alias": "t1",
                                "row_alias": "row1",
                            }
                        ],
                    )
                else:
                    query = get_is_table_not_empty_query(
                        "encounter", f"{code_source['name']}.coding"
                    )
                cursor.execute(query)
                progress.advance(task)

                if len(cursor.fetchall()) > 0:
                    if code_source["is_array"]:
                        query = get_is_table_not_empty_query(
                            "encounter",
                            "t2.row2",
                            [
                                {
                                    "source_col": code_source["name"],
                                    "table_alias": "t1",
                                    "row_alias": "row1",
                                },
                                {
                                    "source_col": "row1.coding",
                                    "table_alias": "t2",
                                    "row_alias": "row2",
                                },
                            ],
                        )
                    else:
                        query = get_is_table_not_empty_query(
                            "encounter",
                            "t1.row1",
                            [
                                {
                                    "source_col": f"{code_source['name']}.coding",
                                    "table_alias": "t1",
                                    "row_alias": "row1",
                                }
                            ],
                        )
                    cursor.execute(query)
                    progress.advance(task)
                    if len(cursor.fetchall()) > 0:
                        cols_with_data.append(code_source)
                else:
                    progress.advance(task)
            else:
                progress.advance(task)
                progress.advance(task)
        return cols_with_data

    def prepare_queries(self, cursor: object, schema: str):
        """Constructs queries related to condition codeableConcept

        :param cursor: A database cursor object
        :param schema: the schema/db name, matching the cursor

        """
        code_sources = [
            {"name": "type", "is_array": True},
            {"name": "servicetype", "is_array": False},
            {"name": "priority", "is_array": False},
        ]
        discovery_length = len(code_sources) * 3

        with get_progress_bar(transient=True) as progress:
            task = progress.add_task(
                "Detecting available encounter codes...",
                total=discovery_length,
            )
            cols_with_data = self._check_data_in_fields(
                code_sources, cursor, progress, task
            )

        config = CodeableConceptConfig(
            source_table="encounter",
            source_id="id",
            cc_columns=cols_with_data,
            target_table="core__encounter_coding",
            code_systems=[
                "http://snomed.info/sct",
                "http://hl7.org/fhir/sid/icd-10-cm",
                "http://hl7.org/fhir/sid/icd-9-cm",
            ],
        )
        self.queries.append(get_codeable_concept_denormalize_query(config))
