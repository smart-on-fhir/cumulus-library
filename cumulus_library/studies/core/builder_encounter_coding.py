""" Module for generating encounter codeableConcept table"""

from cumulus_library.base_table_builder import BaseTableBuilder
from cumulus_library.helper import get_progress_bar, query_console_output
from cumulus_library.template_sql.templates import (
    CodeableConceptConfig,
    get_codeable_concept_denormalize_query,
    get_is_table_not_empty_query,
    get_ctas_empty_query,
)


class EncounterCodingBuilder(BaseTableBuilder):
    display_text = "Creating encounter codeableConcept tables..."

    def _check_data_in_fields(self, code_sources: list[dict], cursor) -> dict:
        """checks if CodeableConcept fields actually have data available

        CodeableConcept fields are mostly optional in the FHIR spec, and may be arrays
        or single objects. Additionally, the null representation can be inconsistent,
        depending on how the data is provided from an EHR and how the ETL manages
        schema inference (wide, but not deep). We :could: try to find the data and
        just catch an error, but that would mask configuration errors/unexpected
        data patterns. So, instead, we are doing the following fussy operation:

        For each column we want to check for data:
        - Check to see if there is any data in a codeableConcept field
        - Check to see if the codeableConcept field contains a coding element
        - Check if that coding element contains non-null data

        The way we do this is slightly different depending on if the field is an
        array or not (generally requiring one extra level of unnesting).

        """
        # TODO: consider moving to a utility library if we have another case like
        # this one

        with get_progress_bar(transient=True) as progress:
            task = progress.add_task(
                "Detecting available encounter codaebleConcepts...",
                # Each column in code_sources requires at most 3 queries to
                # detect valid data is in the DB
                total=len(code_sources) * 3,
            )

            for code_source in code_sources:
                query = get_is_table_not_empty_query("encounter", code_source["name"])
                cursor.execute(query)
                progress.advance(task)
                res = cursor.fetchone()
                if len(res) > 0:
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
                            code_source["has_data"] = True
                    else:
                        progress.advance(task)
                else:
                    progress.advance(task, advance=2)
        return code_sources

    def prepare_queries(self, cursor: object, schema: str):
        """Constructs queries related to condition codeableConcept

        :param cursor: A database cursor object
        :param schema: the schema/db name, matching the cursor

        """

        code_sources = [
            {
                "name": "type",
                "is_array": True,
                "code_systems": [
                    "http://hl7.org/fhir/ValueSet/encounter-type",
                    "http://terminology.hl7.org/CodeSystem/encounter-type",
                    "http://terminology.hl7.org/CodeSystem/v2-0004",
                    "2.16.840.1.113883.4.642.3.248",
                    "http://snomed.info/sct",
                ],
                "has_data": False,
            },
            {
                "name": "servicetype",
                "is_array": False,
                "code_systems": [
                    "http://hl7.org/fhir/ValueSet/service-type",
                    "http://terminology.hl7.org/CodeSystem/service-type",
                    "2.16.840.1.113883.4.642.3.518",
                    "http://snomed.info/sct",
                ],
                "has_data": False,
            },
            {
                "name": "priority",
                "is_array": False,
                "code_systems": [
                    "http://terminology.hl7.org/ValueSet/v3-ActPriority",
                    "http://terminology.hl7.org/CodeSystem/v3-ActPriority",
                    "http://snomed.info/sct",
                ],
                "has_data": False,
            },
        ]
        code_sources = self._check_data_in_fields(code_sources, cursor)
        for code_source in code_sources:
            if code_source["has_data"]:
                config = CodeableConceptConfig(
                    source_table="encounter",
                    source_id="id",
                    cc_column=code_source,
                    target_table=f"core__encounter_dn_{code_source['name']}",
                )
                self.queries.append(get_codeable_concept_denormalize_query(config))
            else:
                self.queries.append(
                    get_ctas_empty_query(
                        schema_name=schema,
                        table_name=f"core__encounter_dn_{code_source['name']}",
                        table_cols=["id", "code", "code_system", "display"],
                    )
                )