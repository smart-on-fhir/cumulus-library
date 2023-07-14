""" Module for generating condition codeableConcept table"""

from cumulus_library.base_table_builder import BaseTableBuilder
from cumulus_library.template_sql.templates import (
    get_codeable_concept_denormalize_query,
)


class ConditionCodableConceptRunner(BaseTableBuilder):
    def __init__(self):
        super().__init__()
        self.display_text = "Creating condition code table..."

    def prepare_queries(self, cursor: object, schema: str):
        """Constructs queries related to condition codeableConcept

        :param cursor: A database cursor object
        :param schema: the schema/db name, matching the cursor

        """
        self.queries.append(
            get_codeable_concept_denormalize_query(
                "condition",
                "code",
                "core__condition_codable_concepts",
                [
                    "http://snomed.info/sct",
                    "http://hl7.org/fhir/sid/icd-10-cm",
                    "http://hl7.org/fhir/sid/icd-9-cm",
                ],
            )
        )
