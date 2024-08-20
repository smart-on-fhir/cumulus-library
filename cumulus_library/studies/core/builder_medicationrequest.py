"""Module for generating core medicationrequest table"""

import cumulus_library
from cumulus_library.studies.core.core_templates import core_templates
from cumulus_library.template_sql import sql_utils

expected_table_cols = {
    "medicationrequest": {
        "id": [],
        "status": [],
        "intent": [],
        "authoredOn": [],
        "reportedBoolean": [],
        "reportedReference": sql_utils.REFERENCE,
        "subject": sql_utils.REFERENCE,
        "encounter": sql_utils.REFERENCE,
        "medicationReference": sql_utils.REFERENCE,
    }
}


class MedicationRequestBuilder(cumulus_library.BaseTableBuilder):
    display_text = "Creating MedicationRequest table..."

    def prepare_queries(
        self,
        *args,
        config: cumulus_library.StudyConfig,
        **kwargs,
    ) -> None:
        """Constructs queries related to medication requests

        :param config: A study config object
        """
        code_sources = [
            sql_utils.CodeableConceptConfig(
                source_table="medication",
                column_hierarchy=[("code", dict)],
                target_table="core__medication_dn_code",
            ),
            sql_utils.CodeableConceptConfig(
                source_table="medicationrequest",
                column_hierarchy=[("medicationCodeableConcept", dict)],
                target_table="core__medicationrequest_dn_inline_code",
            ),
            sql_utils.CodeableConceptConfig(
                source_table="medicationrequest",
                column_hierarchy=[("contained", list), ("code", dict)],
                target_table="core__medicationrequest_dn_contained_code",
                expected={
                    "code": sql_utils.CODEABLE_CONCEPT,
                    "id": {},
                    "resourceType": {},
                },
                extra_fields=[
                    ("id", "contained_id"),
                    ("resourceType", "resource_type"),
                ],
            ),
            sql_utils.CodeableConceptConfig(
                source_table="medicationrequest",
                column_hierarchy=[("category", list)],
                target_table="core__medicationrequest_dn_category",
            ),
        ]
        self.queries += sql_utils.denormalize_complex_objects(config.db, code_sources)
        validated_schema = sql_utils.validate_schema(config.db, expected_table_cols)
        self.queries += [
            core_templates.get_core_template("medicationrequest", validated_schema),
        ]
