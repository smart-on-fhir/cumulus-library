"""Module for generating core medication table"""

from cumulus_library import base_table_builder, base_utils
from cumulus_library.studies.core.core_templates import core_templates
from cumulus_library.template_sql import sql_utils

expected_table_cols = {
    "medicationrequest": {
        "id": [],
        "subject": sql_utils.REFERENCE,
        "encounter": sql_utils.REFERENCE,
        "medicationReference": sql_utils.REFERENCE,
    }
}


class MedicationBuilder(base_table_builder.BaseTableBuilder):
    display_text = "Creating Medication table..."

    def prepare_queries(
        self,
        *args,
        config: base_utils.StudyConfig,
        **kwargs,
    ) -> None:
        """Constructs queries related to condition codeableConcept

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
        ]
        self.queries += sql_utils.denormalize_complex_objects(config.db, code_sources)
        validated_schema = sql_utils.validate_schema(config.db, expected_table_cols)
        self.queries += [
            core_templates.get_core_template("medication", validated_schema),
        ]
