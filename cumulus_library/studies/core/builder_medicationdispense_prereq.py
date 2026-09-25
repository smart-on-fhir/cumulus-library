"""Module for generating core medicationdispense prerequisite tables"""

import cumulus_library
from cumulus_library.template_sql import sql_utils


class MedicationDispenseBuilder(cumulus_library.BaseTableBuilder):
    display_text = "Creating MedicationDispense tables..."

    def prepare_queries(
        self,
        *args,
        config: cumulus_library.StudyConfig,
        **kwargs,
    ) -> None:
        # MedicationDispense can have a reference to core__medication_dn_code from
        # builder_medicationrequest_prereq. Do not duplicate the creation.
        code_sources = [
            sql_utils.CodeableConceptConfig(
                source_table="medicationdispense",
                column_hierarchy=[("medicationCodeableConcept", dict)],
                target_table="core__medicationdispense_dn_inline_code",
            ),
            sql_utils.CodeableConceptConfig(
                source_table="medicationdispense",
                column_hierarchy=[("contained", list), ("code", dict)],
                target_table="core__medicationdispense_dn_contained_code",
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
                source_table="medicationdispense",
                column_hierarchy=[("category", dict)],
                target_table="core__medicationdispense_dn_category",
            ),
            sql_utils.CodeableConceptConfig(
                source_table="medicationdispense",
                column_hierarchy=[("type", dict)],
                target_table="core__medicationdispense_dn_type",
            ),
            sql_utils.CodeableConceptConfig(
                source_table="medicationdispense",
                column_hierarchy=[("dosageInstruction", list), ("route", dict)],
                target_table="core__medicationdispense_dn_dosage_route",
                # dosageInstruction is a Dosage, not a CodeableConcept, so the
                # default expected shape would never match.
                expected={"route": sql_utils.CODEABLE_CONCEPT},
            ),
        ]
        self.queries += sql_utils.denormalize_complex_objects(
            config.db, code_sources, "MedicationDispense"
        )
