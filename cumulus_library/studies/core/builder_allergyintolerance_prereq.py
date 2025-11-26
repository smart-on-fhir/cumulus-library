import cumulus_library
from cumulus_library.template_sql import sql_utils


class CoreAllergyIntolerancePrereqBuilder(cumulus_library.BaseTableBuilder):
    display_text = "Creating AllergyIntolerance codeableConcept tables..."

    def prepare_queries(self, *args, config: cumulus_library.StudyConfig, **kwargs):
        code_sources = [
            sql_utils.CodeableConceptConfig(
                source_table="allergyintolerance",
                column_hierarchy=[("clinicalStatus", dict)],
                target_table="core__allergyintolerance_dn_clinical_status",
                filter_priority=True,
                code_systems=[
                    # Restrict to just this required binding system
                    "http://terminology.hl7.org/CodeSystem/allergyintolerance-clinical",
                ],
            ),
            sql_utils.CodeableConceptConfig(
                source_table="allergyintolerance",
                column_hierarchy=[("verificationStatus", dict)],
                target_table="core__allergyintolerance_dn_verification_status",
                filter_priority=True,
                code_systems=[
                    # Restrict to just this required binding system
                    "http://terminology.hl7.org/CodeSystem/allergyintolerance-verification",
                ],
            ),
            sql_utils.CodeableConceptConfig(
                source_table="allergyintolerance",
                column_hierarchy=[("code", dict)],
                target_table="core__allergyintolerance_dn_code",
            ),
            sql_utils.CodeableConceptConfig(
                source_table="allergyintolerance",
                column_hierarchy=[("reaction", list), ("substance", dict)],
                target_table="core__allergyintolerance_dn_reaction_substance",
                expected={"substance": sql_utils.CODEABLE_CONCEPT},
            ),
            sql_utils.CodeableConceptConfig(
                source_table="allergyintolerance",
                column_hierarchy=[("reaction", list), ("manifestation", list)],
                target_table="core__allergyintolerance_dn_reaction_manifestation",
                expected={"manifestation": sql_utils.CODEABLE_CONCEPT},
            ),
        ]
        self.queries += sql_utils.denormalize_complex_objects(
            config.db, code_sources, "AllergyIntolerance"
        )
