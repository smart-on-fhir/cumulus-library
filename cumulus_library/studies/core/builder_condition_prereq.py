import cumulus_library
from cumulus_library.template_sql import base_templates, sql_utils


class CoreConditionBuilder(cumulus_library.BaseTableBuilder):
    display_text = "Creating Condition prerequesite tables..."

    def denormalize_codes(self):
        configs = [
            sql_utils.CodeableConceptConfig(
                source_table="condition",
                column_hierarchy=[("category", list)],
                target_table="core__condition_dn_category",
                # This is an extensible binding, and US Core already suggests three
                # different code systems to pull its recommended four values from.
                # So let's not filter by system here.
            ),
            sql_utils.CodeableConceptConfig(
                source_table="condition",
                column_hierarchy=[("clinicalStatus", dict)],
                target_table="core__condition_dn_clinical_status",
                filter_priority=True,
                code_systems=[
                    # Restrict to just this required binding system
                    "http://terminology.hl7.org/CodeSystem/condition-clinical",
                ],
            ),
            sql_utils.CodeableConceptConfig(
                source_table="condition",
                column_hierarchy=[("code", dict)],
                target_table="core__condition_codable_concepts_display",
                filter_priority=True,
                code_systems=[
                    "http://snomed.info/sct",
                    "http://hl7.org/fhir/sid/icd-10-cm",
                    "http://hl7.org/fhir/sid/icd-9-cm",
                    "http://hl7.org/fhir/sid/icd-9-cm/diagnosis",
                    # EPIC specific systems (missing bit is "713.3" for BCH, for example)
                    "urn:oid:1.2.840.114350.1.13.%.7.2.728286",
                    "urn:oid:1.2.840.114350.1.13.%.7.4.698084.10375",
                    # Spec allowed code of last resort
                    "http://terminology.hl7.org/CodeSystem/data-absent-reason",
                ],
            ),
            sql_utils.CodeableConceptConfig(
                source_table="condition",
                column_hierarchy=[("code", dict)],
                target_table="core__condition_codable_concepts_all",
            ),
            sql_utils.CodeableConceptConfig(
                source_table="condition",
                column_hierarchy=[("verificationStatus", dict)],
                target_table="core__condition_dn_verification_status",
                filter_priority=True,
                code_systems=[
                    # Restrict to just this required binding system
                    "http://terminology.hl7.org/CodeSystem/condition-ver-status",
                ],
            ),
        ]
        self.queries += [
            base_templates.get_codeable_concept_denormalize_query(config) for config in configs
        ]

    def prepare_queries(
        self,
        *args,
        config: cumulus_library.StudyConfig,
        **kwargs,
    ):
        self.denormalize_codes()
