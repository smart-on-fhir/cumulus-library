from cumulus_library import base_table_builder, databases
from cumulus_library.studies.core.core_templates import core_templates
from cumulus_library.template_sql import base_templates, sql_utils

expected_table_cols = {
    "condition": {
        "category": [
            "coding",
            "code",
            "display",
            "system",
            "userSelected",
            "version",
            "text",
        ],
        "clinicalstatus": [
            "coding",
            "code",
            "display",
            "system",
            "userSelected",
            "version",
            "text",
        ],
        "id": [],
        "recordedDate": [],
        "verificationstatus": [
            "coding",
            "code",
            "display",
            "system",
            "userSelected",
            "version",
            "text",
        ],
        "subject": ["reference", "display", "type"],
        "encounter": ["reference", "display", "type"],
    }
}


class CoreConditionBuilder(base_table_builder.BaseTableBuilder):
    display_text = "Creating Condition tables..."

    def denormalize_codes(self):
        preferred_config = sql_utils.CodeableConceptConfig(
            source_table="condition",
            source_id="id",
            column_hierarchy=[("code", dict)],
            target_table="core__condition_codable_concepts_display",
            filter_priority=True,
            code_systems=[
                "http://snomed.info/sct",
                "http://hl7.org/fhir/sid/icd-10-cm",
                "http://hl7.org/fhir/sid/icd-9-cm",
                "http://hl7.org/fhir/sid/icd-9-cm/diagnosis",
                # EPIC specific systems
                "urn:oid:1.2.840.114350.1.13.71.2.7.2.728286",
                "urn:oid:1.2.840.114350.1.13.71.2.7.4.698084.10375",
                # Spec allowed code of last resort
                "http://terminology.hl7.org/CodeSystem/data-absent-reason",
            ],
        )
        self.queries.append(
            base_templates.get_codeable_concept_denormalize_query(preferred_config)
        )

        all_config = sql_utils.CodeableConceptConfig(
            source_table="condition",
            source_id="id",
            column_hierarchy=[("code", dict)],
            target_table="core__condition_codable_concepts_all",
            filter_priority=False,
        )
        self.queries.append(
            base_templates.get_codeable_concept_denormalize_query(all_config)
        )

    def prepare_queries(
        self,
        cursor: object,
        schema: str,
        *args,
        parser: databases.DatabaseParser = None,
        **kwargs,
    ):
        self.denormalize_codes()
        validated_schema = core_templates.validate_schema(
            cursor, schema, expected_table_cols, parser
        )
        self.queries.append(
            core_templates.get_core_template("condition", validated_schema)
        )
