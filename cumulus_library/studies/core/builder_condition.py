from cumulus_library import base_table_builder
from cumulus_library import databases
from cumulus_library.studies.core.core_templates import core_templates
from cumulus_library.template_sql import templates


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
        "recordeddate": [],
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
    def denormalize_codes(self):
        preferred_config = templates.CodeableConceptConfig(
            source_table="condition",
            source_id="id",
            column_name="code",
            is_array=False,
            target_table="core__condition_codable_concepts_display",
            filter_priority=True,
            code_systems=[
                "http://snomed.info/sct",
                "http://hl7.org/fhir/sid/icd-10-cm",
                "http://hl7.org/fhir/sid/icd-9-cm",
            ],
        )
        self.queries.append(
            templates.get_codeable_concept_denormalize_query(preferred_config)
        )

        all_config = templates.CodeableConceptConfig(
            source_table="condition",
            source_id="id",
            column_name="code",
            is_array=False,
            target_table="core__condition_codable_concepts_all",
            filter_priority=False,
        )
        self.queries.append(
            templates.get_codeable_concept_denormalize_query(all_config)
        )

    def validate_schema(self, cursor: object, schema: str, expected_table_cols, parser):
        validated_schema = {}
        for table, cols in expected_table_cols.items():
            query = templates.get_column_datatype_query(schema, table, cols.keys())
            table_schema = cursor.execute(query).fetchall()
            validated_schema[table] = parser.validate_table_schema(cols, table_schema)
        return validated_schema

    def prepare_queries(
        self,
        cursor: object,
        schema: str,
        *args,
        parser: databases.DatabaseParser = None,
        **kwargs,
    ):
        self.denormalize_codes()
        validated_schema = self.validate_schema(
            cursor, schema, expected_table_cols, parser
        )
        self.queries.append(
            core_templates.get_core_template("condition", validated_schema)
        )