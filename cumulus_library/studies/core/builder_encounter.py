from cumulus_library import base_table_builder, databases
from cumulus_library.studies.core.core_templates import core_templates
from cumulus_library.template_sql import sql_utils

expected_table_cols = {
    "encounter": {
        "status": [],
        "period": [
            "start",
            "end",
        ],
        "class": [
            "code",
            "system",
            "display",
            "userSelected",
            "version",
        ],
        "subject": [
            "reference",
            "display",
            "type",
        ],
        "id": [],
    }
}


class CoreEncounterBuilder(base_table_builder.BaseTableBuilder):
    display_text = "Creating Encounter tables..."

    def denormalize_codes(self, schema, cursor):
        code_sources = [
            {
                "column_name": "type",
                "is_array": True,
                "filter_priority": True,
                "code_systems": [
                    "http://terminology.hl7.org/CodeSystem/encounter-type",
                    "http://terminology.hl7.org/CodeSystem/v2-0004",
                    "urn:oid:2.16.840.1.113883.4.642.3.248",
                    "http://snomed.info/sct",
                    "https://fhir.cerner.com/96976f07-eccb-424c-9825-e0d0b887148b/codeSet/71",
                    "urn:oid:1.2.840.114350.1.13.71.2.7.10",
                    "urn:oid:1.2.840.114350.1.13.71.2.7.2",
                ],
                "has_data": False,
            },
            {
                "column_name": "servicetype",
                "is_array": False,
                "filter_priority": True,
                "code_systems": [
                    "http://terminology.hl7.org/CodeSystem/service-type",
                    "urn:oid:2.16.840.1.113883.4.642.3.518",
                    "http://snomed.info/sct",
                    "https://fhir.cerner.com/96976f07-eccb-424c-9825-e0d0b887148b/codeSet/34",
                    "urn:oid:1.2.840.114350.1.13.71.2.7.10",
                ],
                "has_data": False,
            },
            {
                "column_name": "priority",
                "is_array": False,
                "filter_priority": True,
                "code_systems": [
                    "http://terminology.hl7.org/CodeSystem/v3-ActPriority",
                    "http://snomed.info/sct",
                    "https://fhir.cerner.com/96976f07-eccb-424c-9825-e0d0b887148b/codeSet/3",
                    "urn:oid:1.2.840.114350.1.13.71.2.7.10",
                ],
                "has_data": False,
            },
            {
                "column_name": "reasoncode",
                "is_array": True,
                "filter_priority": True,
                "code_systems": [
                    "http://terminology.hl7.org/CodeSystem/v3-ActPriority",
                    "http://snomed.info/sct",
                    "http://hl7.org/fhir/sid/icd-10-cm",
                    "http://hl7.org/fhir/sid/icd-9-cm",
                    "https://fhir.cerner.com/96976f07-eccb-424c-9825-e0d0b887148b/nomenclature",
                    "urn:oid:1.2.840.114350.1.13.71.2.7.2",
                ],
                "has_data": False,
            },
        ]
        code_configs = []
        for code_source in code_sources:
            code_configs.append(
                sql_utils.CodeableConceptConfig(
                    source_table="encounter",
                    source_id="id",
                    column_name=code_source["column_name"],
                    is_array=code_source["is_array"],
                    filter_priority=code_source["filter_priority"],
                    code_systems=code_source["code_systems"],
                    target_table=f"core__encounter_dn_{code_source['column_name']}",
                )
            )
        self.queries += sql_utils.denormalize_codes(schema, cursor, code_configs)

    def prepare_queries(
        self,
        cursor: object,
        schema: str,
        *args,
        parser: databases.DatabaseParser = None,
        **kwargs,
    ):
        self.denormalize_codes(
            schema,
            cursor,
        )
        validated_schema = core_templates.validate_schema(
            cursor, schema, expected_table_cols, parser
        )
        self.queries.append(
            core_templates.get_core_template("encounter", validated_schema)
        )
