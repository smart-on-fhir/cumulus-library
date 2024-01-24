from cumulus_library import base_table_builder
from cumulus_library import databases
from cumulus_library.studies.core.core_templates import core_templates
from cumulus_library.template_sql import templates
from cumulus_library.template_sql import utils


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
                ],
                "has_data": False,
            },
        ]
        code_configs = []
        for code_source in code_sources:
            code_configs.append(
                utils.CodeableConceptConfig(
                    source_table="encounter",
                    source_id="id",
                    column_name=code_source["column_name"],
                    is_array=code_source["is_array"],
                    filter_priority=code_source["filter_priority"],
                    code_systems=code_source["code_systems"],
                    target_table=f"core__encounter_dn_{code_source['column_name']}",
                )
            )
        self.queries += utils.denormalize_codes(schema, cursor, code_configs)

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
        self.write_queries()
