from dataclasses import dataclass

from cumulus_library import base_table_builder, base_utils
from cumulus_library.studies.core.core_templates import core_templates
from cumulus_library.template_sql import sql_utils

expected_table_cols = {
    "encounter": {
        "id": [],
        "status": [],
        "period": [
            "start",
            "end",
        ],
        "class": sql_utils.CODING,
        "subject": sql_utils.REFERENCE,
    },
    "etl__completion": {
        "group_name": [],
    },
    "etl__completion_encounters": {
        "group_name": [],
    },
}


@dataclass(kw_only=True)
class EncConfig(sql_utils.CodeableConceptConfig):
    """Convenience wrapper for CodeableConceptConfig"""

    source_table: str = "encounter"

    def __post_init__(self):
        self.target_table = f"core__encounter_dn_{self.column_hierarchy[-1][0]}"


class CoreEncounterBuilder(base_table_builder.BaseTableBuilder):
    display_text = "Creating Encounter tables..."

    def denormalize_codes(self, database):
        code_configs = [
            EncConfig(
                column_hierarchy=[("type", list)],
                filter_priority=True,
                code_systems=[
                    "http://terminology.hl7.org/CodeSystem/encounter-type",
                    "http://terminology.hl7.org/CodeSystem/v2-0004",
                    "urn:oid:2.16.840.1.113883.4.642.3.248",
                    "http://snomed.info/sct",
                    # Cerner specific systems
                    "https://fhir.cerner.com/%/codeSet/71",
                    # EPIC specific systems
                    "urn:oid:1.2.840.114350.1.13.71.2.7.10.698084.10110",
                    "urn:oid:1.2.840.114350.1.13.71.2.7.10.698084.18875",
                    "urn:oid:1.2.840.114350.1.13.71.2.7.10.698084.30",
                    "urn:oid:1.2.840.114350.1.13.71.2.7.2.808267",
                ],
            ),
            EncConfig(
                column_hierarchy=[("servicetype", dict)],
                filter_priority=True,
                code_systems=[
                    "http://terminology.hl7.org/CodeSystem/service-type",
                    "http://snomed.info/sct",
                    # Cerner specific systems
                    "https://fhir.cerner.com/%/codeSet/34",
                    # EPIC specific systems
                    "urn:oid:2.16.840.1.113883.4.642.3.518",
                    "urn:oid:1.2.840.114350.1.13.71.2.7.10.698084.18886",
                ],
            ),
            EncConfig(
                column_hierarchy=[("priority", dict)],
                filter_priority=True,
                code_systems=[
                    "http://terminology.hl7.org/CodeSystem/v3-ActPriority",
                    "http://snomed.info/sct",
                    # Cerner specific systems
                    "https://fhir.cerner.com/%/codeSet/3",
                    # EPIC specific systems
                    "urn:oid:1.2.840.114350.1.13.71.2.7.10.698084.410",
                ],
            ),
            EncConfig(
                column_hierarchy=[("reasoncode", list)],
                filter_priority=True,
                code_systems=[
                    "http://terminology.hl7.org/CodeSystem/v3-ActPriority",
                    "http://snomed.info/sct",
                    "http://hl7.org/fhir/sid/icd-10-cm",
                    "http://hl7.org/fhir/sid/icd-9-cm",
                    # Cerner specific systems
                    "https://fhir.cerner.com/%/nomenclature",
                    # EPIC specific systems
                    "urn:oid:1.2.840.114350.1.13.71.2.7.2.728286",
                ],
            ),
            EncConfig(
                column_hierarchy=[
                    ("hospitalization", dict),
                    ("dischargedisposition", dict),
                ],
                expected={"dischargedisposition": sql_utils.CODEABLE_CONCEPT},
            ),
        ]
        self.queries += sql_utils.denormalize_complex_objects(database, code_configs)

    def prepare_queries(
        self,
        *args,
        config: base_utils.StudyConfig,
        **kwargs,
    ):
        self.denormalize_codes(config.db)
        validated_schema = sql_utils.validate_schema(config.db, expected_table_cols)
        self.queries += [
            core_templates.get_core_template("encounter", validated_schema),
            core_templates.get_core_template("incomplete_encounter", validated_schema),
        ]
