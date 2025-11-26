from dataclasses import dataclass

import cumulus_library
from cumulus_library.template_sql import sql_utils


@dataclass(kw_only=True)
class EncConfig(sql_utils.CodeableConceptConfig):
    """Convenience wrapper for CodeableConceptConfig"""

    source_table: str = "encounter"

    def __post_init__(self):
        self.target_table = f"core__encounter_dn_{self.column_hierarchy[-1][0]}"


class CoreEncounterBuilder(cumulus_library.BaseTableBuilder):
    display_text = "Creating Encounter tables..."

    def prepare_queries(
        self,
        *args,
        config: cumulus_library.StudyConfig,
        **kwargs,
    ):
        code_configs = [
            EncConfig(
                column_hierarchy=[("type", list)],
                filter_priority=True,
                code_systems=[
                    "http://terminology.hl7.org/CodeSystem/encounter-type",
                    "http://terminology.hl7.org/CodeSystem/v2-0004",
                    "http://snomed.info/sct",
                    "http://www.ama-assn.org/go/cpt",
                    # HL7 OID version of http://hl7.org/fhir/ValueSet/encounter-type
                    "urn:oid:2.16.840.1.113883.4.642.3.248",
                    # Cerner specific systems
                    "https://fhir.cerner.com/%/codeSet/71",
                    # EPIC specific systems (missing bit is "713.3" for BCH, for example)
                    "urn:oid:1.2.840.114350.1.13.%.7.10.698084.10110",
                    "urn:oid:1.2.840.114350.1.13.%.7.10.698084.18875",
                    "urn:oid:1.2.840.114350.1.13.%.7.10.698084.30",
                    "urn:oid:1.2.840.114350.1.13.%.7.2.808267",
                ],
            ),
            EncConfig(
                column_hierarchy=[("servicetype", dict)],
                filter_priority=True,
                code_systems=[
                    "http://terminology.hl7.org/CodeSystem/service-type",
                    "http://snomed.info/sct",
                    # HL7 OID version of http://hl7.org/fhir/ValueSet/service-type
                    "urn:oid:2.16.840.1.113883.4.642.3.518",
                    # Cerner specific systems
                    "https://fhir.cerner.com/%/codeSet/34",
                    # EPIC specific systems (missing bit is "713.3" for BCH, for example)
                    "urn:oid:1.2.840.114350.1.13.%.7.10.698084.18886",
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
                    # EPIC specific systems (missing bit is "713.3" for BCH, for example)
                    "urn:oid:1.2.840.114350.1.13.%.7.10.698084.410",
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
                    # EPIC specific systems (missing bit is "713.3" for BCH, for example)
                    "urn:oid:1.2.840.114350.1.13.%.7.2.728286",
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
        self.queries += sql_utils.denormalize_complex_objects(config.db, code_configs, "Encounter")
