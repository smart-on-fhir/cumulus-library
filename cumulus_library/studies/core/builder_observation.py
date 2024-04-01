""" Module for extracting US core extensions from patient records"""

from dataclasses import dataclass

from cumulus_library import base_table_builder, databases
from cumulus_library.studies.core.core_templates import core_templates
from cumulus_library.template_sql import sql_utils

expected_table_cols = {
    "observation": {
        "id": [],
        "category": ["coding", "code", "display", "system", "text"],
        "status": [],
        "code": ["coding", "code", "display", "system", "text"],
        "interpretation": ["coding", "code", "display", "system", "text"],
        "referenceRange": [
            "low",
            "high",
            "normalValue",
            "type",
            "appliesTo",
            "age",
            "text",
        ],
        "effectiveDateTime": [],
        "valueQuantity": ["value", "comparator", "unit", "system", "code"],
        "valueCodeableConcept": ["coding", "code", "display", "system"],
        "valueString": [],
        "subject": ["reference"],
        "encounter": ["reference"],
    }
}


@dataclass(kw_only=True)
class ObsConfig(sql_utils.CodeableConceptConfig):
    source_table: str = "observation"

    def __post_init__(self):
        self.target_table = f"core__observation_dn_{self.column_hierarchy[-1][0]}"


class ObservationBuilder(base_table_builder.BaseTableBuilder):
    display_text = "Creating Observation tables..."

    def prepare_queries(
        self,
        cursor: object,
        schema: str,
        *args,
        parser: databases.DatabaseParser = None,
        **kwargs,
    ):
        """constructs queries related to patient extensions of interest

        :param cursor: A database cursor object
        :param schema: the schema/db name, matching the cursor
        """
        code_sources = [
            ObsConfig(column_hierarchy=[("category", list)], filter_priority=False),
            ObsConfig(column_hierarchy=[("code", dict)], filter_priority=False),
            ObsConfig(
                column_hierarchy=[("interpretation", list)], filter_priority=False
            ),
            ObsConfig(
                column_hierarchy=[("valuecodeableconcept", dict)],
                filter_priority=False,
            ),
            ObsConfig(
                column_hierarchy=[("dataabsentreason", dict)],
                filter_priority=False,
            ),
        ]

        self.queries += sql_utils.denormalize_complex_objects(
            schema, cursor, code_sources
        )
        validated_schema = core_templates.validate_schema(
            cursor, schema, expected_table_cols, parser
        )
        self.queries.append(
            core_templates.get_core_template("observation", validated_schema)
        )
