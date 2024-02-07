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
        "referencerange": [
            "low",
            "high",
            "normalvalue",
            "type",
            "appliesto",
            "age",
            "text",
        ],
        "effectivedatetime": [],
        "valuequantity": ["value", "comparator", "unit", "system", "code"],
        "valuecodeableconcept": ["coding", "code", "display", "system"],
        "subject": ["reference"],
        "encounter": ["reference"],
    }
}


@dataclass(kw_only=True)
class ObsConfig(sql_utils.CodeableConceptConfig):
    source_table: str = "observation"

    def __post_init__(self):
        self.target_table = f"core__observation_dn_{self.column_name}"


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
            ObsConfig(column_name="category", is_array=True, filter_priority=False),
            ObsConfig(column_name="code", is_array=False, filter_priority=False),
            ObsConfig(
                column_name="interpretation", is_array=True, filter_priority=False
            ),
            ObsConfig(
                column_name="valuecodeableconcept",
                is_array=False,
                filter_priority=False,
            ),
        ]

        self.queries += sql_utils.denormalize_codes(schema, cursor, code_sources)
        validated_schema = core_templates.validate_schema(
            cursor, schema, expected_table_cols, parser
        )
        self.queries.append(
            core_templates.get_core_template("observation", validated_schema)
        )
