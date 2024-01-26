""" Module for extracting US core extensions from patient records"""
from cumulus_library import base_table_builder
from cumulus_library.template_sql import templates, utils
from cumulus_library import databases
from cumulus_library.studies.core.core_templates import core_templates

CCC = utils.CodeableConceptConfig

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


# TODO: upgrade to 3.10+, use kw_only flag to subclass a dataclass for generating source/target
code_sources = [
    CCC(column_name="category", is_array=True, filter_priority=False),
    CCC(column_name="code", is_array=False, filter_priority=False),
    CCC(column_name="interpretation", is_array=True, filter_priority=False),
    CCC(column_name="valuecodeableconcept", is_array=False, filter_priority=False),
]
for source in code_sources:
    source.source_table = "observation"
    source.target_table = f"core__observation_dn_{source.column_name}"


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
        self.queries += utils.denormalize_codes(schema, cursor, code_sources)
        validated_schema = core_templates.validate_schema(
            cursor, schema, expected_table_cols, parser
        )
        self.queries.append(
            core_templates.get_core_template("observation", validated_schema)
        )
        self.write_queries()
