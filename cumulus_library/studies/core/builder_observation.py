"""Module for extracting US core extensions from patient records"""

from dataclasses import dataclass

from cumulus_library import base_table_builder, base_utils
from cumulus_library.studies.core.core_templates import core_templates
from cumulus_library.template_sql import sql_utils

expected_table_cols = {
    "observation": {
        "id": [],
        "component": {
            "valueQuantity": ["code", "comparator", "system", "unit", "value"],
        },
        "status": [],
        "effectiveDateTime": [],
        "valueQuantity": ["code", "comparator", "system", "unit", "value"],
        "valueString": [],
        "subject": sql_utils.REFERENCE,
        "encounter": sql_utils.REFERENCE,
    }
}


@dataclass(kw_only=True)
class ObsConfig(sql_utils.CodeableConceptConfig):
    source_table: str = "observation"
    is_public: bool = False

    def __post_init__(self):
        # Consideration for future: should all denormalized tables be public?
        # For now, we'll mark the ones we want to encourage use of,
        # and for those, remove the maybe-confusing denormalization tag.
        table_suffix = "" if self.is_public else "dn_"
        table_suffix += "_".join(c[0] for c in self.column_hierarchy)
        self.target_table = f"core__observation_{table_suffix}"


class ObservationBuilder(base_table_builder.BaseTableBuilder):
    display_text = "Creating Observation tables..."

    def prepare_queries(
        self,
        *args,
        config: base_utils.StudyConfig,
        **kwargs,
    ):
        """constructs queries related to patient extensions of interest

        :param config: A study config object
        """
        code_sources = [
            ObsConfig(column_hierarchy=[("category", list)], filter_priority=False),
            ObsConfig(column_hierarchy=[("code", dict)], filter_priority=False),
            ObsConfig(
                is_public=True,
                column_hierarchy=[("component", list), ("code", dict)],
                expected={"code": sql_utils.CODEABLE_CONCEPT},
            ),
            ObsConfig(
                is_public=True,
                column_hierarchy=[("component", list), ("dataabsentreason", dict)],
                expected={"dataabsentreason": sql_utils.CODEABLE_CONCEPT},
            ),
            ObsConfig(
                is_public=True,
                column_hierarchy=[("component", list), ("interpretation", list)],
                expected={"interpretation": sql_utils.CODEABLE_CONCEPT},
            ),
            ObsConfig(
                is_public=True,
                column_hierarchy=[("component", list), ("valuecodeableconcept", dict)],
                expected={"valuecodeableconcept": sql_utils.CODEABLE_CONCEPT},
            ),
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

        self.queries += sql_utils.denormalize_complex_objects(config.db, code_sources)
        validated_schema = sql_utils.validate_schema(config.db, expected_table_cols)
        self.queries += [
            core_templates.get_core_template("observation", validated_schema),
            core_templates.get_core_template(
                "observation_component_valuequantity", validated_schema
            ),
        ]
