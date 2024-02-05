""" Module for extracting US core extensions from medicationrequests

Note: This module assumes that you have already run builder_medication,
as it leverages the core__medication table for data population"""

from cumulus_library import base_table_builder
from cumulus_library.template_sql import base_templates, sql_utils
from cumulus_library import databases
from cumulus_library.studies.core.core_templates import core_templates

expected_table_cols = {
    "medicationrequest": {
        "id": [],
        "status": [],
        "intent": [],
        "authoredon": [],
        "category": ["code", "system", "display"],
        "subject": ["reference"],
    }
}


class MedicationRequestBuilder(base_table_builder.BaseTableBuilder):
    display_text = "Creating MedicationRequest tables..."

    def denormalize_codes(self):
        preferred_config = sql_utils.CodeableConceptConfig(
            source_table="medicationrequest",
            source_id="id",
            column_name="category",
            is_array=True,
            target_table="core__medicationrequest_dn_category",
            filter_priority=False,
        )
        self.queries.append(
            base_templates.get_codeable_concept_denormalize_query(preferred_config)
        )

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
        self.denormalize_codes()
        validated_schema = core_templates.validate_schema(
            cursor, schema, expected_table_cols, parser
        )
        self.queries.append(
            core_templates.get_core_template("medicationrequest", validated_schema)
        )
