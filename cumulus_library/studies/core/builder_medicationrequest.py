"""Module for extracting US core extensions from medicationrequests

Note: This module assumes that you have already run builder_medication,
as it leverages the core__medication table for data population"""

from cumulus_library import base_table_builder, base_utils
from cumulus_library.studies.core.core_templates import core_templates
from cumulus_library.template_sql import sql_utils

expected_table_cols = {
    "medicationrequest": {
        "id": [],
        "status": [],
        "intent": [],
        "authoredOn": [],
        "reportedBoolean": [],
        "subject": sql_utils.REFERENCE,
        "encounter": sql_utils.REFERENCE,
        "dosageInstruction": ["text"],
    }
}


class MedicationRequestBuilder(base_table_builder.BaseTableBuilder):
    display_text = "Creating MedicationRequest tables..."

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
            sql_utils.CodeableConceptConfig(
                source_table="medicationrequest",
                source_id="id",
                column_hierarchy=[("category", list)],
                target_table="core__medicationrequest_dn_category",
            ),
        ]
        self.queries += sql_utils.denormalize_complex_objects(config.db, code_sources)
        validated_schema = sql_utils.validate_schema(config.db, expected_table_cols)
        self.queries.append(
            core_templates.get_core_template("medicationrequest", validated_schema)
        )
