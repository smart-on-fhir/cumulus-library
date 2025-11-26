"""Module for extracting US core extensions from patient records"""

import cumulus_library
from cumulus_library.studies.core.core_templates import core_templates
from cumulus_library.template_sql import sql_utils

expected_table_cols = {
    "patient": {
        "id": [],
        "gender": [],
        "address": {"postalCode": {}},
        "birthDate": [],
    }
}


class PatientBuilder(cumulus_library.BaseTableBuilder):
    display_text = "Creating Patient tables..."

    def prepare_queries(
        self,
        *args,
        config: cumulus_library.StudyConfig,
        **kwargs,
    ):
        """constructs queries related to patient extensions of interest

        :param config: A study config object
        """
        validated_schema = sql_utils.validate_schema(config.db, expected_table_cols)
        self.queries.append(core_templates.get_core_template("patient", validated_schema))
