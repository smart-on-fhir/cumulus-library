"""Module for generating core medicationrequest table"""

import cumulus_library
from cumulus_library.studies.core.core_templates import core_templates
from cumulus_library.template_sql import sql_utils

expected_table_cols = {
    "medicationrequest": {
        "id": [],
        "status": [],
        "intent": [],
        "authoredOn": [],
        "requester": sql_utils.REFERENCE,
        "reportedBoolean": [],
        "reportedReference": sql_utils.REFERENCE,
        "subject": sql_utils.REFERENCE,
        "encounter": sql_utils.REFERENCE,
        "medicationReference": sql_utils.REFERENCE,
    }
}


class MedicationRequestBuilder(cumulus_library.BaseTableBuilder):
    display_text = "Creating MedicationRequest table..."

    def prepare_queries(
        self,
        *args,
        config: cumulus_library.StudyConfig,
        **kwargs,
    ) -> None:
        """Constructs queries related to medication requests

        :param config: A study config object
        """
        validated_schema = sql_utils.validate_schema(config.db, expected_table_cols)
        self.queries += [
            core_templates.get_core_template("medicationrequest", validated_schema),
        ]
