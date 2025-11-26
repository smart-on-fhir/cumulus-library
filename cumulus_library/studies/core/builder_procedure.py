import cumulus_library
from cumulus_library.studies.core.core_templates import core_templates
from cumulus_library.template_sql import sql_utils

expected_table_cols = {
    "procedure": {
        "id": [],
        "status": [],
        "subject": sql_utils.REFERENCE,
        "encounter": sql_utils.REFERENCE,
        "performedDateTime": [],
        "performedPeriod": ["start", "end"],
    }
}


class CoreProcedureBuilder(cumulus_library.BaseTableBuilder):
    display_text = "Creating Procedure tables..."

    def prepare_queries(self, *args, config: cumulus_library.StudyConfig, **kwargs):
        validated_schema = sql_utils.validate_schema(config.db, expected_table_cols)
        self.queries.append(core_templates.get_core_template("procedure", validated_schema))
