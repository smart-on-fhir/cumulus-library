import cumulus_library
from cumulus_library.studies.core.core_templates import core_templates
from cumulus_library.template_sql import sql_utils

expected_table_cols = {
    "diagnosticreport": {
        "id": [],
        "status": [],
        "subject": sql_utils.REFERENCE,
        "encounter": sql_utils.REFERENCE,
        "effectiveDateTime": [],
        "effectivePeriod": ["start", "end"],
        "issued": [],
        "result": sql_utils.REFERENCE,
        "presentedForm": {
            "contentType": [],
            "data": [],
            "_data": {"extension": ["url", "valueCode"]},
        },
    }
}


class CoreDiagnosticReportBuilder(cumulus_library.BaseTableBuilder):
    display_text = "Creating DiagnosticReport tables..."

    def prepare_queries(self, *args, config: cumulus_library.StudyConfig, **kwargs):
        validated_schema = sql_utils.validate_schema(config.db, expected_table_cols)
        self.queries.append(core_templates.get_core_template("diagnosticreport", validated_schema))
