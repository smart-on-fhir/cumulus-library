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
    }
}


class CoreDiagnosticReportBuilder(cumulus_library.BaseTableBuilder):
    display_text = "Creating DiagnosticReport tables..."

    def prepare_queries(self, *args, config: cumulus_library.StudyConfig, **kwargs):
        code_sources = [
            sql_utils.CodeableConceptConfig(
                source_table="diagnosticreport",
                column_hierarchy=[("category", list)],
                target_table="core__diagnosticreport_dn_category",
            ),
            sql_utils.CodeableConceptConfig(
                source_table="diagnosticreport",
                column_hierarchy=[("code", dict)],
                target_table="core__diagnosticreport_dn_code",
            ),
            sql_utils.CodeableConceptConfig(
                source_table="diagnosticreport",
                column_hierarchy=[("conclusionCode", list)],
                target_table="core__diagnosticreport_dn_conclusioncode",
            ),
        ]
        self.queries += sql_utils.denormalize_complex_objects(config.db, code_sources)
        validated_schema = sql_utils.validate_schema(config.db, expected_table_cols)
        self.queries.append(core_templates.get_core_template("diagnosticreport", validated_schema))
