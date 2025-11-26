import cumulus_library
from cumulus_library.template_sql import sql_utils


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
        self.queries += sql_utils.denormalize_complex_objects(
            config.db, code_sources, "DiagnosticReport"
        )
