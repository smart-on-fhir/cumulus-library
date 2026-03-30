import cumulus_library
from cumulus_library.template_sql import sql_utils


class CoreServiceRequestBuilder(cumulus_library.BaseTableBuilder):
    display_text = "Creating ServiceRequest tables..."

    def prepare_queries(self, *args, config: cumulus_library.StudyConfig, **kwargs):
        code_sources = [
            sql_utils.CodeableConceptConfig(
                source_table="servicerequest",
                column_hierarchy=[("category", list)],
                target_table="core__servicerequest_dn_category",
            ),
            sql_utils.CodeableConceptConfig(
                source_table="servicerequest",
                column_hierarchy=[("code", dict)],
                target_table="core__servicerequest_dn_code",
            ),
        ]
        self.queries += sql_utils.denormalize_complex_objects(
            config.db, code_sources, "ServiceRequest"
        )
