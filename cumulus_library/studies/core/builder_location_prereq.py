import cumulus_library
from cumulus_library.template_sql import sql_utils


class CoreLocationBuilder(cumulus_library.BaseTableBuilder):
    display_text = "Creating Location tables..."

    def prepare_queries(self, *args, config: cumulus_library.StudyConfig, **kwargs):
        code_sources = [
            sql_utils.CodeableConceptConfig(
                source_table="location",
                column_hierarchy=[("type", list)],
                target_table="core__location_dn_type",
            ),
        ]
        self.queries += sql_utils.denormalize_complex_objects(config.db, code_sources, "Location")
