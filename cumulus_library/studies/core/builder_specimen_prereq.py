import cumulus_library
from cumulus_library.template_sql import sql_utils


class CoreSpecimenBuilder(cumulus_library.BaseTableBuilder):
    display_text = "Creating Specimen tables..."

    def prepare_queries(self, *args, config: cumulus_library.StudyConfig, **kwargs):
        code_sources = [
            sql_utils.CodeableConceptConfig(
                source_table="specimen",
                column_hierarchy=[("type", dict)],
                target_table="core__specimen_dn_type",
            ),
        ]
        self.queries += sql_utils.denormalize_complex_objects(config.db, code_sources, "Specimen")
