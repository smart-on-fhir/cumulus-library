import cumulus_library
from cumulus_library.studies.core.core_templates import core_templates
from cumulus_library.template_sql import sql_utils

expected_table_cols = {
    "specimen": {
        "id": [],
        "status": [],
        "type": sql_utils.CODEABLE_CONCEPT,
        "subject": sql_utils.REFERENCE,
    }
}


class CoreSpecimenBuilder(cumulus_library.BaseTableBuilder):
    display_text = "Creating Specimen tables..."

    def prepare_queries(self, *args, config: cumulus_library.StudyConfig, **kwargs):
        validated_schema = sql_utils.validate_schema(config.db, expected_table_cols)
        self.queries.append(core_templates.get_core_template("specimen", validated_schema))
