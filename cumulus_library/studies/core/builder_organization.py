import cumulus_library
from cumulus_library.studies.core.core_templates import core_templates
from cumulus_library.template_sql import sql_utils

expected_table_cols = {
    "organization": {
        "id": [],
        "active": [],
        "name": [],
        "partOf": sql_utils.REFERENCE,
    }
}


class CoreOrganizationBuilder(cumulus_library.BaseTableBuilder):
    display_text = "Creating Organization tables..."

    def prepare_queries(self, *args, config: cumulus_library.StudyConfig, **kwargs):
        validated_schema = sql_utils.validate_schema(config.db, expected_table_cols)
        self.queries.append(core_templates.get_core_template("organization", validated_schema))
