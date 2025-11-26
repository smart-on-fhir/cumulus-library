import cumulus_library
from cumulus_library.studies.core.core_templates import core_templates
from cumulus_library.template_sql import sql_utils

expected_table_cols = {
    "encounter": {
        "id": [],
        "status": [],
        "period": [
            "start",
            "end",
        ],
        "class": sql_utils.CODING,
        "subject": sql_utils.REFERENCE,
        "participant": {
            "individual": sql_utils.REFERENCE,
        },
        "serviceProvider": sql_utils.REFERENCE,
    },
    "etl__completion": {
        "group_name": [],
    },
    "etl__completion_encounters": {
        "group_name": [],
    },
}


class CoreEncounterBuilder(cumulus_library.BaseTableBuilder):
    display_text = "Creating Encounter tables..."

    def prepare_queries(
        self,
        *args,
        config: cumulus_library.StudyConfig,
        **kwargs,
    ):
        validated_schema = sql_utils.validate_schema(config.db, expected_table_cols)
        self.queries += [
            core_templates.get_core_template("encounter", validated_schema),
            core_templates.get_core_template("incomplete_encounter", validated_schema),
        ]
