import cumulus_library
from cumulus_library.studies.core.core_templates import core_templates
from cumulus_library.template_sql import sql_utils

expected_table_cols = {
    "documentreference": {
        "id": [],
        "status": [],
        "date": [],
        "docStatus": [],
        "subject": sql_utils.REFERENCE,
        "context": {"encounter": sql_utils.REFERENCE, "period": ["start"]},
        "content": {
            "attachment": {
                "contentType": [],
                "data": [],
                "_data": {"extension": ["url", "valueCode"]},
            },
        },
    }
}


class CoreDocumentreferenceBuilder(cumulus_library.BaseTableBuilder):
    display_text = "Creating DocumentReference tables..."

    def prepare_queries(
        self,
        *args,
        config: cumulus_library.StudyConfig,
        **kwargs,
    ):
        validated_schema = sql_utils.validate_schema(config.db, expected_table_cols)
        self.queries.append(core_templates.get_core_template("documentreference", validated_schema))
