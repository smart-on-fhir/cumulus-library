from cumulus_library import base_table_builder
from cumulus_library import databases
from cumulus_library.studies.core.core_templates import core_templates
from cumulus_library.template_sql import templates


expected_table_cols = {
    "documentreference": {
        "id": [],
        "type": [],
        "status": [],
        "docstatus": [],
        "context": [],
        "subject": ["reference"],
        "context": ["period", "start"],
    }
}


class CoreDocumentreferenceBuilder(base_table_builder.BaseTableBuilder):
    display_text = "Creating DocumentReference table..."

    def prepare_queries(
        self,
        cursor: object,
        schema: str,
        *args,
        parser: databases.DatabaseParser = None,
        **kwargs,
    ):
        validated_schema = core_templates.validate_schema(
            cursor, schema, expected_table_cols, parser
        )
        self.queries.append(
            core_templates.get_core_template("documentreference", validated_schema)
        )
