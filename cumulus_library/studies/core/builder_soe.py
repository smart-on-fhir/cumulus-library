""" Module for generating condition codeableConcept table"""
import re
from cumulus_library.base_table_builder import BaseTableBuilder
from cumulus_library.helper import get_progress_bar, query_console_output
from cumulus_library.template_sql.templates import (
    get_column_datatype_query,
    get_object_denormalize_query,
)


class SOEBuilder(BaseTableBuilder):
    display_text = "Creating SoE support tables..."

    def prepare_queries(self, cursor: object, schema: str):
        """Constructs tables for SOE QA verification

        :param cursor: A database cursor object
        :param schema: the schema/db name, matching the cursor

        """
        table = "documentreference"
        column = "context"
        with get_progress_bar(transient=True) as progress:
            task = progress.add_task(
                "Detecting SOE...",
                total=1,
            )

        query = get_column_datatype_query(schema, table, column)
        cursor.execute(query)
        progress.advance(task)
        result = str(cursor.fetchone()[0])
        field_config = {
            "start": {
                "present": False,
                "type": "varchar",
            },
            "end": {"present": False, "type": "varchar"},
        }
        if "period row" in result:
            # The following will get all text between parenthesis following
            # period row - i.e. the schema of the period object
            field_schema_str = re.search(r"period row\(\s*([^\n\r]*)\),", result)[1]
            for key in field_config.keys():
                if f"{key} {field_config[key]['type']}" in field_schema_str:
                    field_config[key]["present"] = True

        self.queries.append(
            get_object_denormalize_query(
                schema,
                table,
                "id",
                f"{column}.period",
                field_config,
                "core__soe_doc_period",
            )
        )
