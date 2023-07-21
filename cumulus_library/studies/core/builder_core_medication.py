""" Module for generating core medication table"""

from cumulus_library.base_table_builder import BaseTableBuilder
from cumulus_library.helper import get_progress_bar, query_console_output
from cumulus_library.template_sql.templates import (
    CodeableConceptConfig,
    get_core_medication_query,
    get_is_table_not_empty_query,
    get_column_datatype_query,
    get_ctas_empty_query,
)


class MedicationBuilder(BaseTableBuilder):
    display_text = "Creating core medication table..."

    def _check_data_in_fields(self, cursor, schema: str):
        """Validates whether either observed medication source is present"""

        data_types = {
            "by_contained_ref": False,
            "by_external_ref": False,
        }

        with get_progress_bar(transient=True) as progress:
            task = progress.add_task(
                "Detecting available medication sources...",
                total=5,
            )

            # Validating medication requests
            query = get_is_table_not_empty_query(
                "medicationrequest", "medicationreference"
            )
            cursor.execute(query)
            progress.advance(task)
            if cursor.fetchone() is None:
                return data_types
            query = get_column_datatype_query(
                schema, "medicationrequest", "medicationreference"
            )
            cursor.execute(query)
            progress.advance(task)
            if "reference" not in cursor.fetchone()[0]:
                return data_types
            query = get_is_table_not_empty_query(
                "medicationrequest", "medicationreference.reference"
            )
            cursor.execute(query)
            progress.advance(task)
            if cursor.fetchone() is None:
                return data_types

            # checking med ref contents for our two linkage cases
            query = get_is_table_not_empty_query(
                "medicationrequest",
                "medicationreference.reference",
                conditions=["medicationreference.reference LIKE '#%'"],
            )
            cursor.execute(query)
            progress.advance(task)
            if cursor.fetchone() is not None:
                data_types["by_contained_ref"] = True
            query = get_is_table_not_empty_query(
                "medicationrequest",
                "medicationreference.reference",
                conditions=["medicationreference.reference LIKE 'Medication/%'"],
            )
            cursor.execute(query)
            progress.advance(task)
            if cursor.fetchone() is not None:
                data_types["by_external_ref"] = True
            return data_types

    def prepare_queries(self, cursor: object, schema: str) -> dict:
        """Constructs queries related to condition codeableConcept

        :param cursor: A database cursor object
        :param schema: the schema/db name, matching the cursor

        """
        medication_datasources = self._check_data_in_fields(cursor, schema)
        if (
            medication_datasources["by_contained_ref"]
            or medication_datasources["by_external_ref"]
        ):
            self.queries.append(get_core_medication_query(medication_datasources))
        else:
            self.queries.append(
                get_ctas_empty_query(
                    schema,
                    "core__medications",
                    ["id", "resourcetype", "code", "ingredient"],
                )
            )
