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
from cumulus_library.template_sql.utils import is_codeable_concept_populated


class MedicationBuilder(BaseTableBuilder):
    display_text = "Creating core medication table..."

    def _check_data_in_fields(self, cursor, schema: str):
        """Validates whether either observed medication source is present"""

        data_types = {
            "inline": False,
            "by_contained_ref": False,
            "by_external_ref": False,
        }

        table = "medicationrequest"
        base_col = "medicationcodeableconcept"

        with get_progress_bar(transient=True) as progress:
            task = progress.add_task(
                "Detecting available medication sources...",
                total=7,
            )

            # inline medications from FHIR medication
            data_types["inline"] = is_codeable_concept_populated(
                schema, table, base_col, cursor
            )
            if data_types["inline"]:
                query = get_column_datatype_query(schema, table, base_col)
                cursor.execute(query)
                progress.advance(task)
                if "userselected" not in str(cursor.fetchone()[0]):
                    has_userselected = False
                else:
                    has_userselected = True
            else:
                has_userselected = False
            # Validating presence of FHIR medication requests
            query = get_is_table_not_empty_query(
                "medicationrequest", "medicationreference"
            )
            cursor.execute(query)
            progress.advance(task)
            if cursor.fetchone() is None:
                return data_types, has_userselected
            query = get_column_datatype_query(
                schema, "medicationrequest", "medicationreference"
            )
            cursor.execute(query)
            progress.advance(task)
            if "reference" not in cursor.fetchone()[0]:
                return data_types, has_userselected
            query = get_is_table_not_empty_query(
                "medicationrequest", "medicationreference.reference"
            )
            cursor.execute(query)
            progress.advance(task)
            if cursor.fetchone() is None:
                return data_types, has_userselected

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

            return data_types, has_userselected

    def prepare_queries(self, cursor: object, schema: str) -> dict:
        """Constructs queries related to condition codeableConcept

        :param cursor: A database cursor object
        :param schema: the schema/db name, matching the cursor

        """
        medication_datasources, has_userselected = self._check_data_in_fields(
            cursor, schema
        )
        if (
            medication_datasources["inline"]
            or medication_datasources["by_contained_ref"]
            or medication_datasources["by_external_ref"]
        ):
            self.queries.append(
                get_core_medication_query(medication_datasources, has_userselected)
            )
        else:
            self.queries.append(
                get_ctas_empty_query(
                    schema,
                    "core__medication",
                    ["id", "resourcetype", "code", "ingredient"],
                )
            )
