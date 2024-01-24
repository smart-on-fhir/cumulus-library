""" Module for generating core medication table"""

from cumulus_library.base_table_builder import BaseTableBuilder
from cumulus_library.helper import get_progress_bar, query_console_output
from cumulus_library.template_sql import templates
from cumulus_library.template_sql.utils import is_codeable_concept_populated
from cumulus_library.studies.core.core_templates import core_templates


class MedicationBuilder(BaseTableBuilder):
    display_text = "Creating Medication table..."

    def _check_data_in_fields(self, cursor, schema: str):
        """Validates whether either observed medication source is present

        We opt to not use the core_templates.utils based version of
        checking for data fields, since Medication can come in from
        a few different sources - the format is unique to this FHIR
        resource.
        """

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
                query = templates.get_column_datatype_query(schema, table, [base_col])
                cursor.execute(query)
                progress.advance(task)
                if "userselected" not in str(cursor.fetchone()[0]):
                    has_userselected = False
                else:
                    has_userselected = True
            else:
                has_userselected = False
            # Validating presence of FHIR medication requests
            query = templates.get_is_table_not_empty_query(
                "medicationrequest", "medicationreference"
            )
            cursor.execute(query)
            progress.advance(task)
            if cursor.fetchone() is None:
                return data_types, has_userselected
            query = templates.get_column_datatype_query(
                schema, "medicationrequest", ["medicationreference"]
            )
            cursor.execute(query)
            progress.advance(task)
            if "reference" not in cursor.fetchone()[0]:
                return data_types, has_userselected
            query = templates.get_is_table_not_empty_query(
                "medicationrequest", "medicationreference.reference"
            )
            cursor.execute(query)
            progress.advance(task)
            if cursor.fetchone() is None:
                return data_types, has_userselected

            # checking med ref contents for our two linkage cases
            query = templates.get_is_table_not_empty_query(
                "medicationrequest",
                "medicationreference.reference",
                conditions=["medicationreference.reference LIKE '#%'"],
            )
            cursor.execute(query)
            progress.advance(task)
            if cursor.fetchone() is not None:
                data_types["by_contained_ref"] = True
            query = templates.get_is_table_not_empty_query(
                "medicationrequest",
                "medicationreference.reference",
                conditions=["medicationreference.reference LIKE 'Medication/%'"],
            )
            cursor.execute(query)
            progress.advance(task)
            if cursor.fetchone() is not None:
                data_types["by_external_ref"] = True

            return data_types, has_userselected

    def prepare_queries(self, cursor: object, schema: str, *args, **kwargs) -> dict:
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
                core_templates.get_core_template(
                    "medication",
                    config={
                        "medication_datasources": medication_datasources,
                        "has_userselected": has_userselected,
                    },
                )
            )
        else:
            self.queries.append(
                templates.get_ctas_empty_query(
                    schema,
                    "core__medication",
                    [
                        "id",
                        "encounter_ref",
                        "patient_ref",
                        "code",
                        "display",
                        "code_system",
                        "userselected",
                    ],
                )
            )
