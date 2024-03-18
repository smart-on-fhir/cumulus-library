""" Module for generating core medication table"""

from cumulus_library import base_table_builder, base_utils
from cumulus_library.studies.core.core_templates import core_templates
from cumulus_library.template_sql import base_templates, sql_utils


class MedicationBuilder(base_table_builder.BaseTableBuilder):
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
        inline_col = "medicationcodeableconcept"
        with base_utils.get_progress_bar(transient=True) as progress:
            task = progress.add_task(
                "Detecting available medication sources...",
                total=3,
            )

            # inline medications from FHIR medication
            data_types["inline"] = sql_utils.is_field_populated(
                schema=schema,
                source_table=table,
                hierarchy=[(inline_col, dict), ("coding", list)],
                cursor=cursor,
            )
            if data_types["inline"]:
                query = base_templates.get_column_datatype_query(
                    schema, table, [inline_col]
                )
                cursor.execute(query)
                progress.advance(task)
                if "userselected" not in str(cursor.fetchone()[0]):
                    has_userselected = False
                else:
                    has_userselected = True
            else:
                has_userselected = False
            progress.advance(task)
            # Validating presence of FHIR medication requests
            if not sql_utils.is_field_populated(
                schema=schema,
                source_table=table,
                hierarchy=[("medicationreference", dict), ("reference", dict)],
                expected=["reference"],
                cursor=cursor,
            ):
                return data_types, has_userselected

            # checking med ref contents for our two linkage cases
            query = base_templates.get_is_table_not_empty_query(
                "medicationrequest",
                "medicationreference.reference",
                conditions=["medicationreference.reference LIKE '#%'"],
            )
            cursor.execute(query)
            progress.advance(task)
            if cursor.fetchone() is not None:
                data_types["by_contained_ref"] = True
            query = base_templates.get_is_table_not_empty_query(
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
                base_templates.get_ctas_empty_query(
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
