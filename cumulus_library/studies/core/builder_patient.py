""" Module for extracting US core extensions from patient records"""

from cumulus_library import databases
from cumulus_library.base_table_builder import BaseTableBuilder
from cumulus_library.studies.core.core_templates import core_templates
from cumulus_library.template_sql import base_templates, sql_utils

expected_table_cols = {
    "patient": {
        "id": [],
        "gender": [],
        "address": {"postalCode": {}},
        "birthDate": [],
    }
}


class PatientBuilder(BaseTableBuilder):
    display_text = "Creating Patient tables..."

    @staticmethod
    def make_extension_query(
        schema: str,
        cursor: databases.DatabaseCursor,
        parser: databases.DatabaseParser,
        name: str,
        url: str,
    ) -> str:
        has_extensions = sql_utils.is_field_present(
            schema=schema,
            cursor=cursor,
            parser=parser,
            source_table="patient",
            source_col="extension",
            expected={
                "extension": {
                    "url": {},
                    "valueCoding": sql_utils.CODING,
                },
                "url": {},
            },
        )
        if has_extensions:
            config = sql_utils.ExtensionConfig(
                source_table="patient",
                source_id="id",
                target_table=f"core__patient_ext_{name}",
                target_col_prefix=name,
                fhir_extension=url,
                ext_systems=["ombCategory", "detailed"],
                is_array=True,
            )
            return base_templates.get_extension_denormalize_query(config)
        else:
            return base_templates.get_ctas_empty_query(
                schema_name=schema,
                table_name=f"core__patient_ext_{name}",
                table_cols=["id", "system", f"{name}_code", f"{name}_display"],
            )

    def prepare_queries(
        self,
        cursor: databases.DatabaseCursor,
        schema: str,
        *args,
        parser: databases.DatabaseParser = None,
        **kwargs,
    ):
        """constructs queries related to patient extensions of interest

        :param cursor: A database cursor object
        :param schema: the schema/db name, matching the cursor
        """
        extension_types = [
            {
                "name": "race",
                "fhirpath": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-race",
            },
            {
                "name": "ethnicity",
                "fhirpath": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity",
            },
        ]
        for extension in extension_types:
            self.queries.append(
                self.make_extension_query(
                    schema, cursor, parser, extension["name"], extension["fhirpath"]
                )
            )

        validated_schema = sql_utils.validate_schema(
            cursor, schema, expected_table_cols, parser
        )
        self.queries.append(
            core_templates.get_core_template("patient", validated_schema)
        )
