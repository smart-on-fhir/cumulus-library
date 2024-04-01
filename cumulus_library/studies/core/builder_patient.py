""" Module for extracting US core extensions from patient records"""

from cumulus_library import databases
from cumulus_library.base_table_builder import BaseTableBuilder
from cumulus_library.studies.core.core_templates import core_templates
from cumulus_library.template_sql import base_templates, sql_utils

expected_table_cols = {
    "patient": {
        "id": [],
        "gender": [],
        "address": [],
        "birthDate": [],
    }
}


class PatientBuilder(BaseTableBuilder):
    display_text = "Creating Patient tables..."

    def prepare_queries(
        self,
        cursor: object,
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
            config = sql_utils.ExtensionConfig(
                source_table="patient",
                source_id="id",
                target_table=f"core__patient_ext_{extension['name']}",
                target_col_prefix=extension["name"],
                fhir_extension=extension["fhirpath"],
                ext_systems=["ombCategory", "detailed", "text"],
                is_array=True,
            )
            self.queries.append(base_templates.get_extension_denormalize_query(config))
        validated_schema = core_templates.validate_schema(
            cursor, schema, expected_table_cols, parser
        )
        self.queries.append(
            core_templates.get_core_template("patient", validated_schema)
        )
