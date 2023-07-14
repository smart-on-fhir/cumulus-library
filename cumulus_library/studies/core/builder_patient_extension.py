""" Module for extracting US core extensions from patient records"""
from cumulus_library.base_table_builder import BaseTableBuilder
from cumulus_library.template_sql.templates import (
    get_extension_denormalize_query,
    ExtensionConfig,
)


class PatientExtensionBuilder(BaseTableBuilder):
    display_text = "Creating patient extension tables..."

    def prepare_queries(self, cursor: object, schema: str):
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
            config = ExtensionConfig(
                "patient",
                "id",
                f"core__patient_ext_{extension['name']}",
                extension["name"],
                extension["fhirpath"],
                ["ombCategory", "detailed", "text"],
            )
            self.queries.append(get_extension_denormalize_query(config))
