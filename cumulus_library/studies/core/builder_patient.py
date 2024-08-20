"""Module for extracting US core extensions from patient records"""

import cumulus_library
from cumulus_library import databases
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


class PatientBuilder(cumulus_library.BaseTableBuilder):
    display_text = "Creating Patient tables..."

    @staticmethod
    def make_extension_query(
        database: databases.DatabaseBackend,
        name: str,
        url: str,
    ) -> str:
        has_extensions = sql_utils.is_field_present(
            database=database,
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
                schema_name=database.schema_name,
                table_name=f"core__patient_ext_{name}",
                table_cols=["id", "system", f"{name}_code", f"{name}_display"],
            )

    def prepare_queries(
        self,
        *args,
        config: cumulus_library.StudyConfig,
        **kwargs,
    ):
        """constructs queries related to patient extensions of interest

        :param config: A study config object
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
                self.make_extension_query(config.db, extension["name"], extension["fhirpath"])
            )

        validated_schema = sql_utils.validate_schema(config.db, expected_table_cols)
        self.queries.append(core_templates.get_core_template("patient", validated_schema))
