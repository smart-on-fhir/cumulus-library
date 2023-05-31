""" Module for directly loading ICD bsvs into athena tables """
import csv

from rich.progress import Progress

from cumulus_library.base_runner import BaseRunner
from cumulus_library.helper import query_console_output
from cumulus_library.template_sql.templates import (
    get_extension_denormalize_query,
    ExtensionConfig,
)


class PatientExtensionRunner(BaseRunner):
    def run_executor(self, cursor: object, schema: str, verbose: bool):
        """input point from make.execute_sql_template.

        :param cursor: A database cursor object
        :param schema: the schema/db name, matching the cursor
        :param verbose: if true, outputs raw query, else displays progress bar
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
        configs = []
        for extension in extension_types:
            configs.append(
                ExtensionConfig(
                    "patient",
                    "id",
                    f"core__patient_ext_{extension['name']}",
                    extension["name"],
                    extension["fhirpath"],
                    ["ombCategory", "detailed", "text"],
                )
            )

        with Progress(disable=verbose) as progress:
            task = progress.add_task(
                f"Extracting core extensions from patients...",
                total=len(configs),
                visible=not verbose,
            )
            self.build_patient_extensions(
                self, cursor, schema, verbose, progress, task, configs
            )

    @staticmethod
    def build_patient_extensions(
        self, cursor, schema, verbose, progress, task, configs
    ):
        """Constructs queries and posts to athena."""
        for config in configs:
            extension_query = get_extension_denormalize_query(config)
            cursor.execute(extension_query)
            query_console_output(verbose, extension_query, progress, task)
