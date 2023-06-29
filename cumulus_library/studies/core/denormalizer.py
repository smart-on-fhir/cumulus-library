""" Module for generating dynamic denormalized table from FHIR resources. 

If there is a nested resource we're extracting (i.e. a column with multiple
extensions, or a codingConcept field that could have more than one value),
we attempt to dynamically generate denormalized tables of the subset of
the data we're interested in, and we'll later left join these tables to
the parent when we're building the core resource tables.
"""
import csv

from cumulus_library.base_runner import BaseRunner
from cumulus_library.helper import get_progress_bar, query_console_output
from cumulus_library.template_sql.templates import (
    get_codeable_concept_denormalize_query,
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

        with get_progress_bar(disable=verbose) as progress:
            task = progress.add_task(
                f"Preprocessing core datasets...",
                total=len(configs) + 1,
                visible=not verbose,
            )
            self.build_patient_extensions(
                self, cursor, schema, verbose, progress, task, configs
            )
            self.build_condition_codes(self, cursor, schema, verbose, progress, task)

    @staticmethod
    def build_patient_extensions(
        self, cursor, schema, verbose, progress, task, configs
    ):
        """Constructs patient extension queries and posts to athena."""
        for config in configs:
            extension_query = get_extension_denormalize_query(config)
            cursor.execute(extension_query)
            query_console_output(verbose, extension_query, progress, task)

    @staticmethod
    def build_condition_codes(self, cursor, schema, verbose, progress, task):
        """Constructs patient extension queries and posts to athena."""
        codeable_concept_query = get_codeable_concept_denormalize_query(
            "condition",
            "code",
            "core__condition_codable_concepts",
            [
                "http://snomed.info/sct",
                "http://hl7.org/fhir/sid/icd-10-cm",
                "http://hl7.org/fhir/sid/icd-9-cm",
            ],
        )
        cursor.execute(codeable_concept_query)
        query_console_output(verbose, codeable_concept_query, progress, task)


if __name__ == "__main__":
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

    for config in configs:
        print(get_extension_denormalize_query(config))
