"""This builder primarily exists to make sure that the FHIR lookup
tables are created before other builders in the core study run, so that
they are available for joins."""

import pathlib

import sqlparse

from cumulus_library import base_table_builder


class CorePrereqTableBuilder(base_table_builder.BaseTableBuilder):
    display_text = "Creating core prerequisite tables..."

    def prepare_queries(self, cursor: object, schema: str, *args, **kwargs):
        dir_path = pathlib.Path(__file__).resolve().parents[0]
        prereq_sql = [
            "version.sql",
            "setup.sql",
            "fhir_lookup_tables.sql",
            "fhir_mapping_tables.sql",
        ]
        for sql_file in prereq_sql:
            with open(dir_path / sql_file) as file:
                queries = sqlparse.split(file.read())
                self.queries += queries
