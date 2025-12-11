"""Module for generating encounter codeableConcept table"""

import cumulus_library
from cumulus_library.studies.discovery import code_definitions


class CleanupTmpBuilder(cumulus_library.BaseTableBuilder):
    display_text = "Selecting unique code systems..."

    def prepare_queries(
        self,
        *args,
        config: cumulus_library.StudyConfig,
        **kwargs,
    ):
        """Constructs queries related to condition codeableConcept

        :param config: A study config object
        """

        code_sources = []
        for code_definition in code_definitions.code_list:
            code_source = {}
            for key in code_definition.keys():
                code_source[key] = code_definition[key]
            code_sources.append(code_source)
        query = cumulus_library.get_template(
            "show_tables", schema_name=config.schema, prefix="discovery__tmp"
        )
        tmp_tables = config.db.connection.execute(query).fetchall()
        for table in tmp_tables:
            query = cumulus_library.get_template(
                "drop_view_table", view_or_table="table", view_or_table_name=table[0]
            )
            self.queries.append(query)
