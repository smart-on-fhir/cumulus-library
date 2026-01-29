"""Module for generating encounter codeableConcept table"""

import cumulus_library
from cumulus_library.studies.discovery import code_definitions
from cumulus_library.studies.discovery.discovery_templates import discovery_templates


class CodeUnionBuilder(cumulus_library.BaseTableBuilder):
    display_text = "Unioning code tables..."

    def prepare_queries(
        self,
        *args,
        config: cumulus_library.StudyConfig,
        **kwargs,
    ):
        """Constructs queries related to condition codeableConcept

        :param config: A study config object
        """
        query = discovery_templates.get_system_union(
            "discovery__code_sources", code_definitions.code_list
        )
        self.queries.append(query)
