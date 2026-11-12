"""Class for generating IQR statistics from templates"""

import pathlib

from cumulus_library import BaseTableBuilder, base_utils, study_manifest
from cumulus_library.builders import iqr_utils
from cumulus_library.builders.statistics_templates import iqr_templates


class IqrBuilder(BaseTableBuilder):
    """Extends BaseTableBuilder for iqr-related use cases"""

    def __init__(
        self,
        *args,
        toml_config_path: pathlib.Path | None = None,
        **kwargs,
    ):
        super().__init__()
        if toml_config_path:
            self._workflow_config = iqr_utils.load_toml_config(toml_config_path)
        else:
            self._workflow_config = None

    def prepare_queries(
        self,
        config: base_utils.StudyConfig,
        manifest: study_manifest.StudyManifest,
        *args,
        **kwargs,
    ):
        """Renders out iqr queries"""
        if not self._workflow_config:
            return
        for distribution in self._workflow_config["distributions"]:
            self.queries.append(
                iqr_templates.get_iqr_query(
                    schema_name=config.schema,
                    table_name=f"{manifest.get_formatted_study_prefix()}{distribution['name']}",
                    source_table=distribution["source_table"],
                    column_name=distribution["target_col"],
                )
            )
