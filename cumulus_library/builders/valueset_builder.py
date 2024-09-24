import pathlib
import sys
import tomllib

from cumulus_library import BaseTableBuilder, base_utils, study_manifest
from cumulus_library.builders.valueset import (
    additional_rules_builder,
    rxnorm_valueset_builder,
    static_builder,
    valueset_utils,
)


class ValuesetBuilder(BaseTableBuilder):
    """TableBuilder for creating PSM tables"""

    display_text = "Building valueset tables..."

    def __init__(self, toml_config_path: str, data_path: pathlib.Path, **kwargs):
        """Loads PSM job details from a PSM configuration file"""
        super().__init__()
        # We're stashing the toml path for error reporting later
        self.toml_path = toml_config_path
        self.data_path = data_path
        try:
            with open(self.toml_path, "rb") as file:
                toml_config = tomllib.load(file)

        except OSError:
            sys.exit(f"Valueset configuration not found at {self.toml_path}")
        self.valueset_config = valueset_utils.ValuesetConfig(
            rules_file=toml_config.get("rules_file"),
            keyword_file=toml_config.get("keyword_file"),
            table_prefix=toml_config.get("target_table", ""),
            umls_stewards=toml_config.get("umls_stewards", {}),
            vsac_stewards=toml_config.get("vsac_stewards", {}),
        )

    def prepare_queries(
        self,
        *args,
        config: base_utils.StudyConfig,
        manifest: study_manifest.StudyManifest,
        **kwargs,
    ):
        self.queries = []
        s_builder = static_builder.StaticBuilder()
        s_builder.prepare_queries(
            config=config, manifest=manifest, valueset_config=self.valueset_config
        )
        self.queries += s_builder.queries
        rx_builder = rxnorm_valueset_builder.RxNormValuesetBuilder()
        rx_builder.prepare_queries(
            config=config, manifest=manifest, valueset_config=self.valueset_config
        )
        self.queries += rx_builder.queries
        r_builder = additional_rules_builder.AdditionalRulesBuilder()
        r_builder.prepare_queries(
            config=config, manifest=manifest, valueset_config=self.valueset_config
        )
        self.queries += r_builder.queries
