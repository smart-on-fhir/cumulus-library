"""Builder for generating subsets of RxNorm data from a given valueset"""

import pathlib

from cumulus_library import BaseTableBuilder, base_utils, study_manifest
from cumulus_library.builders.valueset import valueset_utils
from cumulus_library.template_sql import base_templates


class AdditionalRulesBuilder(BaseTableBuilder):
    display_text = "Generating rulesets..."
    base_path = pathlib.Path(__file__).resolve().parent

    def prepare_queries(
        self,
        *args,
        config: base_utils.StudyConfig,
        manifest: study_manifest.StudyManifest,
        valueset_config: valueset_utils.ValuesetConfig,
        **kwargs,
    ):
        study_prefix = manifest.get_prefix_with_seperator()
        table_prefix = ""
        if valueset_config.table_prefix:
            table_prefix = f"{valueset_config.table_prefix}_"
        self.queries.append(
            base_templates.get_base_template(
                "create_search_rules_descriptions",
                self.base_path / "template_sql",
                study_prefix=study_prefix,
                table_prefix=table_prefix,
            )
        )
        self.queries.append(
            base_templates.get_create_table_from_tables(
                table_name=f"{study_prefix}{table_prefix}potential_rules",
                # From a domain logic perspective, the _rela table is
                # the leftmost table and we're annotating with the
                # data from rxnconso. Since rxnconso is much, much
                # larger, we're moving it to the left in the actual
                # constructed join for athena performance reasons
                tables=[
                    f"{study_prefix}{table_prefix}all_rxnconso_keywords",
                    f"{study_prefix}{table_prefix}rela",
                ],
                table_aliases=["r", "s"],
                columns=[
                    "s.rxcui",
                    "r.rxcui",
                    "s.tty",
                    "r.tty",
                    "s.rui",
                    "s.rel",
                    "s.rela",
                    "s.str",
                    "r.str",
                    "r.keyword",
                ],
                column_aliases={
                    "s.rxcui": "rxcui1",
                    "s.tty": "tty1",
                    "s.str": "str1",
                    "r.rxcui": "rxcui2",
                    "r.tty": "tty2",
                    "r.str": "str2",
                },
                join_clauses=[
                    "s.rxcui2 = r.rxcui",
                    (
                        "s.rxcui2 NOT IN (SELECT DISTINCT RXCUI FROM "  # noqa: S608
                        f"{study_prefix}{table_prefix}rxnconso_keywords)"
                    ),
                ],
            )
        )
        self.queries.append(
            base_templates.get_create_table_from_tables(
                table_name=f"{study_prefix}{table_prefix}included_rels",
                tables=[
                    f"{study_prefix}{table_prefix}potential_rules",
                    f"{study_prefix}{table_prefix}search_rules",
                ],
                table_aliases=["r", "e"],
                columns=[
                    "r.rxcui1",
                    "r.rxcui2",
                    "r.tty1",
                    "r.tty2",
                    "r.rui",
                    "r.rel",
                    "r.rela",
                    "r.str1",
                    "r.str2",
                    "r.keyword",
                ],
                join_clauses=[
                    "r.REL NOT IN ('RB', 'PAR')",
                    "e.include = TRUE",
                    "r.TTY1 = e.TTY1",
                    "r.TTY2 = e.TTY2",
                    "r.RELA = e.RELA",
                ],
            )
        )
        self.queries.append(
            base_templates.get_base_template(
                "create_included_keywords",
                self.base_path / "template_sql",
                study_prefix=study_prefix,
                table_prefix=table_prefix,
            )
        )
        self.queries.append(
            base_templates.get_create_table_from_union(
                table_name=f"{study_prefix}{table_prefix}combined_ruleset",
                tables=[
                    f"{study_prefix}{table_prefix}included_keywords",
                    f"{study_prefix}{table_prefix}included_rels",
                ],
                columns=[
                    "rxcui1",
                    "rxcui2",
                    "tty1",
                    "tty2",
                    "rui",
                    "rel",
                    "rela",
                    "str1",
                    "str2",
                    "keyword",
                ],
            )
        )
