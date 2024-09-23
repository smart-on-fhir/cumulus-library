"""Builder for generating subsets of RxNorm data from a given valueset"""

import pathlib

from cumulus_library import BaseTableBuilder, base_utils, study_manifest
from cumulus_library.builders.valueset import umls, valueset_utils
from cumulus_library.template_sql import base_templates


class RxNormValuesetBuilder(BaseTableBuilder):
    display_text = "Creating RxNorm subsets from valueset definitions..."
    base_path = pathlib.Path(__file__).resolve().parent

    def prepare_queries(
        self,
        *args,
        config: base_utils.StudyConfig,
        manifest: study_manifest.StudyManifest,
        valueset_config: valueset_utils.ValuesetConfig,
        **kwargs,
    ):
        study_prefix = f"{manifest.get_study_prefix()}__"
        table_prefix = ""
        if valueset_config.table_prefix:
            table_prefix = f"{valueset_config.table_prefix}_"

        def get_create_view_filter_by(
            a_table: str,
            *,
            columns: list,
            b_table: str | None = None,
            a_join_col: str | None = None,
            b_join_col: str | None = None,
            a_schema: str | None = None,
            view_name: str | None = None,
            join_clauses: list | None = None,
            column_aliases: dict | None = None,
        ):
            a_schema = a_schema or "rxnorm."
            a_join_col = a_join_col or "a.rxcui"
            b_join_col = b_join_col or "b.rxcui"
            b_table = b_table or f"{study_prefix}{table_prefix}valuesets"
            join_clauses = join_clauses or [f"{a_join_col} = {b_join_col}"]
            view_name = view_name or f"{study_prefix}{table_prefix}{a_table}"

            return base_templates.get_create_view_from_tables(
                view_name=view_name,
                tables=[f"{a_schema}{a_table}", b_table],
                table_aliases=["a", "b"],
                join_clauses=join_clauses,
                columns=columns,
                column_aliases=column_aliases,
                distinct=True,
            )

        if valueset_config.keyword_file is not None:
            with open(manifest._study_path / valueset_config.keyword_file) as f:
                keywords = [row.rstrip() for row in f.readlines()]

            self.queries.append(
                base_templates.get_base_template(
                    "create_keyword_annotated_table",
                    self.base_path / "template_sql",
                    keywords=keywords,
                    source_table="rxnorm.rxnconso",
                    table_name=f"{study_prefix}{table_prefix}all_rxnconso_keywords",
                )
            )
        umls.generate_umls_tables(config, manifest, valueset_config)
        self.queries.append(
            get_create_view_filter_by(
                "rxnconso",
                b_table=f"{study_prefix}{table_prefix}vsac_valuesets",
                columns=["a.rxcui", "a.str", "a.tty", "a.sab", "a.code", "b.steward"],
                view_name=f"{study_prefix}{table_prefix}vsac_valuesets_hydrated",
            )
        )
        self.queries.append(
            base_templates.get_create_table_from_union(
                table_name=f"{study_prefix}{table_prefix}valuesets",
                tables=[
                    f"{study_prefix}{table_prefix}umls_valuesets",
                    f"{study_prefix}{table_prefix}vsac_valuesets_hydrated",
                ],
                columns=["rxcui", "str", "tty", "sab", "code", "steward"],
            )
        )
        self.queries.append(
            get_create_view_filter_by(
                "rxnconso",
                columns=["a.rxcui", "a.str", "a.tty", "a.sab", "a.code", "b.steward"],
            )
        )
        if valueset_config.keyword_file is not None:
            self.queries.append(
                base_templates.get_base_template(
                    "create_keyword_annotated_table",
                    self.base_path / "template_sql",
                    keywords=keywords,
                    source_table=f"{study_prefix}{table_prefix}rxnconso",
                    table_name=f"{study_prefix}{table_prefix}rxnconso_keywords",
                )
            )
        self.queries.append(
            get_create_view_filter_by(
                "rxnsty",
                columns=["a.rxcui", "a.tui", "a.stn", "a.sty", "a.atui", "a.cvf"],
            )
        )
        self.queries.append(
            get_create_view_filter_by(
                "rxnrel",
                columns=[
                    "a.rxcui1",
                    "a.rxaui1",
                    "a.stype1",
                    "a.rel",
                    "a.rxcui2",
                    "a.rxaui2",
                    "a.stype2",
                    "a.rela",
                    "a.rui",
                    "a.srui",
                    "a.sab",
                    "a.sl",
                    "a.rg",
                    "b.steward",
                ],
                a_join_col="a.rxcui1",
            )
        )
        self.queries.append(
            get_create_view_filter_by(
                f"{table_prefix}rxnconso_keywords",
                view_name=f"{study_prefix}{table_prefix}rela",
                a_schema=f"{study_prefix}",
                b_table=f"{study_prefix}{table_prefix}rxnrel",
                columns=[
                    "a.rxcui",
                    "a.str",
                    "a.tty",
                    "a.sab",
                    "b.rxcui2",
                    "b.rel",
                    "b.rela",
                    "b.rui",
                    "b.steward",
                ],
                b_join_col="b.rxcui1",
            )
        )
