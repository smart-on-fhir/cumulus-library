"""Creates medication lists from UMLS

We're leveraging the MRCONSO and MRREL tables to do some recursive lookups.
For a given medical language system, we'll build up some relationships,
and then use those as a seed for the next iteration, continuing until we've
identified all concepts related to a specific set of seed values.

Source terminology (see UMLS docs for more details):

https://www.ncbi.nlm.nih.gov/books/NBK9685/table/ch03.T.concept_names_and_sources_file_mr/
MRCONSO (source of concept information):
    SAB: Abbreviated source name of the concept. This is the name of the source
        medical vocabulary, i.e. SNOMED, ICD10, MEDRT, etc
    CUI: the unique identifier for a concept in UMLS
    TTY: Term type abbreviation, i.e. CD for clinical drug, DF for dose form, etc
    CODE: The primary ID for a concept in the source vocabulary
    STR: The display name of the concept, like a drug's name

https://www.ncbi.nlm.nih.gov/books/NBK9685/table/ch03.T.related_concepts_file_mrrel_rrf/
MRREL (source of mapping between concepts):
    SAB: Abbreviated source name of the relationship. Same as MRCONSO SAB
    CUI1: The UMLS ID of the concept you're mapping from
    CUI2: The UMLS ID of the concept you're mapping to
    REL: An abbreviation mapping to a relationship type, like RB (broader relationship),
        or RN (narrower relationship)
    RELA: The relationship between the concepts, like `tradename_of` or
        `has_ingredient`. May be null.
"""

import pathlib

from cumulus_library import base_utils, study_manifest
from cumulus_library.builders.valueset import valueset_utils
from cumulus_library.template_sql import base_templates


def generate_umls_tables(
    config: base_utils.StudyConfig,
    manifest: study_manifest.StudyManifest,
    valueset_config: valueset_utils.ValuesetConfig,
):
    base_path = pathlib.Path(__file__).resolve().parent
    study_prefix = manifest.get_prefix_with_seperator()
    cursor = config.db.cursor()
    table_prefix = ""
    if valueset_config.table_prefix:
        table_prefix = valueset_config.table_prefix + "_"
    create_table = True
    cursor.execute(f"DROP TABLE IF EXISTS {study_prefix}{table_prefix}valueset_rels")
    cursor.execute(f"DROP TABLE IF EXISTS {study_prefix}{table_prefix}valueset")
    for steward in valueset_config.umls_stewards:
        cursor = config.db.cursor()
        display_text = f"Discovering {steward} medications, pass "
        tier = 1
        with base_utils.get_progress_bar(disable=config.verbose) as progress:
            task = progress.add_task(
                display_text + str(tier), total=None, visible=not config.verbose
            )
            query = base_templates.get_template(
                "umls_iterate",
                base_path / "template_sql",
                steward=steward,
                prefix=study_prefix,
                table_prefix=table_prefix,
                sab=valueset_config.umls_stewards[steward]["sab"],
                search_terms=valueset_config.umls_stewards[steward]["search_terms"],
                tier=tier,
                create_table=create_table,
            )
            create_table = False
            cursor.execute(query)
            prev = 0
            current = cursor.execute(
                f"SELECT count(*) from {study_prefix}{table_prefix}umls_valuesets_rels"  # noqa: S608
            ).fetchone()[0]
            while current != prev:
                prev = current
                tier += 1
                progress.update(task, description=display_text + str(tier))
                query = base_templates.get_template(
                    "umls_iterate",
                    base_path / "template_sql",
                    steward=steward,
                    prefix=study_prefix,
                    table_prefix=table_prefix,
                    sab=valueset_config.umls_stewards[steward]["sab"],
                    search_terms=valueset_config.umls_stewards[steward]["search_terms"],
                    tier=tier,
                )
                cursor.execute(query)
                current = cursor.execute(
                    f"SELECT count(*) from {study_prefix}{table_prefix}umls_valuesets_rels"  # noqa: S608
                ).fetchone()[0]
    rels_cols = [
        "steward",
        "tier",
        "cui",
        "cui1",
        "rel",
        "rela",
        "cui2",
        "sab",
        "tty",
        "code",
        "str",
    ]
    if valueset_config.umls_stewards:
        query = base_templates.get_create_table_from_tables(
            table_name=f"{study_prefix}{table_prefix}umls_valuesets",
            tables=[
                f"{study_prefix}{table_prefix}umls_valuesets_rels",
                # TODO: allow this to target multiple codings for joins
                "rxnorm.RXNCONSO",
            ],
            columns=["r.rxcui"] + [f"v.{x}" for x in rels_cols],
            join_clauses=["v.code = r.code", "v.sab = r.sab"],
            table_aliases=["v", "r"],
            distinct=True,
        )
        cursor.execute(query)
    else:
        query = base_templates.get_ctas_empty_query(
            schema_name=config.schema,
            table_name=f"{study_prefix}{table_prefix}umls_valuesets",
            table_cols=["rxcui", *rels_cols],
        )
        cursor.execute(query)
