import cumulus_library
from cumulus_library.template_sql import sql_utils


class CoreDocumentreferenceBuilder(cumulus_library.BaseTableBuilder):
    display_text = "Creating DocumentReference tables..."

    def prepare_queries(
        self,
        *args,
        config: cumulus_library.StudyConfig,
        **kwargs,
    ):
        self.queries = sql_utils.denormalize_complex_objects(
            config.db,
            [
                sql_utils.CodeableConceptConfig(
                    source_table="documentreference",
                    source_id="id",
                    column_hierarchy=[("type", dict)],
                    target_table="core__documentreference_dn_type",
                ),
                sql_utils.CodeableConceptConfig(
                    source_table="documentreference",
                    source_id="id",
                    column_hierarchy=[("category", list)],
                    target_table="core__documentreference_dn_category",
                ),
                sql_utils.CodingConfig(
                    source_table="documentreference",
                    source_id="id",
                    column_hierarchy=[("content", list), ("format", dict)],
                    target_table="core__documentreference_dn_format",
                    expected={"format": sql_utils.CODING},
                ),
            ],
            "DocumentReference",
        )
