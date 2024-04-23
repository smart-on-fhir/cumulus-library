from cumulus_library import base_table_builder, databases
from cumulus_library.studies.core.core_templates import core_templates
from cumulus_library.template_sql import sql_utils

expected_table_cols = {
    "documentreference": {
        "id": [],
        "status": [],
        "date": [],
        "docStatus": [],
        "subject": sql_utils.REFERENCE,
        "context": {"encounter": sql_utils.REFERENCE, "period": ["start"]},
    }
}


class CoreDocumentreferenceBuilder(base_table_builder.BaseTableBuilder):
    display_text = "Creating DocumentReference tables..."

    def prepare_queries(
        self,
        cursor: object,
        schema: str,
        *args,
        parser: databases.DatabaseParser = None,
        **kwargs,
    ):
        self.queries = sql_utils.denormalize_complex_objects(
            schema,
            cursor,
            parser,
            [
                sql_utils.CodeableConceptConfig(
                    source_table="documentreference",
                    source_id="id",
                    column_hierarchy=[("type", dict)],
                    target_table="core__documentreference_dn_type",
                ),
                # TODO: The US core profile allows an extensible code for category, but
                # it's unclear what the possible extended codes would be. For the time
                # being, we select the spec's preferred code system (which is a set of
                # one value, 'clinical-note').
                # It may be worth confirming the values in this field with either the
                # quality or discovery studies on an ongoing basis to find other uses
                # for this field.
                sql_utils.CodeableConceptConfig(
                    source_table="documentreference",
                    source_id="id",
                    column_hierarchy=[("category", list)],
                    filter_priority=True,
                    target_table="core__documentreference_dn_category",
                    code_systems=[
                        "http://hl7.org/fhir/us/core/ValueSet/us-core-documentreference-category"
                    ],
                ),
                sql_utils.CodingConfig(
                    source_table="documentreference",
                    source_id="id",
                    column_hierarchy=[("content", list), ("format", dict)],
                    target_table="core__documentreference_dn_format",
                    expected={"format": sql_utils.CODING},
                ),
            ],
        )
        validated_schema = sql_utils.validate_schema(
            cursor, schema, expected_table_cols, parser
        )
        self.queries.append(
            core_templates.get_core_template("documentreference", validated_schema)
        )
