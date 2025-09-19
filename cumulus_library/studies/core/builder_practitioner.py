import cumulus_library
from cumulus_library.studies.core.core_templates import core_templates
from cumulus_library.template_sql import sql_utils

expected_table_cols = {
    "practitioner": {
        "id": [],
        "active": [],
    }
}


class CorePractitionerBuilder(cumulus_library.BaseTableBuilder):
    display_text = "Creating Practitioner tables..."

    def prepare_queries(self, *args, config: cumulus_library.StudyConfig, **kwargs):
        code_sources = [
            sql_utils.CodeableConceptConfig(
                source_table="practitioner",
                column_hierarchy=[("qualification", list), ("code", dict)],
                target_table="core__practitioner_dn_qualification_code",
                expected={"code": sql_utils.CODEABLE_CONCEPT},
            ),
        ]
        self.queries += sql_utils.denormalize_complex_objects(config.db, code_sources)
        validated_schema = sql_utils.validate_schema(config.db, expected_table_cols)
        self.queries.append(core_templates.get_core_template("practitioner", validated_schema))
