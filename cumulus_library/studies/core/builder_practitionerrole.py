import cumulus_library
from cumulus_library.studies.core.core_templates import core_templates
from cumulus_library.template_sql import sql_utils

expected_table_cols = {
    "practitionerrole": {
        "id": [],
        "active": [],
        "practitioner": sql_utils.REFERENCE,
        "organization": sql_utils.REFERENCE,
        "location": sql_utils.REFERENCE,
    }
}


class CorePractitionerRoleBuilder(cumulus_library.BaseTableBuilder):
    display_text = "Creating PractitionerRole tables..."

    def prepare_queries(self, *args, config: cumulus_library.StudyConfig, **kwargs):
        code_sources = [
            sql_utils.CodeableConceptConfig(
                source_table="practitionerrole",
                column_hierarchy=[("code", list)],
                target_table="core__practitionerrole_dn_code",
            ),
            sql_utils.CodeableConceptConfig(
                source_table="practitionerrole",
                column_hierarchy=[("specialty", list)],
                target_table="core__practitionerrole_dn_specialty",
            ),
        ]
        self.queries += sql_utils.denormalize_complex_objects(config.db, code_sources)
        validated_schema = sql_utils.validate_schema(config.db, expected_table_cols)
        self.queries.append(core_templates.get_core_template("practitionerrole", validated_schema))
