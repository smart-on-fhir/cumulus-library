import cumulus_library
from cumulus_library.studies.core.core_templates import core_templates
from cumulus_library.template_sql import sql_utils

expected_table_cols = {
    "medicationdispense": {
        "id": [],
        "status": [],
        "subject": sql_utils.REFERENCE,
        # MedicationDispense uses 'context' rather than 'encounter' in R4
        "context": sql_utils.REFERENCE,
        "medicationReference": sql_utils.REFERENCE,
        # 0..*, there may be multiple orders, or none
        "authorizingPrescription": sql_utils.REFERENCE,
        "performer": {"actor": sql_utils.REFERENCE},
        "whenPrepared": [],
        "whenHandedOver": [],
        "quantity": ["value", "unit", "system", "code"],
        "daysSupply": ["value", "unit", "system", "code"],
    }
}


class MedicationDispenseBuilder(cumulus_library.BaseTableBuilder):
    display_text = "Creating MedicationDispense table..."

    def prepare_queries(
        self,
        *args,
        config: cumulus_library.StudyConfig,
        **kwargs,
    ) -> None:
        validated_schema = sql_utils.validate_schema(config.db, expected_table_cols)
        self.queries += [
            core_templates.get_core_template("medicationdispense", validated_schema),
            core_templates.get_core_template(
                "medicationdispense_authorizingprescription", validated_schema
            ),
            core_templates.get_core_template("medicationdispense_performer", validated_schema),
        ]
