import cumulus_library
from cumulus_library.studies.core.core_templates import core_templates
from cumulus_library.template_sql import sql_utils

expected_table_cols = {
    "medicationrequest": {
        "id": [],
        "dosageInstruction": {
            "sequence": {},
            "text": {},
            "patientInstruction": {},
            "asNeededBoolean": {},
            "route": ["text"],
            "timing": {
                "code": ["text"],
                "repeat": {
                    "frequency": {},
                    "period": {},
                    "periodUnit": {},
                    "boundsPeriod": sql_utils.PERIOD,
                },
            },
            "doseAndRate": {
                "doseQuantity": ["value", "unit", "system", "code"],
                "doseRange": {
                    "low": ["value", "unit", "system", "code"],
                    "high": ["value", "unit", "system", "code"],
                },
            },
        },
    }
}


class MedicationRequestDosageInstructionBuilder(cumulus_library.BaseTableBuilder):
    display_text = "Creating MedicationRequest dosage instruction table..."

    def prepare_queries(
        self,
        *args,
        config: cumulus_library.StudyConfig,
        **kwargs,
    ) -> None:
        validated_schema = sql_utils.validate_schema(config.db, expected_table_cols)
        self.queries += [
            core_templates.get_core_template(
                "medicationrequest_dosageinstruction", validated_schema
            ),
        ]
