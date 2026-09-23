import cumulus_library
from cumulus_library.studies.core.core_templates import core_templates
from cumulus_library.template_sql import sql_utils

expected_table_cols = {
    "medicationdispense": {
        "id": [],
        "dosageInstruction": {
            "text": {},
            "route": ["text"],
            "timing": {
                "code": ["text"],
                "repeat": {
                    "count": {},
                    "countMax": {},
                    "duration": {},
                    "durationMax": {},
                    "durationUnit": {},
                    "frequency": {},
                    "frequencyMax": {},
                    "period": {},
                    "periodMax": {},
                    "periodUnit": {},
                    "offset": {},
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


class MedicationDispenseDosageInstructionBuilder(cumulus_library.BaseTableBuilder):
    display_text = "Creating MedicationDispense dosage instruction table..."

    def prepare_queries(
        self,
        *args,
        config: cumulus_library.StudyConfig,
        **kwargs,
    ) -> None:
        validated_schema = sql_utils.validate_schema(config.db, expected_table_cols)
        self.queries += [
            core_templates.get_core_template(
                "medicationdispense_dosageinstruction", validated_schema
            ),
        ]
