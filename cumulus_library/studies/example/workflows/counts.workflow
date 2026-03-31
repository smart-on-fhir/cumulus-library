config_type = "counts"


[tables.count_bronchitis_encounter]
source_table = "example__bronchitis_encounter"
description = """Encounters of patients with a diagnosis of bronchitis"""
table_cols = [
    "period_start_month",
    "age_at_visit",
    "class_display",
    "type_display"
]
min_subject=2
secondary_id="encounter_ref"

[tables.count_bronchitis_medicationrequest]
source_table = "example__bronchitis_medicationrequest"
description = """Details about requests for medication for patients with bronchitis"""
table_cols = [
    "authoredon_month",
    "medication_code",
    "medication_display",
]
min_subject=2

[tables.count_bronchitis_patient]
source_table = "example__bronchitis_patient"
description = """Demographic details about patients with bronchitis"""
table_cols = [
    "gender",
    "birthdate",
    "race_display",
    "ethnicity_display"
]
min_subject=2

[tables.count_bronchitis_meds_by_patient]
source_table = "example__bronchitis_meds_by_patient"
description = """Details about medications for bronchitis with patient demographic details"""
table_cols = [
    "gender",
    "age_at_visit",
    "race_display",
    "medication_code",
]
min_subject=2

[tables.count_bronchitis_meds_by_patient.annotation]
field = "medication_code"
join_table = "example__med_delivery_mechanism"
join_field = "medication_code"
columns = [
    ["medication_display", "varchar"],
    ["delivery_mechanism", "varchar"]
]