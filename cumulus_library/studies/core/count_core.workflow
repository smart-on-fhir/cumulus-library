config_type = "counts"

[tables.count_allergyintolerance_month]
source_table = "core__allergyintolerance"
description = """A general count of patient allergic reactions by month.

This table provides a summary snapshot of all allergic reactions for the entire patient population
that have been loaded into a database for use by the Cumulus ecosystem. It bins by intolerance category,
intolerance code, the reaction manifestation, and the month the reaction was observed in. 
It is primarily intended as a validation tool to ensure that data has been successfully extracted 
from a source system via the FHIR data format.
"""
primary_id = "patient_ref"
table_cols = [
    ["category", "varchar"],
    ["recordedDate_month", "date"],
    ["code_display", "varchar"],
    ["reaction_manifestation_display", "varchar"],
]

[tables.count_condition_month]
source_table = "core__condition"
description = """A general count of observed conditions by month.

This table provides a summary snapshot of all conditions observed in the entire patient population
that have been loaded into a database for use by the Cumulus ecosystem. It bins by condition category,
condition code, and the month the condition was observed in. It is primarily intended as a
validation tool to ensure that data has been successfully extracted from a source system via 
the FHIR data format.
"""
table_cols = [
    ["category_code", "varchar"],
    ["recordedDate_month", "date"],
    ["code_display", "varchar"],
    ["code", "varchar"],
]
secondary_id = "encounter_ref"

[tables.count_diagnosticreport_month]
source_table = "core__diagnosticreport"
description = """A general count of patient's diagnostic reports by month.

This table provides a summary snapshot of all diagnostic reports for the entire patient population
that have been loaded into a database for use by the Cumulus ecosystem. It bins by diagnostic category,
diagnostic code, and the month the diagnostic was performed in. It is primarily intended as a
validation tool to ensure that data has been successfully extracted from a source system via 
the FHIR data format.
"""
table_cols = [
    ["category_display", "varchar"],
    ["code_display", "varchar"],
    # Issued is not the _preferred_ time to pull, since it is an administrative time,
    # not a clinical one. But the clinical dates are annoyingly spread across three
    # fields: effectiveDateTime, effectivePeriod.start, and effectivePeriod.end.
    # So rather than do some fancy collation, just use issued. These core counts are
    # just a rough idea of the data, not a polished final product.
    ["issued_month", "date"],
]

[tables.count_documentreference_month]
source_table = "core__documentreference"
description = """A general count of documents related to a patient by month.

This table provides a summary snapshot of all documents for the entire patient population
that have been loaded into a database for use by the Cumulus ecosystem. It bins by document type
and the month the document was written in. It includes the related encounter class by joining
with the associated enounter resource, and filters all documents that are not in a current state,
or in a final or amended document state. It is primarily intended as a validation
tool to ensure that data has been successfully extracted from a source system via the FHIR
data format.
"""
table_cols = [
    ["type_display", "varchar"],
    ["author_month", "date"],
]
secondary_table = "core__encounter"
secondary_id = "documentreference_ref"
alt_secondary_join_id = "encounter_ref"
secondary_cols = ["class_display"]
filter_cols = [
    { name = "status", values = ["current"], include_nulls = false },
    { name = "docStatus", values = ["final", "amended"], include_nulls = true },
]

[tables.count_encounter_month]
description = """A general count of encounters by month.

This table provides a summary snapshot of all encounters for the entire patient population
that have been loaded into a database for use by the Cumulus ecosystem. It bins by encounter class,
age at visit, gender, the CDC's Race & Ethnicity valueset, and the month of the encounter. 
It filters out all encounters that are not in a finished state. It is primarily intended as a validation
tool to ensure that data has been successfully extracted from a source system via the FHIR
data format.
"""
source_table = "core__encounter"
table_cols = [
    "period_start_month",
    "class_display",
    "age_at_visit",
    "gender",
    "race_display",
    "ethnicity_display",
]
secondary_id = "encounter_ref"
filter_cols = [
    { name = "status", values = ["finished"], include_nulls = false }
]

[tables.count_encounter_all_types]
description = """A general count of encounter states.

This table provides a summary snapshot of all encounters for the entire patient population
that have been loaded into a database for use by the Cumulus ecosystem. It bins by encounter class,
encounter type, service type, and encounter priority. It is primarily intended as a validation
tool to ensure that data has been successfully extracted from a source system via the FHIR
data format.
"""
source_table = "core__encounter"
table_cols = [
    "class_display",
    "type_display",
    "serviceType_display",
    "priority_display",
]
secondary_id = "encounter_ref"

[tables.count_encounter_all_types_month]
description = """A general count of encounter states by month.

This table provides a summary snapshot of all encounter states for the entire patient population
that have been loaded into a database for use by the Cumulus ecosystem. It bins by encounter class,
encounter type, service type, encounter priority, and the month of the encounter. It is primarily intended as a validation
tool to ensure that data has been successfully extracted from a source system via the FHIR
data format.
"""
source_table = "core__encounter"
table_cols = [
    "class_display",
    "type_display",
    "serviceType_display",
    "priority_display",
    "period_start_month"
]
secondary_id = "encounter_ref"

[tables.count_encounter_type_month]
description = """A general count of encounter types by month.

This table provides a summary snapshot of all encounter types for the entire patient population
that have been loaded into a database for use by the Cumulus ecosystem. It bins by encounter class,
encounter type, and the month of the encounter. It is primarily intended as a validation
tool to ensure that data has been successfully extracted from a source system via the FHIR
data format.
"""
source_table = "core__encounter"
table_cols = [
    "class_display",
    "type_display",
    "period_start_month"
]
secondary_id = "encounter_ref"

[tables.count_encounter_priority_month]
description = """A general count of encounter priorities by month.

This table provides a summary snapshot of all encounter priorities for the entire patient population
that have been loaded into a database for use by the Cumulus ecosystem. It bins by encounter class,
encounter priority, and the month of the encounter. It is primarily intended as a validation
tool to ensure that data has been successfully extracted from a source system via the FHIR
data format.
"""
source_table = "core__encounter"
table_cols = [
    "class_display",
    "priority_display",
    "period_start_month"
]
secondary_id = "encounter_ref"

[tables.count_encounter_service_month]
description = """A general count of encounter service types by month.

This table provides a summary snapshot of all the enounters for the entire patient population
that have been loaded into a database for use by the Cumulus ecosystem. It bins by value,
observation code, and the date of the lab. It is primarily intended as a validation
tool to ensure that data has been successfully extracted from a source system via the FHIR
data format.
"""
source_table = "core__encounter"
table_cols = [
    "class_display",
    "serviceType_display",
    "period_start_month"
]
secondary_id = "encounter_ref"

[tables.count_medicationrequest_month]
description = """A general count of patients with medication requests by month.

This table provides a summary snapshot of all the medication requests for the entire patient population
that have been loaded into a database for use by the Cumulus ecosystem. It bins by medication name,
request status, intent of medication, and the date of the request authoring. It is primarily intended as a validation
tool to ensure that data has been successfully extracted from a source system via the FHIR
data format.
"""
source_table = "core__medicationrequest"
table_cols = [
    "status",
    "intent",
    "authoredon_month",
    "medication_display"
]

[tables.count_observation_lab_month]
source_table = "core__observation_lab"
description = """A general count of patient's lab observations by month.

This table provides a summary snapshot of all the labs for the entire patient population
that have been loaded into a database for use by the Cumulus ecosystem. It bins by value,
observation code, and the date of the lab. It also joins the encounter class from the associated
ecnounter. It is primarily intended as a validation tool to ensure that data has been 
successfully extracted from a source system via the FHIR data format.
"""
table_cols = [
    "effectiveDateTime_month",
    "observation_code",
    "valueCodeableConcept_display",
]
secondary_table = "core__encounter"
secondary_id = "observation_ref"
alt_secondary_join_id = "encounter_ref"
secondary_cols = ['class_display']
filter_cols = [ {name= "status", values = ["final", "amended"], include_nulls = false} ]

[tables.count_patient]
description = """A general patient population count.

This table provides a summary snapshot of the entire patient population that has been
loaded into a database for use by the Cumulus ecosystem. It provides binning based on
gender and the CDC's Race & Ethnicity valueset. It is primarily intended as a validation
tool to ensure that data has been successfully extracted from a source system via the FHIR
data format.
"""
source_table = "core__patient"
table_cols = [
    "gender",
    "race_display",
    "ethnicity_display"
]

[tables.count_procedure_month]
source_table = "core__procedure"
description = """A count of patient's general procedures by month.

This table provides a summary snapshot of all the procedures for the entire patient population
that have been loaded into a database for use by the Cumulus ecosystem. It bins by category,
procedure code, and the date of procedure. It is primarily intended as a validation
tool to ensure that data has been successfully extracted from a source system via the FHIR
data format.
"""
table_cols = [
    ["category_display", "varchar"],
    ["code_display", "varchar"],
    # The performed date is annoyingly spread across three fields: performedDateTime,
    # performedPeriod.start, and performedPeriod.end.
    # Rather than do some fancy collation, we just use performedDateTime.
    # It's the only "must support" performed field, and period seems less common.
    # These core counts are just a rough idea of the data, not a polished final product.
    ["performedDateTime_month", "date"],
]

[tables.count_servicerequest_month]
source_table = "core__servicerequest"
description = """A count of patient's general service requests by month.

This table provides a summary snapshot of all the service requests for the entire patient population
that have been loaded into a database for use by the Cumulus ecosystem. It bins by category,
service request code, and the author date. It is primarily intended as a validation
tool to ensure that data has been successfully extracted from a source system via the FHIR
data format.
"""
table_cols = [
    ["category_display", "varchar"],
    ["code_display", "varchar"],
    ["authoredOn_month", "date"],
]