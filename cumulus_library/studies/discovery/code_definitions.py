# A collection of codes & codeableConcepts to extract available codes from.
# Two optional booleans are available for use:
#   - is_array: the field in question is an array of CodeableConcepts
#   - is_bare_coding: the field in question is a Coding not wrapped in concepts
#   - otherwise, it is assumed to be a 0..1 or 1..1 CodeableConcept
# TODO: if another state is needed, move to an Enum

code_list = [
    # Condition
    {"table_name": "condition", "column_name": "category", "is_array": True},
    {
        "table_name": "condition",
        "column_name": "code",
    },
    # DocumentReference
    {
        "table_name": "documentreference",
        "column_name": "type",
    },
    {"table_name": "documentreference", "column_name": "category", "is_array": True},
    # Encounter
    {
        "table_name": "encounter",
        "column_name": "class",
        "is_bare_coding": True,
    },
    {
        "table_name": "encounter",
        "column_name": "type",
        "is_array": True,
    },
    {
        "table_name": "encounter",
        "column_name": "servicetype",
    },
    {
        "table_name": "encounter",
        "column_name": "priority",
    },
    {"table_name": "encounter", "column_name": "reasoncode", "is_array": True},
    # Medication
    {
        "table_name": "medication",
        "column_name": "code",
    },
    # Observation
    {"table_name": "observation", "column_name": "category", "is_array": True},
    {
        "table_name": "observation",
        "column_name": "code",
    },
    # Patient
    {
        "table_name": "patient",
        "column_name": "maritalstatus",
    },
]
