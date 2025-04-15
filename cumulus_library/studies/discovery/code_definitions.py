# A collection of codes & codeableConcepts to extract available codes from.

from cumulus_library.template_sql import sql_utils

code_list = [
    # AllergyIntolerance
    {
        "table_name": "allergyintolerance",
        "column_hierarchy": [("clinicalstatus", dict), ("coding", list)],
    },
    {
        "table_name": "allergyintolerance",
        "column_hierarchy": [("verificationstatus", dict), ("coding", list)],
    },
    {
        "table_name": "allergyintolerance",
        "column_hierarchy": [("code", dict), ("coding", list)],
    },
    {
        "table_name": "allergyintolerance",
        "column_hierarchy": [("reaction", list), ("substance", dict), ("coding", list)],
    },
    {
        "table_name": "allergyintolerance",
        "column_hierarchy": [("reaction", list), ("manifestation", list), ("coding", list)],
    },
    {
        "table_name": "allergyintolerance",
        "column_hierarchy": [("reaction", list), ("exposureroute", dict), ("coding", list)],
    },
    # Condition
    {
        "table_name": "condition",
        "column_hierarchy": [("category", list), ("coding", list)],
    },
    {
        "table_name": "condition",
        "column_hierarchy": [("code", dict), ("coding", list)],
    },
    # DiagnosticReport
    {
        "table_name": "diagnosticreport",
        "column_hierarchy": [("category", list), ("coding", list)],
    },
    {
        "table_name": "diagnosticreport",
        "column_hierarchy": [("code", dict), ("coding", list)],
    },
    {
        "table_name": "diagnosticreport",
        "column_hierarchy": [("conclusioncode", list), ("coding", list)],
    },
    # DocumentReference
    {
        "table_name": "documentreference",
        "column_hierarchy": [("type", dict), ("coding", list)],
    },
    {
        "table_name": "documentreference",
        "column_hierarchy": [("category", list), ("coding", list)],
    },
    # Encounter
    {
        "table_name": "encounter",
        "column_hierarchy": [("class", dict)],
        "expected": sql_utils.CODING,
    },
    {
        "table_name": "encounter",
        "column_hierarchy": [("type", list), ("coding", list)],
    },
    {
        "table_name": "encounter",
        "column_hierarchy": [("servicetype", dict), ("coding", list)],
    },
    {
        "table_name": "encounter",
        "column_hierarchy": [("priority", dict), ("coding", list)],
    },
    {
        "table_name": "encounter",
        "column_hierarchy": [("reasoncode", list), ("coding", list)],
    },
    {
        "table_name": "encounter",
        "column_hierarchy": [
            ("hospitalization", dict),
            ("dischargedisposition", dict),
            ("coding", list),
        ],
        "expected": {"dischargedisposition": sql_utils.CODEABLE_CONCEPT},
    },
    # Medication
    {
        "table_name": "medication",
        "column_hierarchy": [("codecodeableconcept", dict), ("coding", list)],
    },
    {
        "table_name": "medication",
        "column_hierarchy": [("medicationcode", dict), ("coding", list)],
    },
    # MedicationRequest
    {
        "table_name": "medicationrequest",
        "column_hierarchy": [("medicationcodeableconcept", dict), ("coding", list)],
    },
    # Observation
    {
        "table_name": "observation",
        "column_hierarchy": [("category", list), ("coding", list)],
    },
    {
        "table_name": "observation",
        "column_hierarchy": [("code", dict), ("coding", list)],
    },
    {
        "table_name": "observation",
        "column_hierarchy": [("component", list), ("code", dict), ("coding", dict)],
        "expected": {"code": sql_utils.CODEABLE_CONCEPT},
    },
    {
        "table_name": "observation",
        "column_hierarchy": [("interpretation", list), ("coding", list)],
    },
    {
        "table_name": "observation",
        "column_hierarchy": [("valuecodeableconcept", dict), ("coding", list)],
    },
    {
        "table_name": "observation",
        "column_hierarchy": [("dataabsentreason", dict), ("coding", list)],
    },
    # Patient
    {
        "table_name": "patient",
        "column_hierarchy": [("maritalstatus", dict), ("coding", list)],
    },
    # Procedure
    {
        "table_name": "procedure",
        "column_hierarchy": [("statusreason", dict), ("coding", list)],
    },
    {
        "table_name": "procedure",
        "column_hierarchy": [("category", dict), ("coding", list)],
    },
    {
        "table_name": "procedure",
        "column_hierarchy": [("code", dict), ("coding", list)],
    },
    {
        "table_name": "procedure",
        "column_hierarchy": [("performer", list), ("function", dict), ("coding", list)],
    },
    {
        "table_name": "procedure",
        "column_hierarchy": [("reasoncode", list), ("coding", list)],
    },
    {
        "table_name": "procedure",
        "column_hierarchy": [("bodysite", list), ("coding", list)],
    },
    {
        "table_name": "procedure",
        "column_hierarchy": [("outcome", dict), ("coding", list)],
    },
    {
        "table_name": "procedure",
        "column_hierarchy": [("complication", list), ("coding", list)],
    },
    {
        "table_name": "procedure",
        "column_hierarchy": [("followup", list), ("coding", list)],
    },
    {
        "table_name": "procedure",
        "column_hierarchy": [("focalDevice", list), ("action", dict), ("coding", list)],
    },
    {
        "table_name": "procedure",
        "column_hierarchy": [("usedcode", list), ("coding", list)],
    },
]
