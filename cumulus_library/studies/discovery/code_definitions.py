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
    # EpisodeOfCare
    {
        "table_name": "episodeofcare",
        "column_hierarchy": [("type", list), ("coding", list)],
    },
    # Location
    {
        "table_name": "location",
        "column_hierarchy": [("type", list), ("coding", list)],
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
    # Organization
    {
        "table_name": "organization",
        "column_hierarchy": [("type", list), ("coding", list)],
    },
    # Patient
    {
        "table_name": "patient",
        "column_hierarchy": [("maritalstatus", dict), ("coding", list)],
    },
    # Practitioner
    {
        "table_name": "practitioner",
        "column_hierarchy": [("qualification", list), ("code", dict), ("coding", list)],
    },
    # PractitionerRole
    {
        "table_name": "practitionerrole",
        "column_hierarchy": [("code", list), ("coding", list)],
    },
    {
        "table_name": "practitionerrole",
        "column_hierarchy": [("specialty", list), ("coding", list)],
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
    # ServiceRequest
    {
        "table_name": "servicerequest",
        "column_hierarchy": [("category", list), ("coding", list)],
    },
    {
        "table_name": "servicerequest",
        "column_hierarchy": [("code", dict), ("coding", list)],
    },
    {
        "table_name": "servicerequest",
        "column_hierarchy": [("orderDetail", list), ("coding", list)],
    },
    {
        "table_name": "servicerequest",
        "column_hierarchy": [("asNeededCodeableConcept", dict), ("coding", list)],
    },
    {
        "table_name": "servicerequest",
        "column_hierarchy": [("performerType", dict), ("coding", list)],
    },
    {
        "table_name": "servicerequest",
        "column_hierarchy": [("locationCode", list), ("coding", list)],
    },
    {
        "table_name": "servicerequest",
        "column_hierarchy": [("reasonCode", list), ("coding", list)],
    },
    {
        "table_name": "servicerequest",
        "column_hierarchy": [("bodySite", list), ("coding", list)],
    },
    # Specimen
    {
        "table_name": "specimen",
        "column_hierarchy": [("type", dict), ("coding", list)],
    },
    {
        "table_name": "specimen",
        "column_hierarchy": [("collection", dict), ("method", dict), ("coding", list)],
    },
    {
        "table_name": "specimen",
        "column_hierarchy": [("collection", dict), ("bodySite", dict), ("coding", list)],
    },
    {
        "table_name": "specimen",
        "column_hierarchy": [
            ("collection", dict),
            ("fastingStatusCodeableConcept", dict),
            ("coding", list),
        ],
    },
    {
        "table_name": "specimen",
        "column_hierarchy": [("processing", list), ("procedure", dict), ("coding", list)],
    },
    {
        "table_name": "specimen",
        "column_hierarchy": [("container", list), ("type", dict), ("coding", list)],
    },
    {
        "table_name": "specimen",
        "column_hierarchy": [
            ("container", list),
            ("additiveCodeableConcept", dict),
            ("coding", list),
        ],
    },
    {
        "table_name": "specimen",
        "column_hierarchy": [("condition", list), ("coding", list)],
    },
]
