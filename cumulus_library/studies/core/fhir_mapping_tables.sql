-- ############################################################
-- FHIR mapping of code systems to URIs
-- This includes both the expected URI, as well as several found URIs from
-- various source systems

CREATE TABLE core__fhir_mapping_code_system_uri AS SELECT * FROM
    (
        VALUES
        ('ICD10', 'http://hl7.org/fhir/sid/icd-10-cm'),
        ('ICD10', '2.16.840.1.113883.6.90'),
        ('ICD10', 'ICD10'),
        ('ICD10', 'ICD-10'),
        ('ICD10', 'ICD-10-CM'),
        ('ICD10', 'ICD10-CM'),

        ('ICD9', 'http://hl7.org/fhir/sid/icd-9-cm'),
        ('ICD9', '2.16.840.1.113883.6.103'),
        ('ICD9', 'ICD9'),
        ('ICD9', 'ICD-9'),
        ('ICD9', 'ICD-9-CM'),
        ('ICD9', 'ICD9-CM'),

        ('SNOMED', 'http://snomed.info/sct'),
        ('SNOMED', '2.16.840.1.113883.6.96'),
        ('SNOMED', 'SNOMEDCT'),
        ('SNOMED', 'SNOMEDCT_US'),
        ('SNOMED', 'SNOMED'),

        ('LOINC', 'http://loinc.org'),
        ('LOINC', '2.16.840.1.113883.6.1'),
        ('LOINC', 'LOINC'),
        ('LOINC', 'LNC'),

        ('RXNORM', 'http://www.nlm.nih.gov/research/umls/'),
        ('RXNORM', '2.16.840.1.113883.6.88'),
        ('RXNORM', 'RXNORM'),

        ('UMLS', 'http://www.nlm.nih.gov/research/umls/'),
        ('UMLS', 'UMLS'),

        ('CPT', 'http://www.ama-assn.org/go/cpt'),
        ('CPT', 'CPT')
    ) AS t (code_system, uri); --noqa: AL05

-- ############################################################
-- FHIR mapping of Resource names to expected URIs

CREATE TABLE core__fhir_mapping_resource_uri AS
SELECT * FROM
    (
        VALUES
        (
            'Patient',
            'http://hl7.org/fhir/us/core/StructureDefinition/us-core-patient'
        ),
        ('Gender', 'http://hl7.org/fhir/ValueSet/administrative-gender'),
        (
            'Race',
            'http://hl7.org/fhir/us/core/StructureDefinition/us-core-race'
        ),
        (
            'Ethnicity',
            'http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity'
        ),
        (
            'PostalCode',
            'http://hl7.org/fhir/datatypes-definitions.html#Address.postalCode'
        ),
        (
            'PatientClass',
            'http://terminology.hl7.org/CodeSystem/v2-0004'
        ),
        (
            'Encounter',
            'http://hl7.org/fhir/us/core/StructureDefinition/us-core-encounter'
        ),
        ('EncounterStatus', 'http://hl7.org/fhir/ValueSet/encounter-status'),
        ('EncounterType', 'http://hl7.org/fhir/ValueSet/encounter-type'),
        ('EncounterReason', 'http://hl7.org/fhir/ValueSet/encounter-reason'),
        ('EncounterCode', 'http://terminology.hl7.org/ValueSet/v3-ActEncounterCode'),
        ('EncounterPriority', 'http://terminology.hl7.org/CodeSystem/v3-ActPriority'),
        (
            'EncounterLocationStatus',
            'http://hl7.org/fhir/ValueSet/encounter-location-status'
        ),
        ('Period', 'http://hl7.org/fhir/datatypes.html#Period'),
        ('Coding', 'http://hl7.org/fhir/datatypes.html#Coding'),
        (
            'DocumentReference',
            'http://hl7.org/fhir/us/core/StructureDefinition/us-core-documentreference'
        ),
        ('DocumentType', 'http://hl7.org/fhir/ValueSet/c80-doc-typecodes'),
        ('Condition', 'http://hl7.org/fhir/condition-definitions.html'),
        ('ConditionCode', 'http://hl7.org/fhir/ValueSet/condition-code'),
        (
            'ConditionCategory',
            'http://hl7.org/fhir/ValueSet/condition-category'
        ),
        (
            'ObservationLab',
            'http://hl7.org/fhir/us/core/StructureDefinition/us-core-observation-lab'
        ),
        ('ObservationCode', 'http://hl7.org/fhir/ValueSet/observation-codes'),
        (
            'ObservationCategory',
            'http://hl7.org/fhir/ValueSet/observation-category'
        ),
        (
            'ObservationInterpretation',
            'http://hl7.org/fhir/ValueSet/observation-interpretation'
        ),
        (
            'ObservationValue',
            'http://hl7.org/fhir/observation-definitions.html#Observation.value_x_'
        ),
        ('VitalSign', 'http://hl7.org/fhir/observation-vitalsigns.html')
    ) AS t (resource, uri); --noqa: AL05

-- ############################################################
-- FHIR mapping of as found Encounter codes to the expected encounter code from
-- http://hl7.org/fhir/STU3/v3/ActEncounterCode/vs.html

CREATE TABLE core__fhir_mapping_expected_act_encounter_code_v3 AS
SELECT * FROM
    (
        VALUES
        ('AMB', 'AMB'),
        ('AMB', 'R'),
        ('AMB', 'O'),
        ('EMER', 'EMER'),
        ('EMER', 'E'),
        ('FLD', 'FLD'),
        ('HH', 'HH'),
        ('IMP', 'IMP'),
        ('ACUTE', 'ACUTE'),
        ('NONAC', 'NONAC'),
        ('PRENC', 'PRENC'),
        ('SS', 'SS'),
        ('VR', 'VR')
    ) AS t (expected, found)
