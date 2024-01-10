-- http://hl7.org/fhir/r4/v3/ActEncounterCode/vs.html
CREATE TABLE core__fhir_act_encounter_code_v3 AS
SELECT
    t.code,
    t.display
FROM
    (
        VALUES
        (
            'AMB',
            'ambulatory'
        ),
        (
            'EMER',
            'emergency'
        ),
        (
            'FLD',
            'field'
        ),
        (
            'HH',
            'home health'
        ),
        (
            'IMP',
            'inpatient encounter'
        ),
        (
            'ACUTE',
            'inpatient acute'
        ),
        (
            'NONAC',
            'inpatient non-acute'
        ),
        (
            'OBSENC',
            'observation encounter'
        ),
        (
            'PRENC',
            'pre-admission'
        ),
        (
            'SS',
            'short stay'
        ),
        (
            'VR',
            'virtual'
        )
    )
        AS t (code, display);
