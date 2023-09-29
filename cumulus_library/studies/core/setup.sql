-- Emergency Department Notes
--
CREATE TABLE core__ed_note AS
SELECT
    t.from_system,
    t.from_code,
    t.analyte,
    t.code_system,
    t.code,
    t.display
FROM
    (
        VALUES
        (
            'BCH',
            'NOTE:149798455',
            'Emergency MD',
            'http://loinc.org',
            '34878-9',
            'Emergency medicine Note'
        ),
        (
            'BCH',
            'NOTE:159552404',
            'ED Note Scanned',
            'http://loinc.org',
            '34878-9',
            'Emergency medicine Note'
        ),
        (
            'BCH',
            'NOTE:3807712',
            'ED Note',
            'http://loinc.org',
            '34878-9',
            'Emergency medicine Note'
        ),
        (
            'BCH',
            'NOTE:189094644',
            'Emergency Dept Scanned Forms',
            'http://loinc.org',
            '34878-9',
            'Emergency medicine Note'
        ),
        (
            'BCH',
            'NOTE:189094576',
            'ED Scanned',
            'http://loinc.org',
            '34878-9',
            'Emergency medicine Note'
        ),
        (
            'BCH',
            'NOTE:3710480',
            'ED Consultation',
            'http://loinc.org',
            '51846-4',
            'Emergency department Consult note'
        )
    )
    AS t (from_system, from_code, analyte, code_system, code, display);

CREATE TABLE core__act_encounter_code_v3 AS
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
    AS t (code, display)
