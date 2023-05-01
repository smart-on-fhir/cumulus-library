-- Emergency Department Notes
--
CREATE OR REPLACE VIEW core__ed_note AS SELECT * FROM
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
    --    , ('BCH', 'NOTE:318198113', 'ED Social Work',                 'http://loinc.org','28653-4', 'Social work Note')
    --    , ('BCH', 'NOTE:318198110', 'ED Social Work Brief Screening', 'http://loinc.org','28653-4', 'Social work Note')
    --    , ('BCH', 'NOTE:318198107', 'ED Social Work Assessment',      'http://loinc.org','28653-4', 'Social work Note')
    --    , ('BCH', 'NOTE:189094619', 'Sexual Assault Nurse Exam (SANE) Report', 'http://loinc.org',  '57053-1', 'Nurse Emergency department Note')
    );
