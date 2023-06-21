CREATE TABLE template__influenza_codes AS
SELECT
    t.from_system,
    t.analyte,
    t.system,
    t.code,
    t.display
FROM
    (
        VALUES
        (
            'Synthea-1000',
            'FLU',
            'http://loinc.org',
            '92142-9',
            'Influenza virus A RNA [Presence] in Respiratory specimen by NAA with probe detection' --noqa: LT05
        ),
        (
            'Synthea-1000',
            'FLU',
            'http://loinc.org',
            '92141-1',
            'Influenza virus B RNA [Presence] in Respiratory specimen by NAA with probe detection' --noqa: LT05
        ),
        (
            'Synthea-1000',
            'FLU',
            'http://loinc.org',
            '80383-3',
            'Influenza virus B Ag [Presence] in Upper respiratory specimen by Rapid immunoassay' --noqa: LT05
        ),
        (
            'Synthea-1000',
            'FLU',
            'http://loinc.org',
            '80382-5',
            'Influenza virus A Ag [Presence] in Upper respiratory specimen by Rapid immunoassay' --noqa: LT05
        )
    ) AS t (from_system, analyte, system, code, display);
