CREATE OR REPLACE VIEW template__flu_codes AS
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
            '92140-3',
            'Parainfluenza virus 1 RNA [Presence] in Respiratory specimen by NAA with probe detection' --noqa: LT05
        ),
        (
            'Synthea-1000',
            'FLU',
            'http://loinc.org',
            '92142-9',
            'Influenza virus A RNA [Presence] in Respiratory specimen by NAA with probe detection' --noqa: LT05
        )
    ) AS t (from_system, analyte, system, code, display);
