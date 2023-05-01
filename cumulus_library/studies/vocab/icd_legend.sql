CREATE OR REPLACE VIEW vocab__icd_legend AS
SELECT
    code,
    display,
    code_display,
    system
FROM (
    SELECT
        code,
        str AS display,
        CONCAT(code, ' ', str) AS code_display,
        sab AS code_system,
        ROW_NUMBER()
        OVER (
            PARTITION BY sab, code
            ORDER BY LENGTH(str) ASC
        ) AS pref
    FROM vocab__icd
)
WHERE pref = 1;