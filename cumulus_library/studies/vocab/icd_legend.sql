CREATE TABLE vocab__icd_legend AS
SELECT
    code,
    display,
    code_display,
    code_system
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
