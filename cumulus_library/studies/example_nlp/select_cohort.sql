-- Just select five arbitrary entries from DocRefs and DxReports each.

CREATE TABLE example_nlp__cohort AS
WITH docrefs AS (
    SELECT DISTINCT src.documentreference_ref AS note_ref
    FROM core__documentreference AS src
    ORDER BY src.documentreference_ref
    LIMIT 5
),

dxreports AS (
    SELECT DISTINCT src.diagnosticreport_ref AS note_ref
    FROM core__diagnosticreport AS src
    ORDER BY src.diagnosticreport_ref
    LIMIT 5
)

SELECT * FROM docrefs
UNION ALL
SELECT * FROM dxreports
