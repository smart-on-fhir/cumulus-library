-- ############################################################
-- FHIR MedicationRequest : Med Orders *possibly* Administered
CREATE TABLE core__medicationrequest AS
SELECT
    mr.status,
    mr.intent,
    date(from_iso8601_timestamp(mr.authoredon)) AS authoredon,
    date_trunc('month', date(from_iso8601_timestamp(mr.authoredon)))
    AS authoredon_month,
    mr.category,
    cm.code_system AS rx_system,
    cm.code AS rx_code,
    coalesce(cm.display, 'None') AS rx_display,
    mr.id AS med_admin_id,
    mr.subject.reference AS subject_ref
FROM medicationrequest AS mr
INNER JOIN core__medication AS cm ON cm.id = mr.id
WHERE cm.code_system = 'http://www.nlm.nih.gov/research/umls/rxnorm';


CREATE TABLE core__count_medicationrequest_month AS
WITH powerset AS (
    SELECT
        count(DISTINCT cmr.subject_ref) AS cnt_subject,
        cmr.status,
        cmr.intent,
        cmr.authoredon_month,
        cmr.rx_display AS display
    FROM core__medicationrequest AS cmr
    GROUP BY cube(cmr.status, cmr.intent, cmr.authoredon_month, cmr.rx_display)
)

SELECT
    cnt_subject AS cnt,
    status,
    intent,
    authoredon_month,
    display
FROM powerset
WHERE cnt_subject >= 10
ORDER BY cnt DESC;
