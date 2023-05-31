-- ############################################################
-- FHIR MedicationRequest : Med Orders *possibly* Administered
CREATE TABLE core__medicationrequest AS
SELECT
    mr.status,
    mr.intent,
    date(from_iso8601_timestamp(mr.authoredon)) AS authoredon,
    mr.category,
    med_coding.code AS code,
    med_coding.display AS display,
    med_coding.system AS system, --noqa: RF04
    mr.id AS med_admin_id,
    mr.subject.reference AS subject_id
FROM medicationrequest AS mr,
    unnest(mr.medicationcodeableconcept.coding) AS t (med_coding); --noqa: AL05


CREATE OR REPLACE VIEW core__countmedicationrequest_month AS
WITH powerset AS (
    SELECT
        count(DISTINCT cmr.subject_id) AS cnt_subject,
        cmr.status,
        cmr.intent,
        date_trunc('month', cmr.authoredon) AS authored_month,
        cmr.display,
        cmr.system
    FROM core__medicationrequest AS cmr
    GROUP BY cube(cmr.status, cmr.intent, cmr.authoredon, cmr.display, cmr.system)
)

SELECT
    cnt_subject AS cnt,
    status,
    intent,
    authored_month,
    display,
    system
FROM powerset
WHERE cnt_subject >= 10
ORDER BY cnt DESC;
