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
    med_coding.code AS code,
    med_coding.display AS display,
    med_coding.system AS system, --noqa: RF04
    mr.id AS med_admin_id,
    mr.subject.reference AS subject_id
FROM medicationrequest AS mr,
    unnest(mr.medicationcodeableconcept.coding) AS t (med_coding) --noqa: AL05
WHERE med_coding.system = 'http://www.nlm.nih.gov/research/umls/rxnorm';


CREATE OR REPLACE VIEW core__count_medicationrequest_month AS
WITH powerset AS (
    SELECT
        count(DISTINCT cmr.subject_id) AS cnt_subject,
        cmr.status,
        cmr.intent,
        cmr.authoredon_month,
        cmr.display
    FROM core__medicationrequest AS cmr
    GROUP BY cube(cmr.status, cmr.intent, cmr.authoredon_month, cmr.display)
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
