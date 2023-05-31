-- ############################################################
-- FHIR MedicationRequest : Med Orders *possibly* Administered
CREATE TABLE core__medication_request AS
SELECT
    mr.status,
    mr.intent,
    mr.authoredon,
    mr.category,
    med_coding.code AS code,
    med_coding.display AS display,
    med_coding.system AS system, --noqa: RF04
    mr.id AS med_admin_id,
    mr.subject.reference AS subject_id
FROM medicationrequest AS mr,
    UNNEST(mr.medicationcodeableconcept.coding) AS t (med_coding); --noqa: AL05
