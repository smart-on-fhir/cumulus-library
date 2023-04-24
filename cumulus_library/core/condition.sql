-- ############################################################
-- Condition
-- https://build.fhir.org/ig/HL7/US-Core/StructureDefinition-us-core-condition-encounter-diagnosis.html
-- 
--Each Condition must have:
--    a category code of “problem-list-item” or “health-concern”
--    a code that identifies the condition
--    a patient
--Each Condition must support:
--    a clinical status of the condition (e.g., active or resolved)
--    a verification status
--    a category code of ‘sdoh’
--    a date of diagnosis*
--    abatement date (in other words, date of resolution or remission)
--    a date when recorded

DROP TABLE IF EXISTS core_condition;

CREATE TABLE core_condition AS
WITH temp_condition AS (
    SELECT
        c.category,
        c.code,
        c.clinicalstatus,
        c.verificationstatus,
        c.subject.reference AS subject_ref,
        c.encounter.reference AS encounter_ref,
        c.id AS condition_id,
        date(from_iso8601_timestamp(c.recordeddate)) AS recordeddate,
        concat('Condition/', c.id) AS condition_ref
    FROM condition AS c
)

SELECT
    t_category_coding.category_row AS category,
    c.code AS cond_code,
    c.subject_ref,
    c.encounter_ref,
    c.condition_id,
    c.condition_ref,
    c.recordeddate,
    date_trunc('week', date(c.recordeddate)) AS recorded_week,
    date_trunc('month', date(c.recordeddate)) AS recorded_month,
    date_trunc('year', date(c.recordeddate)) AS recorded_year
FROM temp_condition AS c,
    unnest(category) AS t_category (category_coding), --noqa
    unnest(category_coding.coding) AS t_category_coding (category_row), --noqa
    unnest(code.coding) AS t_coding (code_row) --noqa
WHERE c.recordeddate BETWEEN date('2016-01-01') AND current_date;

CREATE OR REPLACE VIEW join_core_condition_icd AS
SELECT
    c.subject_ref,
    c.encounter_ref,
    c.recorded_month AS cond_month,
    e.enc_class.code AS enc_class_code,
    icd.code AS cond_code,
    icd.code_display AS cond_code_display
FROM core_condition AS c, core_encounter AS e, icd_legend AS icd
WHERE
    c.encounter_ref = e.encounter_ref
    AND c.cond_code.coding[1].code = icd.code; --noqa

CREATE OR REPLACE VIEW count_core_condition_icd10_month AS
WITH powerset AS (
    SELECT
        count(DISTINCT c.subject_ref) AS cnt_subject,
        count(DISTINCT c.encounter_ref) AS cnt_encounter,
        icd.code_display,
        c.recorded_month,
        e.enc_class
    FROM core_condition AS c, core_encounter AS e, icd_legend AS icd
    WHERE
        c.encounter_ref = e.encounter_ref
        AND c.cond_code.coding[1].code = icd.code --noqa
    GROUP BY cube(icd.code_display, c.recorded_month, e.enc_class)
)

SELECT
    powerset.cnt_subject AS cnt,
    powerset.recorded_month AS cond_month,
    powerset.code_display AS cond_code_display,
    enc_class.code AS enc_class_code
FROM powerset
WHERE powerset.cnt_subject >= 10
ORDER BY powerset.cnt_subject DESC, powerset.cnt_encounter DESC;
