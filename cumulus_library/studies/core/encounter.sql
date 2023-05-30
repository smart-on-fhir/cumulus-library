-- #############################################################
-- Encounter
-- https://build.fhir.org/ig/HL7/US-Core/StructureDefinition-us-core-encounter.html

CREATE TABLE core__encounter AS
WITH temp_encounter AS (
    SELECT DISTINCT
        e.period,
        e.status,
        e.class,
        e.type,
        e.subject.reference AS subject_ref,
        e.id AS encounter_id,
        date(from_iso8601_timestamp(e.period."start")) AS start_date,
        date(from_iso8601_timestamp(e.period."end")) AS end_date,
        concat('Encounter/', e.id) AS encounter_ref
    -- , reasonCode
    -- , dischargeDisposition
    FROM encounter AS e
)

SELECT DISTINCT
    e.class AS enc_class,
    e.type AS enc_type,
    date_diff('year', date(p.birthdate), e.start_date) AS age_at_visit,
    date_trunc('day', e.start_date) AS start_date,
    date_trunc('day', e.end_date) AS end_date,
    date_trunc('week', e.start_date) AS start_week,
    date_trunc('month', e.start_date) AS start_month,
    date_trunc('year', e.start_date) AS start_year,
    e.subject_ref,
    e.encounter_ref,
    e.encounter_id
FROM temp_encounter AS e, core__patient AS p
WHERE
    e.subject_ref = p.subject_ref
    AND start_date BETWEEN date('2016-06-01') AND current_date;

CREATE OR REPLACE VIEW core__join_encounter_patient AS
SELECT
    ce.enc_class,
    ce.enc_type,
    ce.age_at_visit,
    ce.start_date,
    ce.end_date,
    ce.start_week,
    ce.start_month,
    ce.start_year,
    ce.subject_ref,
    ce.encounter_ref,
    ce.encounter_id,
    ce.enc_class.code AS enc_class_code,
    cp.gender,
    cp.race_display,
    cp.ethnicity_display,
    cp.postalcode3
FROM core__encounter AS ce, core__patient AS cp
WHERE ce.subject_ref = cp.subject_ref;


CREATE OR REPLACE VIEW core__count_encounter_month AS
WITH powerset AS (
    SELECT
        count(DISTINCT ce.subject_ref) AS cnt_subject,
        count(DISTINCT ce.encounter_id) AS cnt_encounter,
        ce.enc_class.code AS enc_class_code,
        ce.start_month,
        ce.age_at_visit,
        cp.gender,
        cp.race_display,
        cp.ethnicity_display,
        cp.postalcode3
    FROM core__encounter AS ce, core__patient AS cp
    WHERE ce.subject_ref = cp.subject_ref
    GROUP BY
        cube(
            ce.enc_class,
            ce.start_month,
            ce.age_at_visit,
            cp.gender,
            cp.race_display,
            cp.ethnicity_display,
            cp.postalcode3
        )
)

SELECT DISTINCT
    powerset.cnt_encounter AS cnt,
    powerset.enc_class_code,
    powerset.start_month,
    powerset.age_at_visit,
    powerset.gender,
    powerset.race_display,
    powerset.ethnicity_display,
    powerset.postalcode3
FROM powerset
WHERE powerset.cnt_subject >= 10
ORDER BY
    powerset.start_month ASC, powerset.enc_class_code ASC, powerset.age_at_visit ASC;

CREATE OR REPLACE VIEW core__count_encounter_day AS
WITH powerset AS (
    SELECT
        count(DISTINCT ce.subject_ref) AS cnt_subject,
        count(DISTINCT ce.encounter_id) AS cnt_encounter,
        ce.enc_class.code AS enc_class_code,
        ce.start_date
    FROM core__encounter AS ce, core__patient AS cp
    WHERE ce.subject_ref = cp.subject_ref
    GROUP BY cube(ce.enc_class, ce.start_date)
)

SELECT DISTINCT
    cnt_encounter AS cnt,
    enc_class_code,
    start_date
FROM powerset
WHERE cnt_subject >= 10
ORDER BY start_date ASC, enc_class_code ASC;
