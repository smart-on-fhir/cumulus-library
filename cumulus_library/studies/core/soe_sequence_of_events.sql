-- SOE "Sequence of Events" links FHIR resources based on encounter reference
-- and FHIR resource datetime "period". Ideally, every FHIR resource references
-- an encounter. The primary function of the SOE tables and counts is QA/Verification.
--
-- ###########################################################################
-- Encounter
-- Encounter specifies the start/end period from which other FHIR resources can
-- be mapped. This is useful when FHIR Resource.encounter.reference is missing.
--
-- # length of stay calculations
-- from Karen Olson
--  LOS= discharge date/time - admit date/time
--  EDLOS= checkout date/time - check-in date/time
--  check-in date/time generally = admit date/time, sometimes it's off a little
--  Care_class= Emergency is ED only
--  An encounter with care_class= Inpatient or Observation can include time in the ED
--  If patient was admitted from the ED, THEN only encounter.
--
CREATE TABLE core__soe AS
WITH soe_rawdata AS (
    SELECT DISTINCT
        date(from_iso8601_timestamp(e.period."start")) AS enc_start_date,
        date(from_iso8601_timestamp(e.period."end")) AS enc_end_date,
        cast(
            from_iso8601_timestamp(e.period."start") AS timestamp
        ) AS enc_start_datetime,
        cast(from_iso8601_timestamp(e.period."end") AS timestamp) AS enc_end_datetime,
        e.class AS enc_class,
        subject.reference AS subject_ref,
        concat('Encounter/', e.id) AS encounter_ref
    FROM encounter AS e
)

SELECT DISTINCT
    enc_start_date,
    enc_end_date,
    enc_start_datetime,
    enc_end_datetime,
    enc_class,
    subject_ref,
    encounter_ref,
    date_diff('hour', enc_start_datetime, enc_end_datetime) AS los_hours,
    date_diff('day', enc_start_date, enc_end_date) AS los_days
FROM soe_rawdata
WHERE enc_start_date BETWEEN date('2016-01-01') AND current_date;

-- ###########################################################################
-- Condition

CREATE TABLE core__soe_cond AS
WITH soe_cond_rawdata AS (
    SELECT DISTINCT
        cast(
            from_iso8601_timestamp(c.recordeddate) AS timestamp
        ) AS cond_recorded_datetime,
        cast(
            from_iso8601_timestamp(c.onsetdatetime) AS timestamp
        ) AS cond_onset_datetime,
        c.subject.reference AS subject_ref,
        c.encounter.reference AS encounter_ref,
        c.category,
        concat('Condition/', c.id) AS condition_ref
    FROM condition AS c
),

cond AS (
    SELECT DISTINCT
        scr.cond_recorded_datetime,
        scr.cond_onset_datetime,
        scr.subject_ref,
        scr.encounter_ref,
        scr.category,
        scr.condition_ref,
        t2.category_row
    FROM soe_cond_rawdata AS scr,
        unnest(category) AS t (category_coding),
        unnest(category_coding.coding) AS t2 (category_row)
    WHERE scr.cond_recorded_datetime BETWEEN date('2016-01-01') AND current_date
),

cond_link_reference AS (
    SELECT DISTINCT
        cond.cond_recorded_datetime,
        cond.cond_onset_datetime,
        cond.condition_ref,
        cs.enc_start_date,
        cs.enc_end_date,
        cs.enc_start_datetime,
        cs.enc_end_datetime,
        cs.enc_class,
        cs.subject_ref,
        cs.encounter_ref,
        cs.los_hours,
        cs.los_days
    FROM cond, core__soe AS cs
    WHERE
        cond.encounter_ref IS NOT NULL
        AND cond.encounter_ref = cs.encounter_ref
),

cond_link_period AS (
    SELECT DISTINCT
        cond.cond_recorded_datetime,
        cond.cond_onset_datetime,
        cond.condition_ref,
        cs.enc_start_date,
        cs.enc_end_date,
        cs.enc_start_datetime,
        cs.enc_end_datetime,
        cs.enc_class,
        cs.subject_ref,
        cs.encounter_ref,
        cs.los_hours,
        cs.los_days
    FROM cond, core__soe AS cs
    WHERE
        cond.encounter_ref IS NULL
        AND cond.subject_ref = cs.subject_ref
        AND (
            (
                cond.cond_recorded_datetime
                BETWEEN cs.enc_start_datetime
                AND cs.enc_end_datetime
            )
            OR
            (
                cond.cond_onset_datetime
                BETWEEN cs.enc_start_datetime
                AND cs.enc_end_datetime
            )
        )
)

SELECT DISTINCT
    cond.category_row AS category,
    date(cond.cond_recorded_datetime) AS cond_recorded_date,
    date(cond.cond_onset_datetime) AS cond_onset_date,
    cond.cond_recorded_datetime,
    cond.cond_onset_datetime,
    lr.encounter_ref AS encounter_ref,
    lp.encounter_ref AS period_ref,
    coalesce(lr.encounter_ref, lp.encounter_ref) AS encounter_link,
    coalesce(lr.enc_class, lp.enc_class) AS enc_class,
    -- condition and subject must match.
    cond.subject_ref,
    cond.condition_ref
FROM cond
LEFT JOIN cond_link_period AS lp ON cond.condition_ref = lp.condition_ref
LEFT JOIN cond_link_reference AS lr ON cond.condition_ref = lr.condition_ref;

-- ###########################################################################
-- MedicationRequest

CREATE TABLE core__soe_medreq AS
WITH soe_medreq_rawdata AS (
    SELECT DISTINCT
        cast(
            from_iso8601_timestamp(mr.authoredon) AS timestamp
        ) AS medreq_authored_datetime,
        mr.subject.reference AS subject_ref,
        mr.encounter.reference AS encounter_ref,
        mr.id AS medreq_id,
        mr.status AS status,
        concat('MedicationRequest/', mr.id) AS medreq_ref,
        mr.medicationreference.reference AS medication_ref
    FROM medicationrequest AS mr
),

medreq AS (
    SELECT DISTINCT
        medreq_authored_datetime,
        subject_ref,
        encounter_ref,
        medreq_id,
        status,
        medreq_ref,
        medication_ref
    FROM soe_medreq_rawdata
    WHERE medreq_authored_datetime BETWEEN date('2016-01-01') AND current_date
),

soe_medreq_link_reference AS (
    SELECT DISTINCT
        medreq.medreq_authored_datetime,
        medreq.medreq_ref,
        cs.enc_start_date,
        cs.enc_end_date,
        cs.enc_start_datetime,
        cs.enc_end_datetime,
        cs.enc_class,
        cs.subject_ref,
        cs.encounter_ref,
        cs.los_hours,
        cs.los_days
    FROM medreq, core__soe AS cs
    WHERE
        medreq.encounter_ref IS NOT NULL
        AND medreq.encounter_ref = cs.encounter_ref
),

soe_medreq_link_period AS (
    SELECT DISTINCT
        medreq.medreq_authored_datetime,
        medreq.medreq_ref,
        cs.enc_start_date,
        cs.enc_end_date,
        cs.enc_start_datetime,
        cs.enc_end_datetime,
        cs.enc_class,
        cs.subject_ref,
        cs.encounter_ref,
        cs.los_hours,
        cs.los_days
    FROM medreq, core__soe AS cs
    WHERE
        medreq.encounter_ref IS NULL
        AND medreq.subject_ref = cs.subject_ref
        AND medreq.medreq_authored_datetime
        BETWEEN cs.enc_start_datetime AND cs.enc_end_datetime
)

SELECT DISTINCT
    medreq.status,
    date(medreq.medreq_authored_datetime) AS medreq_recorded_date,
    medreq.medreq_authored_datetime,
    lr.encounter_ref AS encounter_ref,
    lp.encounter_ref AS period_ref,
    coalesce(lr.encounter_ref, lp.encounter_ref) AS encounter_link,
    coalesce(lr.enc_class, lp.enc_class) AS enc_class,
    -- condition AND subject must match.
    medreq.subject_ref,
    medreq.medreq_ref
FROM medreq
LEFT JOIN soe_medreq_link_period AS lp ON medreq.medreq_ref = lp.medreq_ref
LEFT JOIN soe_medreq_link_reference AS lr ON medreq.medreq_ref = lr.medreq_ref;

-- ###########################################################################
-- DocumentReference

CREATE TABLE core__soe_document AS
WITH soe_document_rawdata AS (
    SELECT DISTINCT
        cast(
            from_iso8601_timestamp(context.period."start") AS timestamp
        ) AS doc_start_datetime,
        cast(
            from_iso8601_timestamp(context.period."end") AS timestamp
        ) AS doc_end_datetime,
        doc.subject.reference AS subject_ref,
        doc.context,
        doc.id AS doc_id,
        concat('DocumentReference/', doc.id) AS doc_ref
    FROM documentreference AS doc
),

document AS (
    SELECT DISTINCT
        doc_start_datetime,
        doc_end_datetime,
        subject_ref,
        context,
        doc_id,
        doc_ref
    FROM soe_document_rawdata
    WHERE
        (doc_start_datetime BETWEEN date('2016-01-01') AND current_date)
        OR (doc_end_datetime BETWEEN date('2016-01-01') AND current_date)
),

document_enc AS (
    SELECT DISTINCT
        d.doc_start_datetime,
        d.doc_end_datetime,
        d.subject_ref,
        d.context,
        d.doc_id,
        d.doc_ref,
        t.context_encounter.reference AS encounter_ref
    FROM
        document AS d,
        unnest(context.encounter) AS t (context_encounter)
),

doc_link_period AS (
    SELECT DISTINCT
        document.doc_start_datetime,
        document.doc_end_datetime,
        document.doc_ref,
        cs.enc_start_date,
        cs.enc_end_date,
        cs.enc_start_datetime,
        cs.enc_end_datetime,
        cs.enc_class,
        cs.subject_ref,
        cs.encounter_ref,
        cs.los_hours,
        cs.los_days
    FROM document, core__soe AS cs
    WHERE
        document.subject_ref = cs.subject_ref
        AND (
            (
                document.doc_start_datetime
                BETWEEN cs.enc_start_datetime
                AND cs.enc_end_datetime
            )
            OR
            (
                document.doc_end_datetime
                BETWEEN cs.enc_start_datetime
                AND cs.enc_end_datetime
            )
        )
),

doc_link_reference AS (
    SELECT DISTINCT
        document_enc.doc_start_datetime,
        document_enc.doc_end_datetime,
        document_enc.doc_ref,
        cs.enc_start_date,
        cs.enc_end_date,
        cs.enc_start_datetime,
        cs.enc_end_datetime,
        cs.enc_class,
        cs.subject_ref,
        cs.encounter_ref,
        cs.los_hours,
        cs.los_days
    FROM document_enc, core__soe AS cs
    WHERE
        document_enc.encounter_ref IS NOT NULL
        AND document_enc.encounter_ref = cs.encounter_ref
)

SELECT DISTINCT
    document.doc_ref,
    document.doc_start_datetime,
    document.doc_end_datetime,
    lr.encounter_ref AS encounter_ref,
    lp.encounter_ref AS period_ref,
    -- link REF via Encounter.reference when provided, ELSE link Encounter.period
    coalesce(lr.encounter_ref, lp.encounter_ref) AS encounter_link,
    -- link CLASS via Encounter.reference when provided, ELSE link Encounter.period
    coalesce(lr.enc_class, lp.enc_class) AS enc_class,
    -- document must match
    document.subject_ref
FROM document
LEFT JOIN doc_link_period AS lp ON document.doc_ref = lp.doc_ref
LEFT JOIN doc_link_reference AS lr ON document.doc_ref = lr.doc_ref;

-- ###########################################################################
-- Sequence Of Events, COUNTS
--
-- COUNT Condition by week
CREATE TABLE core__count_soe_cond_week AS
WITH discrete AS (
    SELECT
        core__soe_cond.condition_ref,
        enc_class.display AS enc_class_display,
        enc_class.system AS enc_class_system,
        date_trunc('week', core__soe_cond.cond_recorded_date) AS cond_recorded_week,
        coalesce(
            core__soe_cond.cond_onset_date IS NOT NULL, FALSE
        ) AS cond_onset_date_exists,
        coalesce(
            core__soe_cond.encounter_ref IS NOT NULL, FALSE
        ) AS encounter_ref_exists,
        coalesce(
            core__soe_cond.period_ref IS NOT NULL, FALSE
        ) AS period_ref_exists
    FROM core__soe_cond
)

SELECT
    count(DISTINCT condition_ref) AS cnt,
    enc_class_system,
    enc_class_display,
    cond_recorded_week,
    cond_onset_date_exists,
    encounter_ref_exists,
    period_ref_exists
FROM discrete
GROUP BY cube(
    enc_class_system,
    enc_class_display,
    cond_recorded_week,
    cond_onset_date_exists,
    encounter_ref_exists,
    period_ref_exists
);


-- COUNT MedReq by week

CREATE TABLE core__count_soe_medreq_week AS
WITH discrete AS (
    SELECT
        core__soe_medreq.medreq_ref,
        enc_class.display AS enc_class_display,
        enc_class.system AS enc_class_system,
        date_trunc(
            'week', core__soe_medreq.medreq_recorded_date
        ) AS medreq_recorded_week,
        coalesce(
            core__soe_medreq.encounter_ref IS NOT NULL, FALSE
        ) AS encounter_ref_exists,
        coalesce(
            core__soe_medreq.period_ref IS NOT NULL, FALSE
        ) AS period_ref_exists
    FROM core__soe_medreq
)

SELECT
    count(DISTINCT medreq_ref) AS cnt,
    enc_class_display,
    enc_class_system,
    medreq_recorded_week,
    encounter_ref_exists,
    period_ref_exists
FROM discrete
GROUP BY cube(
    enc_class_display,
    enc_class_system,
    medreq_recorded_week,
    encounter_ref_exists,
    period_ref_exists
);


-- COUNT Document by week

CREATE TABLE core__count_soe_document_week AS
WITH discrete AS (
    SELECT
        core__soe_document.doc_ref,
        enc_class.display AS enc_class_display,
        enc_class.system AS enc_class_system,
        date_trunc(
            'week', date(core__soe_document.doc_start_datetime)
        ) AS doc_start_week,
        coalesce(
            core__soe_document.encounter_ref IS NOT NULL, FALSE
        ) AS encounter_ref_exists,
        coalesce(
            core__soe_document.period_ref IS NOT NULL, FALSE
        ) AS period_ref_exists
    FROM core__soe_document
)

SELECT
    count(DISTINCT doc_ref) AS cnt,
    enc_class_display,
    enc_class_system,
    doc_start_week,
    encounter_ref_exists,
    period_ref_exists
FROM discrete
GROUP BY cube(
    enc_class_display,
    enc_class_system,
    doc_start_week,
    encounter_ref_exists,
    period_ref_exists
);
