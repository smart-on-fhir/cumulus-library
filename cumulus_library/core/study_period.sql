DROP TABLE IF EXISTS core_study_period;
DROP TABLE IF EXISTS core_meta_date;

CREATE TABLE core_study_period AS
WITH documented_encounter AS (
    SELECT DISTINCT
        e.start_date,
        e.start_week,
        e.start_month,
        e.end_date,
        e.age_at_visit,
        d.author_date,
        d.author_week,
        d.author_month,
        d.author_year,
        p.gender,
        p.race.display AS race_display,
        p.subject_ref,
        e.encounter_ref,
        d.doc_ref,
        date_diff('day', e.start_date, d.author_date) AS diff_enc_note_days,
        coalesce(enc_class.code, '?') AS enc_class_code,
        coalesce(doc_type.code, '?') AS doc_type_code,
        coalesce(doc_type.display, doc_type.code) AS doc_type_display
    FROM
        core_patient AS p,
        core_encounter AS e,
        core_documentreference AS d
    WHERE
        (p.subject_ref = e.subject_ref)
        AND (e.encounter_ref = d.encounter_ref)
        AND (d.author_date BETWEEN date('2016-06-01') AND current_date)
        AND (e.start_date BETWEEN date('2016-06-01') AND current_date)
        AND (e.end_date BETWEEN date('2016-06-01') AND current_date)
)

SELECT
    documented_encounter.*,
    coalesce(ed.code IS NOT NULL, false) AS ed_note
FROM documented_encounter
LEFT JOIN site_ed_note AS ed
    ON documented_encounter.doc_type_code = ed.from_code;

CREATE TABLE core_meta_date AS
SELECT
    min(start_date) AS min_date,
    max(end_date) AS max_date
FROM core_study_period;
