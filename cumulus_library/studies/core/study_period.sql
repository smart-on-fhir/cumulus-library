CREATE TABLE core__study_period AS
WITH documented_encounter AS (
    SELECT DISTINCT
        ce.start_date,
        ce.start_week,
        ce.start_month,
        ce.end_date,
        ce.age_at_visit,
        cd.author_date,
        cd.author_week,
        cd.author_month,
        cd.author_year,
        cp.gender,
        cp.race_display,
        cp.ethnicity_display,
        cp.subject_ref,
        ce.encounter_ref,
        ce.status,
        cd.doc_ref,
        date_diff('day', ce.start_date, cd.author_date) AS diff_enc_note_days,
        coalesce(ce.enc_class_code, 'None') AS enc_class_code,
        coalesce(ce.enc_class_display, 'None') AS enc_class_display,
        coalesce(cd.doc_type.code, 'None') AS doc_type_code,
        coalesce(cd.doc_type.display, cd.doc_type.code) AS doc_type_display
    FROM
        core__patient AS cp,
        core__encounter AS ce,
        core__documentreference AS cd
    WHERE
        (cp.subject_ref = ce.subject_ref)
        AND (ce.encounter_ref = cd.encounter_ref)
        AND (cd.author_date BETWEEN date('2016-06-01') AND current_date)
        AND (ce.start_date BETWEEN date('2016-06-01') AND current_date)
        AND (ce.end_date BETWEEN date('2016-06-01') AND current_date)
)

SELECT
    de.start_date,
    de.start_week,
    de.start_month,
    de.end_date,
    de.age_at_visit,
    de.author_date,
    de.author_week,
    de.author_month,
    de.author_year,
    de.gender,
    de.race_display,
    de.ethnicity_display,
    de.subject_ref,
    de.encounter_ref,
    de.status,
    de.doc_ref,
    de.diff_enc_note_days,
    de.enc_class_code,
    de.enc_class_display,
    de.doc_type_code,
    de.doc_type_display,
    coalesce(ed.code IS NOT NULL, false) AS ed_note
FROM documented_encounter AS de
LEFT JOIN core__ed_note AS ed
    ON de.doc_type_code = ed.from_code;

CREATE TABLE core__meta_date AS
SELECT
    min(start_date) AS min_date,
    max(end_date) AS max_date
FROM core__study_period;
