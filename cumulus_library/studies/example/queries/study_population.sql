CREATE TABLE cumulus_example__study_population AS (
    SELECT
        p.subject_ref,
        p.birthdate,
        e.encounter_ref,
        e.period_start_year
    FROM core__patient AS p
    INNER JOIN core__encounter AS e ON p.subject_ref = e.subject_ref
    WHERE
        date_diff('year', p.birthdate, now()) < (13 + 5)
        AND date_diff('year', e.period_start_year, now()) < 5
        AND date_diff('year', p.birthdate, e.period_start_year) < 13
);
