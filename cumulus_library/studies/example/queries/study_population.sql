CREATE TABLE example__study_population AS (
    SELECT
        p.subject_ref,
        p.birthdate,
        e.encounter_ref,
        e.period_start_year
    FROM core__patient AS p
    INNER JOIN core__encounter AS e ON p.subject_ref = e.subject_ref
    WHERE
        date_diff('year', p.birthdate, CAST('2026-01-01' AS date) ) < (13 + 5)
        AND date_diff('year', e.period_start_year, CAST('2026-01-01' AS date) ) < 5
        AND date_diff('year', p.birthdate, e.period_start_year) < 13
);
