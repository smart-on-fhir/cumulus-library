CREATE TABLE core__meta_date AS
WITH valid_period AS (
    SELECT DISTINCT
        ce.period_start_day,
        ce.period_end_day
    FROM
        core__patient AS cp,
        core__encounter AS ce
    WHERE
        (cp.subject_ref = ce.subject_ref)
)

SELECT
    min(period_start_day) AS min_date,
    max(period_end_day) AS max_date
FROM valid_period;
