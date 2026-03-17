CREATE TABLE example__meta_date AS
SELECT
    min(period_start_day) AS min_date,
    max(period_end_day) AS max_date
FROM example__bronchitis_encounter;
