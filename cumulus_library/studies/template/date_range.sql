CREATE TABLE template__meta_date AS
SELECT
    min(tilo.influenza_test_date) AS min_date,
    max(tilo.influenza_test_date) AS max_date
FROM template__influenza_lab_observations AS tilo;
