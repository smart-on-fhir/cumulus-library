CREATE TABLE psm_test__psm_cohort AS (
    SELECT * FROM core__condition ORDER BY condition_id DESC LIMIT 7 --noqa: AM04
);
