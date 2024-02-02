CREATE TABLE psm_test__psm_cohort AS (
    SELECT * FROM core__condition ORDER BY id DESC LIMIT 100 --noqa: AM04
);
