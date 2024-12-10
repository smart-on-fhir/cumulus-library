CREATE TABLE psm_test__psm_cohort AS (
    SELECT * FROM core__condition --noqa: AM04
    ORDER BY id DESC LIMIT 100
);
