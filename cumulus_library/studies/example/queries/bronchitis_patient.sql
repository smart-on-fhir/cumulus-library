CREATE TABLE cumulus_example__bronchitis_patient AS (
    SELECT DISTINCT p.*
    FROM core__patient AS p
    INNER JOIN cumulus_example__bronchitis_condition AS c
        ON p.subject_ref = c.subject_ref
);
