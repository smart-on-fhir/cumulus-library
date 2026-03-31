CREATE TABLE example__bronchitis_encounter AS (
    SELECT DISTINCT e.*
    FROM core__encounter AS e
    INNER JOIN example__bronchitis_condition AS c
        ON e.subject_ref = c.subject_ref
);
