CREATE TABLE cumulus_example__bronchitis_medicationrequest AS (
    SELECT DISTINCT mr.*
    FROM core__medicationrequest AS mr
    INNER JOIN cumulus_example__bronchitis_condition AS c
        ON mr.subject_ref = c.subject_ref AND mr.encounter_ref = c.encounter_ref
);
