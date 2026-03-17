CREATE TABLE cumulus_example__bronchitis_condition AS (
    SELECT DISTINCT c.*
    FROM core__condition AS c
    INNER JOIN cumulus_example__snomed_bronchitis AS sb
        ON c.code = sb.code AND c.system = sb.system
    INNER JOIN cumulus_example__study_population AS sp
        ON c.subject_ref = sp.subject_ref
);
