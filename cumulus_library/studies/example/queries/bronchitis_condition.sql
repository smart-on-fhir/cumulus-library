CREATE TABLE example__bronchitis_condition AS (
    SELECT DISTINCT c.*
    FROM core__condition AS c
    INNER JOIN example__snomed_bronchitis AS sb
        ON c.code = sb.code AND c.system = sb.system
    INNER JOIN example__study_population AS sp
        ON c.subject_ref = sp.subject_ref
);
