CREATE TABLE example__bronchitis_meds_by_patient AS (
    WITH step_1 AS (
        SELECT
            e.subject_ref,
            e.encounter_ref,
            e.age_at_visit,
            p.gender,
            p.race_display
        FROM example__bronchitis_encounter AS e
        LEFT JOIN example__bronchitis_patient AS p
            ON e.subject_ref = p.subject_ref
        ORDER BY e.subject_ref, e.encounter_ref
    )

    SELECT
        s.subject_ref,
        s.encounter_ref,
        s.gender,
        s.race_display,
        s.age_at_visit,
        m.medication_code
    FROM step_1 AS s
    LEFT JOIN example__bronchitis_medicationrequest AS m
        ON
            s.subject_ref = m.subject_ref
            AND s.encounter_ref = m.encounter_ref
);
