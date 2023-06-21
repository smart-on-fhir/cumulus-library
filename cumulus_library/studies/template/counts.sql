CREATE TABLE template__count_influenza_test_month AS
WITH powerset AS (
    SELECT
        count(DISTINCT tflo.subject_ref) AS cnt_subject,
        count(DISTINCT tflo.encounter_ref) AS cnt_encounter,
        tflo.influenza_test_code AS influenza_lab_code,
        upper(tflo.influenza_test_result.display) AS influenza_result_display,
        tflo.influenza_test_month
    FROM template__influenza_lab_observations AS tflo
    GROUP BY
        cube(
            tflo.influenza_test_code,
            tflo.influenza_test_result,
            tflo.influenza_test_month
        )
)

SELECT DISTINCT
    cnt_encounter AS cnt,
    influenza_lab_code,
    influenza_result_display,
    influenza_test_month
FROM powerset
WHERE cnt_subject >= 10
ORDER BY cnt_encounter DESC, influenza_test_month ASC, influenza_result_display ASC;
