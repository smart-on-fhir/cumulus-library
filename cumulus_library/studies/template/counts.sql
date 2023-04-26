CREATE OR REPLACE VIEW template__count_flu_test_month AS
WITH powerset AS (
    SELECT
        count(DISTINCT tflo.subject_ref) AS cnt_subject,
        count(DISTINCT tflo.encounter_ref) AS cnt_encounter,
        upper(tflo.flu_test_result.display) AS flu_result_display,
        tflo.flu_test_month
    FROM template__flu_lab_observations AS tflo
    GROUP BY
        cube(
            tflo.flu_test_result,
            tflo.flu_test_month
        )
)

SELECT DISTINCT
    cnt_encounter AS cnt,
    flu_result_display,
    flu_test_month
FROM powerset
WHERE cnt_subject >= 10
ORDER BY flu_test_month ASC, flu_result_display ASC;
