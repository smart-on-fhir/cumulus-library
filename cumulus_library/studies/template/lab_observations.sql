CREATE TABLE template__flu_lab_observations AS
SELECT DISTINCT
    upper(o.lab_result.display) AS flu_test_display,
    o.lab_result AS flu_test_result,
    o.lab_code AS flu_test_code,
    o.lab_date AS flu_test_date,
    o.lab_week AS flu_test_week,
    o.lab_month AS flu_test_month,
    o.subject_ref,
    o.encounter_ref,
    o.observation_ref
FROM core_observation_lab AS o,
    template__flu_codes AS tfc
WHERE
    (o.lab_date BETWEEN date('2016-06-01') AND current_date)
    AND (o.lab_code.code = tfc.code);
