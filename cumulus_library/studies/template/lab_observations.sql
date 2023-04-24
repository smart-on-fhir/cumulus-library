CREATE TABLE template__influenza_lab_observations AS
SELECT DISTINCT
    upper(o.lab_result.display) AS influenza_test_display,
    o.lab_result AS influenza_test_result,
    o.lab_code AS influenza_test_code,
    o.lab_date AS influenza_test_date,
    o.lab_week AS influenza_test_week,
    o.lab_month AS influenza_test_month,
    o.subject_ref,
    o.encounter_ref,
    o.observation_ref
FROM core__observation_lab AS o,
    template__influenza_codes AS tfc
WHERE
    (o.lab_date BETWEEN date('2016-06-01') AND current_date)
    AND (o.lab_code.code = tfc.code);
