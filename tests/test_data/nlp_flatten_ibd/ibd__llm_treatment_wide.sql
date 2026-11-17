CREATE TABLE ibd__llm_treatment_wide AS
SELECT DISTINCT
    nlp.note_ref,
    nlp.encounter_ref,
    nlp.subject_ref,
    'ibd__nlp_treatment_gpt_oss_120b' AS origin,
    nlp.generated_on AS generated_on,
    nlp.task_version AS task_version,
    nlp.system_fingerprint AS system_fingerprint,
    med.rx_class.rx_class AS rx_class,
    med.rx_status.rx_status AS rx_status,
    med.rx_treatment_phase.rx_treatment_phase AS rx_treatment_phase,
    med.rx_start_date.rx_start_date AS rx_start_date,
    med.rx_start_date.rx_start_date_precision AS rx_start_date_precision,
    med.rx_response.rx_response AS rx_response,
    ade.ade AS ade
FROM
    ibd__nlp_treatment_gpt_oss_120b AS nlp
    CROSS JOIN UNNEST(nlp.result.rx_annotations) AS t_med(med)
    LEFT JOIN UNNEST(med.rx_adverse_drug_events) AS t_ade(ade) ON TRUE
WHERE
    nlp.task_version IN (7)
