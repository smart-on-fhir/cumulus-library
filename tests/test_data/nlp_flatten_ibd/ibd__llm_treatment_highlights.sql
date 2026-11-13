-- The actual table logic -
CREATE TABLE ibd__llm_treatment_highlights AS
    SELECT
        nlp.note_ref,
        nlp.subject_ref,
        nlp.generated_on,
        nlp.task_version,
        nlp.system_fingerprint,
        'ibd__nlp_treatment_gpt_oss_120b' AS origin,
        'Medication' AS label,
        -- Merge 5 parallel span arrays into one start:end span
        CONCAT(CAST(ARRAY_MIN(FLATTEN(FLATTEN(FILTER(ARRAY[
            mention0.rx_class.spans,
            mention0.rx_status.spans,
            mention0.rx_treatment_phase.spans,
            mention0.rx_start_date.spans,
            mention0.rx_response.spans
        ], s -> s IS NOT NULL)))) AS VARCHAR), ':', CAST(ARRAY_MAX(FLATTEN(FLATTEN(FILTER(ARRAY[
            mention0.rx_class.spans,
            mention0.rx_status.spans,
            mention0.rx_treatment_phase.spans,
            mention0.rx_start_date.spans,
            mention0.rx_response.spans
        ], s -> s IS NOT NULL)))) AS VARCHAR)) AS span,
        -- Handle sublabel_name and sublabel_value per rx_class
        'Medication Class' AS sublabel_name,
        CAST(mention0.rx_class.rx_class AS VARCHAR) AS sublabel_value
    FROM
        ibd__nlp_treatment_gpt_oss_120b AS nlp,
        UNNEST(nlp.result.rx_annotations) AS t_mention0(mention0)
    -- Custom filtering per mention_key
    WHERE
        nlp.task_version IN (7)
        AND mention0.rx_class IS NOT NULL
        AND mention0.rx_class.rx_class != 'NONE_OF_THE_ABOVE'
        -- Drop mentions where every merged span array was NULL or empty. This guard is
        -- load-bearing and must stay on the raw bound rather than the concatenated span:
        -- CONCAT(NULL, ':', NULL) is NULL on Trino but ':' on DuckDB, so filtering on the
        -- assembled span would let junk rows through on one backend and not the other.
        AND ARRAY_MIN(FLATTEN(FLATTEN(FILTER(ARRAY[
            mention0.rx_class.spans,
            mention0.rx_status.spans,
            mention0.rx_treatment_phase.spans,
            mention0.rx_start_date.spans,
            mention0.rx_response.spans
        ], s -> s IS NOT NULL)))) IS NOT NULL
    UNION ALL
    SELECT
        nlp.note_ref,
        nlp.subject_ref,
        nlp.generated_on,
        nlp.task_version,
        nlp.system_fingerprint,
        'ibd__nlp_treatment_gpt_oss_120b' AS origin,
        'Medication' AS label,
        -- Merge 5 parallel span arrays into one start:end span
        CONCAT(CAST(ARRAY_MIN(FLATTEN(FLATTEN(FILTER(ARRAY[
            mention0.rx_class.spans,
            mention0.rx_status.spans,
            mention0.rx_treatment_phase.spans,
            mention0.rx_start_date.spans,
            mention0.rx_response.spans
        ], s -> s IS NOT NULL)))) AS VARCHAR), ':', CAST(ARRAY_MAX(FLATTEN(FLATTEN(FILTER(ARRAY[
            mention0.rx_class.spans,
            mention0.rx_status.spans,
            mention0.rx_treatment_phase.spans,
            mention0.rx_start_date.spans,
            mention0.rx_response.spans
        ], s -> s IS NOT NULL)))) AS VARCHAR)) AS span,
        -- Handle sublabel_name and sublabel_value per rx_status
        'Medication Status' AS sublabel_name,
        CAST(mention0.rx_status.rx_status AS VARCHAR) AS sublabel_value
    FROM
        ibd__nlp_treatment_gpt_oss_120b AS nlp,
        UNNEST(nlp.result.rx_annotations) AS t_mention0(mention0)
    -- Custom filtering per mention_key
    WHERE
        nlp.task_version IN (7)
        AND mention0.rx_status IS NOT NULL
        AND mention0.rx_status.rx_status != 'NONE_OF_THE_ABOVE'
        -- Drop mentions where every merged span array was NULL or empty. This guard is
        -- load-bearing and must stay on the raw bound rather than the concatenated span:
        -- CONCAT(NULL, ':', NULL) is NULL on Trino but ':' on DuckDB, so filtering on the
        -- assembled span would let junk rows through on one backend and not the other.
        AND ARRAY_MIN(FLATTEN(FLATTEN(FILTER(ARRAY[
            mention0.rx_class.spans,
            mention0.rx_status.spans,
            mention0.rx_treatment_phase.spans,
            mention0.rx_start_date.spans,
            mention0.rx_response.spans
        ], s -> s IS NOT NULL)))) IS NOT NULL
    UNION ALL
    SELECT
        nlp.note_ref,
        nlp.subject_ref,
        nlp.generated_on,
        nlp.task_version,
        nlp.system_fingerprint,
        'ibd__nlp_treatment_gpt_oss_120b' AS origin,
        'Medication' AS label,
        -- Merge 5 parallel span arrays into one start:end span
        CONCAT(CAST(ARRAY_MIN(FLATTEN(FLATTEN(FILTER(ARRAY[
            mention0.rx_class.spans,
            mention0.rx_status.spans,
            mention0.rx_treatment_phase.spans,
            mention0.rx_start_date.spans,
            mention0.rx_response.spans
        ], s -> s IS NOT NULL)))) AS VARCHAR), ':', CAST(ARRAY_MAX(FLATTEN(FLATTEN(FILTER(ARRAY[
            mention0.rx_class.spans,
            mention0.rx_status.spans,
            mention0.rx_treatment_phase.spans,
            mention0.rx_start_date.spans,
            mention0.rx_response.spans
        ], s -> s IS NOT NULL)))) AS VARCHAR)) AS span,
        -- Handle sublabel_name and sublabel_value per rx_treatment_phase
        'Medication Treatment Phase' AS sublabel_name,
        CAST(mention0.rx_treatment_phase.rx_treatment_phase AS VARCHAR) AS sublabel_value
    FROM
        ibd__nlp_treatment_gpt_oss_120b AS nlp,
        UNNEST(nlp.result.rx_annotations) AS t_mention0(mention0)
    -- Custom filtering per mention_key
    WHERE
        nlp.task_version IN (7)
        AND mention0.rx_treatment_phase IS NOT NULL
        AND mention0.rx_treatment_phase.rx_treatment_phase != 'NONE_OF_THE_ABOVE'
        -- Drop mentions where every merged span array was NULL or empty. This guard is
        -- load-bearing and must stay on the raw bound rather than the concatenated span:
        -- CONCAT(NULL, ':', NULL) is NULL on Trino but ':' on DuckDB, so filtering on the
        -- assembled span would let junk rows through on one backend and not the other.
        AND ARRAY_MIN(FLATTEN(FLATTEN(FILTER(ARRAY[
            mention0.rx_class.spans,
            mention0.rx_status.spans,
            mention0.rx_treatment_phase.spans,
            mention0.rx_start_date.spans,
            mention0.rx_response.spans
        ], s -> s IS NOT NULL)))) IS NOT NULL
    UNION ALL
    SELECT
        nlp.note_ref,
        nlp.subject_ref,
        nlp.generated_on,
        nlp.task_version,
        nlp.system_fingerprint,
        'ibd__nlp_treatment_gpt_oss_120b' AS origin,
        'Medication' AS label,
        -- Merge 5 parallel span arrays into one start:end span
        CONCAT(CAST(ARRAY_MIN(FLATTEN(FLATTEN(FILTER(ARRAY[
            mention0.rx_class.spans,
            mention0.rx_status.spans,
            mention0.rx_treatment_phase.spans,
            mention0.rx_start_date.spans,
            mention0.rx_response.spans
        ], s -> s IS NOT NULL)))) AS VARCHAR), ':', CAST(ARRAY_MAX(FLATTEN(FLATTEN(FILTER(ARRAY[
            mention0.rx_class.spans,
            mention0.rx_status.spans,
            mention0.rx_treatment_phase.spans,
            mention0.rx_start_date.spans,
            mention0.rx_response.spans
        ], s -> s IS NOT NULL)))) AS VARCHAR)) AS span,
        -- Handle sublabel_name and sublabel_value per rx_start_date
        'Medication Start Date' AS sublabel_name,
        CAST(mention0.rx_start_date.rx_start_date AS VARCHAR) AS sublabel_value
    FROM
        ibd__nlp_treatment_gpt_oss_120b AS nlp,
        UNNEST(nlp.result.rx_annotations) AS t_mention0(mention0)
    -- Custom filtering per mention_key
    WHERE
        nlp.task_version IN (7)
        AND mention0.rx_start_date IS NOT NULL
        AND mention0.rx_start_date.rx_start_date IS NOT NULL
        -- Drop mentions where every merged span array was NULL or empty. This guard is
        -- load-bearing and must stay on the raw bound rather than the concatenated span:
        -- CONCAT(NULL, ':', NULL) is NULL on Trino but ':' on DuckDB, so filtering on the
        -- assembled span would let junk rows through on one backend and not the other.
        AND ARRAY_MIN(FLATTEN(FLATTEN(FILTER(ARRAY[
            mention0.rx_class.spans,
            mention0.rx_status.spans,
            mention0.rx_treatment_phase.spans,
            mention0.rx_start_date.spans,
            mention0.rx_response.spans
        ], s -> s IS NOT NULL)))) IS NOT NULL
    UNION ALL
    SELECT
        nlp.note_ref,
        nlp.subject_ref,
        nlp.generated_on,
        nlp.task_version,
        nlp.system_fingerprint,
        'ibd__nlp_treatment_gpt_oss_120b' AS origin,
        'Medication' AS label,
        -- Merge 5 parallel span arrays into one start:end span
        CONCAT(CAST(ARRAY_MIN(FLATTEN(FLATTEN(FILTER(ARRAY[
            mention0.rx_class.spans,
            mention0.rx_status.spans,
            mention0.rx_treatment_phase.spans,
            mention0.rx_start_date.spans,
            mention0.rx_response.spans
        ], s -> s IS NOT NULL)))) AS VARCHAR), ':', CAST(ARRAY_MAX(FLATTEN(FLATTEN(FILTER(ARRAY[
            mention0.rx_class.spans,
            mention0.rx_status.spans,
            mention0.rx_treatment_phase.spans,
            mention0.rx_start_date.spans,
            mention0.rx_response.spans
        ], s -> s IS NOT NULL)))) AS VARCHAR)) AS span,
        -- Handle sublabel_name and sublabel_value per rx_start_date
        'Medication Start Date Precision' AS sublabel_name,
        CAST(mention0.rx_start_date.rx_start_date_precision AS VARCHAR) AS sublabel_value
    FROM
        ibd__nlp_treatment_gpt_oss_120b AS nlp,
        UNNEST(nlp.result.rx_annotations) AS t_mention0(mention0)
    -- Custom filtering per mention_key
    WHERE
        nlp.task_version IN (7)
        AND mention0.rx_start_date IS NOT NULL
        AND mention0.rx_start_date.rx_start_date_precision IS NOT NULL
        -- Drop mentions where every merged span array was NULL or empty. This guard is
        -- load-bearing and must stay on the raw bound rather than the concatenated span:
        -- CONCAT(NULL, ':', NULL) is NULL on Trino but ':' on DuckDB, so filtering on the
        -- assembled span would let junk rows through on one backend and not the other.
        AND ARRAY_MIN(FLATTEN(FLATTEN(FILTER(ARRAY[
            mention0.rx_class.spans,
            mention0.rx_status.spans,
            mention0.rx_treatment_phase.spans,
            mention0.rx_start_date.spans,
            mention0.rx_response.spans
        ], s -> s IS NOT NULL)))) IS NOT NULL
    UNION ALL
    SELECT
        nlp.note_ref,
        nlp.subject_ref,
        nlp.generated_on,
        nlp.task_version,
        nlp.system_fingerprint,
        'ibd__nlp_treatment_gpt_oss_120b' AS origin,
        'Medication' AS label,
        -- Merge 5 parallel span arrays into one start:end span
        CONCAT(CAST(ARRAY_MIN(FLATTEN(FLATTEN(FILTER(ARRAY[
            mention0.rx_class.spans,
            mention0.rx_status.spans,
            mention0.rx_treatment_phase.spans,
            mention0.rx_start_date.spans,
            mention0.rx_response.spans
        ], s -> s IS NOT NULL)))) AS VARCHAR), ':', CAST(ARRAY_MAX(FLATTEN(FLATTEN(FILTER(ARRAY[
            mention0.rx_class.spans,
            mention0.rx_status.spans,
            mention0.rx_treatment_phase.spans,
            mention0.rx_start_date.spans,
            mention0.rx_response.spans
        ], s -> s IS NOT NULL)))) AS VARCHAR)) AS span,
        -- Handle sublabel_name and sublabel_value per rx_response
        'Medication Response' AS sublabel_name,
        CAST(mention0.rx_response.rx_response AS VARCHAR) AS sublabel_value
    FROM
        ibd__nlp_treatment_gpt_oss_120b AS nlp,
        UNNEST(nlp.result.rx_annotations) AS t_mention0(mention0)
    -- Custom filtering per mention_key
    WHERE
        nlp.task_version IN (7)
        AND mention0.rx_response IS NOT NULL
        AND mention0.rx_response.rx_response != 'NONE_OF_THE_ABOVE'
        -- Drop mentions where every merged span array was NULL or empty. This guard is
        -- load-bearing and must stay on the raw bound rather than the concatenated span:
        -- CONCAT(NULL, ':', NULL) is NULL on Trino but ':' on DuckDB, so filtering on the
        -- assembled span would let junk rows through on one backend and not the other.
        AND ARRAY_MIN(FLATTEN(FLATTEN(FILTER(ARRAY[
            mention0.rx_class.spans,
            mention0.rx_status.spans,
            mention0.rx_treatment_phase.spans,
            mention0.rx_start_date.spans,
            mention0.rx_response.spans
        ], s -> s IS NOT NULL)))) IS NOT NULL
    UNION ALL
    SELECT
        nlp.note_ref,
        nlp.subject_ref,
        nlp.generated_on,
        nlp.task_version,
        nlp.system_fingerprint,
        'ibd__nlp_treatment_gpt_oss_120b' AS origin,
        'Adverse Drug Event' AS label,
        -- Turn the span array into a string representation of the start and end indices
        CONCAT(CAST(span[1] AS VARCHAR), ':', CAST(span[2] AS VARCHAR)) AS span,
        -- Handle sublabel_name and sublabel_value per ade
        'Adverse Drug Event' AS sublabel_name,
        CAST(mention1.ade AS VARCHAR) AS sublabel_value
    FROM
        ibd__nlp_treatment_gpt_oss_120b AS nlp,
        UNNEST(nlp.result.rx_annotations) AS t_mention0(mention0),
        UNNEST(mention0.rx_adverse_drug_events) AS t_mention1(mention1),
        UNNEST(mention1.spans) AS t_span(span)
    -- Custom filtering per mention_key
    WHERE
        nlp.task_version IN (7)
        AND mention1.ade IS NOT NULL
        AND mention1.ade != 'NONE_OF_THE_ABOVE'
    
