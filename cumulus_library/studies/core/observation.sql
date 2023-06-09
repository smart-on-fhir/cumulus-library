drop table if exists core__observation;

create TABLE core__observation as
with temp_observation as (
    SELECT
          category
        , component
        , status
        , code_row as obs_code
        , code
        , interpretation
        , referenceRange
        , valueQuantity
        , valueCodeableConcept
        , subject.reference as subject_ref
        , encounter.reference as encounter_ref
        , date(from_iso8601_timestamp(effectiveDateTime)) as effectiveDateTime
        , id as observation_id, concat('Observation/', id) as observation_ref
    FROM observation
        ,UNNEST(code.coding) t (code_row)
)
SELECT
      category
    , component
    , status
    , obs_code
    , interpretation
    , referenceRange
    , valueQuantity
    , valueCodeableConcept
    , date_trunc('day',   date(effectiveDateTime)) as obs_date
    , date_trunc('week',  date(effectiveDateTime)) as obs_week
    , date_trunc('month', date(effectiveDateTime)) as obs_month
    , date_trunc('year',  date(effectiveDateTime)) as obs_year
    , subject_ref
    , encounter_ref
    , observation_id, observation_ref
FROM temp_observation
WHERE effectiveDateTime between date('2016-06-01') and current_date
;
