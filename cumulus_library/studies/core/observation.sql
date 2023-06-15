CREATE TABLE core__observation AS
WITH temp_observation AS (
    SELECT
        o.category,
        o.component,
        o.status,
        t_coding.code_row AS obs_code,
        o.code,
        o.interpretation,
        o.referencerange,
        o.valuequantity,
        o.valuecodeableconcept,
        o.subject.reference AS subject_ref,
        o.encounter.reference AS encounter_ref,
        date(from_iso8601_timestamp(o.effectivedatetime)) AS effectivedatetime,
        o.id AS observation_id,
        concat('Observation/', o.id) AS observation_ref
    FROM observation AS o,
        unnest(code.coding) AS t_coding (code_row) --noqa: AL05
)

SELECT
    category,
    component,
    status,
    obs_code,
    interpretation,
    referencerange,
    valuequantity,
    valuecodeableconcept,
    date_trunc('day', date(effectivedatetime)) AS obs_date,
    date_trunc('week', date(effectivedatetime)) AS obs_week,
    date_trunc('month', date(effectivedatetime)) AS obs_month,
    date_trunc('year', date(effectivedatetime)) AS obs_year,
    subject_ref,
    encounter_ref,
    observation_id,
    observation_ref
FROM temp_observation
WHERE effectivedatetime BETWEEN date('2016-06-01') AND current_date;
