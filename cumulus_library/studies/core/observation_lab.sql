-- ############################################################
-- FHIR Observation
-- http://hl7.org/fhir/us/core/StructureDefinition/us-core-observation-lab

--Each Observation must have:

--a status
--a category code of ‘laboratory’
--a LOINC code, if available, which tells you what is being measured
--a patient
--Each Observation must support:
--
--a time indicating when the measurement was taken
--a result value or a reason why the data is absent*
--if the result value is a numeric quantity, a standard UCUM unit


CREATE TABLE core__observation_lab AS
WITH temp_observation_lab AS (
    SELECT
        category_row.code AS category,
        o.status,
        o.code,
        o.valuecodeableconcept,
        subject.reference AS subject_ref,
        encounter.reference AS encounter_ref,
        o.id AS observation_id,
        date(from_iso8601_timestamp(o.effectivedatetime)) AS effectivedatetime,
        concat('Observation/', o.id) AS observation_ref
    -- , valueQuantity
    FROM observation AS o,
        unnest(category) AS t_category(observation_category), --noqa
        unnest(observation_category.coding) AS t_coding(category_row) --noqa
    WHERE category_row.code = 'laboratory'
)

SELECT
    tol.category,
    t_coding.code_row AS lab_code,
    t_vcc.value_concept_row AS lab_result,
    date_trunc('day', date(tol.effectivedatetime)) AS lab_date,
    date_trunc('week', date(tol.effectivedatetime)) AS lab_week,
    date_trunc('month', date(tol.effectivedatetime)) AS lab_month,
    date_trunc('year', date(tol.effectivedatetime)) AS lab_year,
    tol.subject_ref,
    tol.encounter_ref,
    tol.observation_id,
    tol.observation_ref
FROM temp_observation_lab AS tol,
    unnest(code.coding) AS t_coding (code_row),
    unnest(valuecodeableconcept.coding) AS t_vcc (value_concept_row)
WHERE tol.effectivedatetime BETWEEN date('2016-06-01') AND current_date;

-- ###########################################################################
-- COUNT Patients, Encounters, Lab Results
-- ###########################################################################
CREATE TABLE core__count_observation_lab_month AS
WITH powerset AS (
    SELECT
        count(DISTINCT o.subject_ref) AS cnt_subject,
        count(DISTINCT o.encounter_ref) AS cnt_encounter,
        count(DISTINCT o.observation_id) AS cnt_observation,
        o.lab_month,
        o.lab_code,
        o.lab_result,
        e.enc_class
    FROM core__observation_lab AS o, core__encounter AS e
    WHERE o.encounter_ref = e.encounter_ref
    GROUP BY cube(o.lab_month, o.lab_code, o.lab_result, e.enc_class)
)

SELECT
    powerset.cnt_observation AS cnt,
    powerset.lab_month,
    lab_code.code AS lab_code,
    lab_result.display AS lab_result_display,
    enc_class.code AS enc_class_code
FROM powerset
WHERE powerset.cnt_subject >= 10
ORDER BY powerset.cnt_observation DESC;
