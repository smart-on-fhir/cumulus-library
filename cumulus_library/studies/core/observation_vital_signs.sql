-- ############################################################
-- FHIR Observation Vital Signs
-- https://terminology.hl7.org/5.1.0/CodeSystem-observation-category.html#observation-category-vital-signs
-- https://build.fhir.org/observation-vitalsigns.html

--Each Observation must have:

--a status
--a category code of ‘vital-signs’
--a LOINC code, if available, which tells you what is being measured
--a patient
--Each Observation must support:
--
--a time indicating when the measurement was taken
--a result value or a reason why the data is absent*
--if the result value is a numeric quantity, a standard UCUM unit

CREATE TABLE core__observation_vital_signs AS
SELECT
    co.category,
    co.component,
    co.status,
    co.obs_code,
    co.interpretation,
    co.referencerange,
    co.valuequantity,
    co.valuecodeableconcept,
    co.obs_date,
    co.obs_week,
    co.obs_month,
    co.obs_year,
    co.subject_ref,
    co.encounter_ref,
    co.observation_id,
    co.observation_ref,
    t_obs.observation_category,
    t_cat.category_row
FROM core__observation AS co,
    UNNEST(category) AS t_obs (observation_category), --noqa: AL05
    UNNEST(observation_category.coding) AS t_cat (category_row) --noqa: AL05
WHERE category_row.code = 'vital-signs';
