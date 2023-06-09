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

drop table if exists core__observation_vital_signs;

create TABLE core__observation_vital_signs as
    SELECT * from core__observation
        ,UNNEST(category) t (observation_category)
        ,UNNEST(observation_category.coding) t (category_row)
    WHERE category_row.code = 'vital-signs'
;
