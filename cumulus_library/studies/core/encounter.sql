-- #############################################################
-- Encounter
-- https://build.fhir.org/ig/HL7/US-Core/StructureDefinition-us-core-encounter.html

CREATE TABLE core__encounter AS


WITH temp_encounter AS (
    SELECT DISTINCT
        e.period,
        e.status,
        e.class,
        edt.code AS type_code,
        edt.code_system AS type_code_system,
        eds.code AS sevicetype_code,
        eds.code_system AS sevicetype_code_system,
        edp.code AS priority_code,
        edp.code_system AS priority_code_system,
        edr.code AS reasoncode_code,
        edr.code_system AS reasoncode_code_system,
        e.subject.reference AS subject_ref,
        e.id AS encounter_id,
        date(from_iso8601_timestamp(e.period."start")) AS start_date,
        date(from_iso8601_timestamp(e.period."end")) AS end_date,
        concat('Encounter/', e.id) AS encounter_ref
    FROM encounter AS e
    LEFT JOIN core__encounter_dn_priority AS edt ON e.id = edt.id
    LEFT JOIN core__encounter_dn_servicetype AS eds ON e.id = eds.id
    LEFT JOIN core__encounter_dn_priority AS edp ON e.id = edp.id
    LEFT JOIN core__encounter_dn_reasoncode AS edr ON e.id = edr.id
)

SELECT DISTINCT
    e.class AS enc_class,
    ac.code AS enc_class_code,
    ac.display AS enc_class_display,
    e.status,
    e.type_code,
    e.type_code_system,
    e.sevicetype_code,
    e.sevicetype_code_system,
    e.priority_code,
    e.priority_code_system,
    e.reasoncode_code,
    e.reasoncode_code_system,
    date_diff('year', date(p.birthdate), e.start_date) AS age_at_visit,
    date_trunc('day', e.start_date) AS start_date,
    date_trunc('day', e.end_date) AS end_date,
    date_trunc('week', e.start_date) AS start_week,
    date_trunc('month', e.start_date) AS start_month,
    date_trunc('year', e.start_date) AS start_year,
    e.subject_ref,
    e.encounter_ref,
    e.encounter_id,
    p.gender,
    p.race_display,
    p.ethnicity_display,
    p.postalcode3
FROM temp_encounter AS e
LEFT JOIN core__fhir_mapping_expected_act_encounter_code_v3 AS eac
    ON eac.found = e.class.code
LEFT JOIN core__fhir_act_encounter_code_v3 AS ac ON eac.expected = ac.code
INNER JOIN core__patient AS p ON e.subject_ref = p.subject_ref
WHERE
    start_date BETWEEN date('2016-06-01') AND current_date;
