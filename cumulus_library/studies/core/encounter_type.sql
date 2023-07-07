-- ############################################################################
-- # Encounter.Type, Encounter.serviceType, Encounter.Priority
--
-- use is OPTIONAL

CREATE TABLE core__encounter_type AS WITH
join_enc_type AS (
    SELECT DISTINCT
        e.encounter_id,
        t.as_coding
    FROM core__encounter AS e,
        unnest(enc_type) AS tr (as_row), --noqa: AL05
        unnest(tr.as_row.coding) AS t (as_coding) --noqa: AL05
),

join_service AS (
    SELECT DISTINCT
        e.encounter_id,
        t.as_coding
    FROM core__encounter AS e,
        unnest(service_type.coding) AS t (as_coding) --noqa: AL05
),

join_priority AS (
    SELECT DISTINCT
        e.encounter_id,
        t.as_coding
    FROM core__encounter AS e,
        unnest(priority.coding) AS t (as_coding) --noqa: AL05
)

SELECT DISTINCT
    e.enc_class_code,
    e.enc_class_display,
    coalesce(join_enc_type.as_coding.system, 'None') AS enc_type_system,
    coalesce(join_enc_type.as_coding.code, 'None') AS enc_type_code,
    coalesce(join_enc_type.as_coding.display, 'None') AS enc_type_display,
    coalesce(join_service.as_coding.system, 'None') AS enc_service_system,
    coalesce(join_service.as_coding.code, 'None') AS enc_service_code,
    coalesce(join_service.as_coding.display, 'None') AS enc_service_display,
    coalesce(join_priority.as_coding.system, 'None') AS enc_priority_system,
    coalesce(join_priority.as_coding.code, 'None') AS enc_priority_code,
    coalesce(join_priority.as_coding.display, 'None') AS enc_priority_display,
    e.reason_code,
    e.age_at_visit,
    e.start_date,
    e.end_date,
    e.start_week,
    e.start_month,
    e.start_year,
    e.subject_ref,
    e.encounter_ref,
    e.encounter_id,
    e.gender,
    e.race_display,
    e.ethnicity_display,
    e.postalcode3
FROM core__encounter AS e
LEFT JOIN join_enc_type ON e.encounter_id = join_enc_type.encounter_id
LEFT JOIN join_service ON e.encounter_id = join_service.encounter_id
LEFT JOIN join_priority ON e.encounter_id = join_priority.encounter_id;
