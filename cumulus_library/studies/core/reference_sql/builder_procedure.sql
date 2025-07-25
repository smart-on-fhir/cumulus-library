-- noqa: disable=all
-- This sql was autogenerated as a reference example using the library
-- CLI. Its format is tied to the specific database it was run against,
-- and it may not be correct for all databases. Use the CLI's build 
-- option to derive the best SQL for your dataset.

-- ###########################################################

CREATE TABLE IF NOT EXISTS "main"."core__procedure_dn_category"
AS (
    SELECT * FROM (
        VALUES
        (cast(NULL AS varchar),cast(NULL AS bigint),cast(NULL AS varchar),cast(NULL AS varchar),cast(NULL AS varchar),cast(NULL AS boolean))
    )
        AS t ("id","row","code","system","display","userSelected")
    WHERE 1 = 0 -- ensure empty table
);

-- ###########################################################

CREATE TABLE core__procedure_dn_code AS (
    WITH

    system_code_0 AS (
        SELECT DISTINCT
            s.id AS id,
            0 AS row,
            u.coding.code,
            u.coding.display,
            u.coding.system,
            u.coding.userSelected
        FROM
            procedure AS s,
            UNNEST(s.code.coding) AS u (coding)
    ), --noqa: LT07

    union_table AS (
        SELECT
            id,
            row,
            system,
            code,
            display,
            userSelected
        FROM system_code_0
        
    )
    SELECT
        id,
        code,
        system,
        display,
        userSelected
    FROM union_table
);


-- ###########################################################



-- This table includes all fields of interest to the US Core Procedure profile.
-- EXCEPT FOR:
-- * the 'performedAge' and 'performedRange' fields, simply because they are annoying to
--   represent and not frequently used. They aren't even marked as Must Support by the profile
--   (heck, neither is performedPeriod, but we include that since EHRs often like to use periods)
--
-- AND ADDING:
-- * the `category` field, because it's helpful for classification
-- * the `encounter` field, because come on, why is it left out of the US Core profile
--
-- There are lots of interesting possible fields to support from the base FHIR spec that aren't
-- in the US Core profile, like reasonCode, bodySite, and outcome. But EHR support seems low since
-- they aren't in the profile, so they have been left out so far.
--
-- US Core profile for reference:
-- * http://hl7.org/fhir/us/core/STU4/StructureDefinition-us-core-procedure.html

CREATE TABLE core__procedure AS
WITH temp_procedure AS (
    SELECT
        src.id,
        src.status,
        src.subject.reference AS subject_ref,
        src.encounter.reference AS encounter_ref,
        date_trunc('day', cast(from_iso8601_timestamp(src."performedDateTime") AS date))
            AS performedDateTime_day,
        date_trunc('week', cast(from_iso8601_timestamp(src."performedDateTime") AS date))
            AS performedDateTime_week,
        date_trunc('month', cast(from_iso8601_timestamp(src."performedDateTime") AS date))
            AS performedDateTime_month,
        date_trunc('year', cast(from_iso8601_timestamp(src."performedDateTime") AS date))
            AS performedDateTime_year,
        date_trunc('day', cast(from_iso8601_timestamp(src."performedPeriod"."start") AS date))
            AS performedPeriod_start_day,
        date_trunc('week', cast(from_iso8601_timestamp(src."performedPeriod"."start") AS date))
            AS performedPeriod_start_week,
        date_trunc('month', cast(from_iso8601_timestamp(src."performedPeriod"."start") AS date))
            AS performedPeriod_start_month,
        date_trunc('year', cast(from_iso8601_timestamp(src."performedPeriod"."start") AS date))
            AS performedPeriod_start_year,
        date_trunc('day', cast(from_iso8601_timestamp(src."performedPeriod"."end") AS date))
            AS performedPeriod_end_day,
        date_trunc('week', cast(from_iso8601_timestamp(src."performedPeriod"."end") AS date))
            AS performedPeriod_end_week,
        date_trunc('month', cast(from_iso8601_timestamp(src."performedPeriod"."end") AS date))
            AS performedPeriod_end_month,
        date_trunc('year', cast(from_iso8601_timestamp(src."performedPeriod"."end") AS date))
            AS performedPeriod_end_year
    FROM "procedure" AS src
)

SELECT
    tp.id,
    tp.status,

    dn_category.code AS category_code,
    dn_category.system AS category_system,
    dn_category.display AS category_display,

    dn_code.code AS code_code,
    dn_code.system AS code_system,
    dn_code.display AS code_display,

    tp.performedDateTime_day,
    tp.performedDateTime_week,
    tp.performedDateTime_month,
    tp.performedDateTime_year,

    tp.performedPeriod_start_day,
    tp.performedPeriod_start_week,
    tp.performedPeriod_start_month,
    tp.performedPeriod_start_year,

    tp.performedPeriod_end_day,
    tp.performedPeriod_end_week,
    tp.performedPeriod_end_month,
    tp.performedPeriod_end_year,

    concat('Procedure/', tp.id) AS procedure_ref,
    tp.subject_ref,
    tp.encounter_ref

FROM temp_procedure AS tp
LEFT JOIN core__procedure_dn_code AS dn_code ON tp.id = dn_code.id
LEFT JOIN core__procedure_dn_category AS dn_category ON tp.id = dn_category.id;
