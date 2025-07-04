-- noqa: disable=all
-- This sql was autogenerated as a reference example using the library
-- CLI. Its format is tied to the specific database it was run against,
-- and it may not be correct for all databases. Use the CLI's build 
-- option to derive the best SQL for your dataset.

-- ###########################################################

CREATE TABLE core__observation_dn_category AS (
    WITH

    flattened_rows AS (
        WITH
        data_and_row_num AS (
            SELECT
                t.id AS id,
                generate_subscripts(t."category", 1) AS row,
                UNNEST(t."category") AS "category" -- must unnest in SELECT here
            FROM observation AS t
        )
        SELECT
            id,
            row,
            "category"
        FROM data_and_row_num
    ),

    system_category_0 AS (
        SELECT DISTINCT
            s.id AS id,
            s.row,
            u.coding.code,
            u.coding.display,
            u.coding.system,
            u.coding.userSelected
        FROM
            flattened_rows AS s,
            UNNEST(s.category.coding) AS u (coding)
    ), --noqa: LT07

    union_table AS (
        SELECT
            id,
            row,
            system,
            code,
            display,
            userSelected
        FROM system_category_0
        
    )
    SELECT
        id,
        row,
        code,
        system,
        display,
        userSelected
    FROM union_table
);


-- ###########################################################

CREATE TABLE core__observation_dn_code AS (
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
            observation AS s,
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

CREATE TABLE core__observation_component_code AS (
    WITH

    flattened_rows AS (
        WITH
        data_and_row_num AS (
            SELECT
                t.id AS id,
                generate_subscripts(t."component", 1) AS row,
                UNNEST(t."component") AS data -- must unnest in SELECT here
            FROM observation AS t
        )
        SELECT
            id,
            row,
            data."code"
        FROM data_and_row_num
    ),

    system_code_0 AS (
        SELECT DISTINCT
            s.id AS id,
            s.row,
            u.coding.code,
            u.coding.display,
            u.coding.system,
            u.coding.userSelected
        FROM
            flattened_rows AS s,
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
        row,
        code,
        system,
        display,
        userSelected
    FROM union_table
);


-- ###########################################################

CREATE TABLE core__observation_component_dataabsentreason AS (
    WITH

    flattened_rows AS (
        WITH
        data_and_row_num AS (
            SELECT
                t.id AS id,
                generate_subscripts(t."component", 1) AS row,
                UNNEST(t."component") AS data -- must unnest in SELECT here
            FROM observation AS t
        )
        SELECT
            id,
            row,
            data."dataabsentreason"
        FROM data_and_row_num
    ),

    system_dataabsentreason_0 AS (
        SELECT DISTINCT
            s.id AS id,
            s.row,
            u.coding.code,
            u.coding.display,
            u.coding.system,
            u.coding.userSelected
        FROM
            flattened_rows AS s,
            UNNEST(s.dataabsentreason.coding) AS u (coding)
    ), --noqa: LT07

    union_table AS (
        SELECT
            id,
            row,
            system,
            code,
            display,
            userSelected
        FROM system_dataabsentreason_0
        
    )
    SELECT
        id,
        row,
        code,
        system,
        display,
        userSelected
    FROM union_table
);


-- ###########################################################

CREATE TABLE core__observation_component_interpretation AS (
    WITH

    flattened_rows AS (
        WITH
        data_and_row_num AS (
            SELECT
                t.id AS id,
                generate_subscripts(t."component", 1) AS row,
                UNNEST(t."component") AS data -- must unnest in SELECT here
            FROM observation AS t
        )
        SELECT
            id,
            row,
            data."interpretation"
        FROM data_and_row_num
    ),

    child_flattened_rows AS (
        SELECT DISTINCT
            s.id,
            s.row, -- keep the parent row number
            u."interpretation"
        FROM
            flattened_rows AS s,
            UNNEST(s.interpretation) AS u ("interpretation")
    ),

    system_interpretation_0 AS (
        SELECT DISTINCT
            s.id AS id,
            s.row,
            u.coding.code,
            u.coding.display,
            u.coding.system,
            u.coding.userSelected
        FROM
            child_flattened_rows AS s,
            UNNEST(s.interpretation.coding) AS u (coding)
    ), --noqa: LT07

    union_table AS (
        SELECT
            id,
            row,
            system,
            code,
            display,
            userSelected
        FROM system_interpretation_0
        
    )
    SELECT
        id,
        row,
        code,
        system,
        display,
        userSelected
    FROM union_table
);


-- ###########################################################

CREATE TABLE IF NOT EXISTS "main"."core__observation_component_valuecodeableconcept"
AS (
    SELECT * FROM (
        VALUES
        (cast(NULL AS varchar),cast(NULL AS bigint),cast(NULL AS varchar),cast(NULL AS varchar),cast(NULL AS varchar),cast(NULL AS boolean))
    )
        AS t ("id","row","code","system","display","userSelected")
    WHERE 1 = 0 -- ensure empty table
);

-- ###########################################################

CREATE TABLE IF NOT EXISTS "main"."core__observation_dn_interpretation"
AS (
    SELECT * FROM (
        VALUES
        (cast(NULL AS varchar),cast(NULL AS bigint),cast(NULL AS varchar),cast(NULL AS varchar),cast(NULL AS varchar),cast(NULL AS boolean))
    )
        AS t ("id","row","code","system","display","userSelected")
    WHERE 1 = 0 -- ensure empty table
);

-- ###########################################################

CREATE TABLE core__observation_dn_valuecodeableconcept AS (
    WITH

    system_valuecodeableconcept_0 AS (
        SELECT DISTINCT
            s.id AS id,
            0 AS row,
            u.coding.code,
            u.coding.display,
            u.coding.system,
            u.coding.userSelected
        FROM
            observation AS s,
            UNNEST(s.valuecodeableconcept.coding) AS u (coding)
    ), --noqa: LT07

    union_table AS (
        SELECT
            id,
            row,
            system,
            code,
            display,
            userSelected
        FROM system_valuecodeableconcept_0
        
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

CREATE TABLE IF NOT EXISTS "main"."core__observation_dn_dataabsentreason"
AS (
    SELECT * FROM (
        VALUES
        (cast(NULL AS varchar),cast(NULL AS bigint),cast(NULL AS varchar),cast(NULL AS varchar),cast(NULL AS varchar),cast(NULL AS boolean))
    )
        AS t ("id","row","code","system","display","userSelected")
    WHERE 1 = 0 -- ensure empty table
);

-- ###########################################################



CREATE TABLE core__observation AS
WITH temp_observation AS (
    SELECT
        o.id,
        o.status,
        o.encounter.reference AS encounter_ref,
        o.subject.reference AS subject_ref,
        o.valueString,
        o.valueQuantity.value AS valueQuantity_value,
        o.valueQuantity.comparator AS valueQuantity_comparator,
        o.valueQuantity.unit AS valueQuantity_unit,
        o.valueQuantity.system AS valueQuantity_system,
        o.valueQuantity.code AS valueQuantity_code,
        date_trunc('day', cast(from_iso8601_timestamp(o."effectiveDateTime") AS date))
            AS effectiveDateTime_day,
        date_trunc('week', cast(from_iso8601_timestamp(o."effectiveDateTime") AS date))
            AS effectiveDateTime_week,
        date_trunc('month', cast(from_iso8601_timestamp(o."effectiveDateTime") AS date))
            AS effectiveDateTime_month,
        date_trunc('year', cast(from_iso8601_timestamp(o."effectiveDateTime") AS date))
            AS effectiveDateTime_year,
        odc.code AS observation_code,
        odc.system AS observation_system,
        odcat.code AS category_code,
        odcat.system AS category_system,
        odi.code AS interpretation_code,
        odi.system AS interpretation_system,
        odi.display AS interpretation_display,
        odvcc.code AS valueCodeableConcept_code,
        odvcc.system AS valueCodeableConcept_system,
        odvcc.display AS valueCodeableConcept_display,
        odda.code AS dataAbsentReason_code,
        odda.system AS dataAbsentReason_system,
        odda.display AS dataAbsentReason_display
    FROM observation AS o
    LEFT JOIN core__observation_dn_category AS odcat ON o.id = odcat.id
    LEFT JOIN core__observation_dn_code AS odc ON o.id = odc.id
    LEFT JOIN core__observation_dn_interpretation AS odi ON o.id = odi.id
    LEFT JOIN core__observation_dn_valuecodeableconcept AS odvcc ON o.id = odvcc.id
    LEFT JOIN core__observation_dn_dataabsentreason AS odda ON o.id = odda.id
)

SELECT
    id,
    category_code,
    category_system,
    status,
    observation_code,
    observation_system,
    interpretation_code,
    interpretation_system,
    interpretation_display,
    effectiveDateTime_day,
    effectiveDateTime_week,
    effectiveDateTime_month,
    effectiveDateTime_year,
    valueCodeableConcept_code,
    valueCodeableConcept_system,
    valueCodeableConcept_display,
    valueQuantity_value,
    valueQuantity_comparator,
    valueQuantity_unit,
    valueQuantity_system,
    valueQuantity_code,
    valueString,
    dataAbsentReason_code,
    dataAbsentReason_system,
    dataAbsentReason_display,
    subject_ref,
    encounter_ref,
    concat('Observation/', id) AS observation_ref
FROM temp_observation;

-- ###########################################################




CREATE TABLE core__observation_component_valuequantity AS (
    SELECT
        'x' AS id,
        CAST(NULL AS BIGINT) AS row,
        CAST(NULL AS DOUBLE) AS value, -- noqa: disable=L029
        'x' AS comparator,
        'x' AS unit,
        'x' AS system,
        'x' AS code
    WHERE 1=0 -- empty table
);
