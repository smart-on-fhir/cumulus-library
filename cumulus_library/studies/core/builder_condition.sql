-- noqa: disable=all

/*
This is a reference output of the SQL generated by builder_condition.py 
that is used by the core__encounter_type table. It is not invoked directly.
*/
CREATE TABLE core__condition_codable_concepts_display AS (
    WITH

    system_code_0 AS (
        SELECT DISTINCT
            s.id AS id,
            '0' AS priority,
            u.codeable_concept.code AS code,
            u.codeable_concept.display AS display,
            u.codeable_concept.system AS code_system
        FROM
            condition AS s,
            UNNEST(s.code.coding) AS u (codeable_concept)
        WHERE
            u.codeable_concept.system = 'http://snomed.info/sct'
    ), --noqa: LT07

    system_code_1 AS (
        SELECT DISTINCT
            s.id AS id,
            '1' AS priority,
            u.codeable_concept.code AS code,
            u.codeable_concept.display AS display,
            u.codeable_concept.system AS code_system
        FROM
            condition AS s,
            UNNEST(s.code.coding) AS u (codeable_concept)
        WHERE
            u.codeable_concept.system = 'http://hl7.org/fhir/sid/icd-10-cm'
    ), --noqa: LT07

    system_code_2 AS (
        SELECT DISTINCT
            s.id AS id,
            '2' AS priority,
            u.codeable_concept.code AS code,
            u.codeable_concept.display AS display,
            u.codeable_concept.system AS code_system
        FROM
            condition AS s,
            UNNEST(s.code.coding) AS u (codeable_concept)
        WHERE
            u.codeable_concept.system = 'http://hl7.org/fhir/sid/icd-9-cm'
    ), --noqa: LT07

    union_table AS (
        SELECT
            id,
            priority,
            code_system,
            code,
            display
        FROM system_code_0
        UNION
        SELECT
            id,
            priority,
            code_system,
            code,
            display
        FROM system_code_1
        UNION
        SELECT
            id,
            priority,
            code_system,
            code,
            display
        FROM system_code_2
    ),

    partitioned_table AS (
        SELECT
            id,
            code,
            code_system,
            display,
            priority,
            ROW_NUMBER()
                OVER (
                    PARTITION BY id
                    ORDER BY priority ASC
                ) AS available_priority
        FROM union_table
        GROUP BY id, priority, code_system, code, display
        ORDER BY priority ASC
    )

    SELECT
        id,
        code,
        code_system,
        display
    FROM partitioned_table
    WHERE available_priority = 1
);


-- ###########################################################
CREATE TABLE core__condition_codable_concepts_all AS (
    WITH

    system_code_0 AS (
        SELECT DISTINCT
            s.id AS id,
            u.codeable_concept.code AS code,
            u.codeable_concept.display AS display,
            u.codeable_concept.system AS code_system
        FROM
            condition AS s,
            UNNEST(s.code.coding) AS u (codeable_concept)
    ), --noqa: LT07

    union_table AS (
        SELECT
            id,
            code_system,
            code,
            display
        FROM system_code_0
    )
    SELECT
        id,
        code,
        code_system,
        display
    FROM union_table
);


-- ###########################################################


CREATE TABLE core__condition AS
WITH temp_condition AS (
    SELECT
        concat('Condition/', c.id) AS condition_ref,
        c.id,
        c.category,
        NULL AS verificationstatus,
        NULL AS clinicalstatus,
        c.subject.reference AS subject_ref,
        c.encounter.reference AS encounter_ref,
        cca.code,
        cca.code_system,
        cca.display,
        date(from_iso8601_timestamp(c.recordeddate)) AS recordeddate
    FROM condition AS c
    LEFT JOIN core__condition_codable_concepts_all AS cca ON c.id = cca.id
)

SELECT
    t_category_coding.category_row.code AS category_code,
    t_category_coding.category_row.display AS category_display,
    tc.code,
    tc.code_system,
    tc.display AS code_display,
    tc.subject_ref,
    tc.encounter_ref,
    tc.id as condition_id,
    tc.condition_ref,
    tc.recordeddate,
    date_trunc('week', date(tc.recordeddate)) AS recorded_week,
    date_trunc('month', date(tc.recordeddate)) AS recorded_month,
    date_trunc('year', date(tc.recordeddate)) AS recorded_year
FROM temp_condition AS tc,
    unnest(category) AS t_category (category_coding),
    unnest(category_coding.coding) AS t_category_coding (category_row)

WHERE tc.recordeddate BETWEEN date('2016-01-01') AND current_date;