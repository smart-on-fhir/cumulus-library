-- noqa: disable=all
-- This sql was autogenerated as a reference example using the library CLI.
-- Its format is tied to the specific database it was run against, and it may not
-- be correct for all databases. Use the CLI's build option to derive the best SQL
-- for your dataset.
CREATE TABLE core__medicationrequest_dn_category AS (
    WITH

    system_category_0 AS (
        SELECT DISTINCT
            s.id AS id,
            u.codeable_concept.code AS code,
            u.codeable_concept.display AS display,
            u.codeable_concept.system AS code_system
        FROM
            medicationrequest AS s,
            UNNEST(s.category) AS cc (cc_row),
            UNNEST(cc.cc_row.coding) AS u (codeable_concept)
    ), --noqa: LT07

    union_table AS (
        SELECT
            id,
            code_system,
            code,
            display
        FROM system_category_0
    )
    SELECT
        id,
        code,
        code_system,
        display
    FROM union_table
);


-- ###########################################################



CREATE TABLE core__medicationrequest AS
WITH temp_mr AS (
    SELECT
        mr.id,
        mr.status,
        mr.intent,
        date(from_iso8601_timestamp(mr.authoredon)) AS authoredon,
        date_trunc('month', date(from_iso8601_timestamp(mr.authoredon))) 
            AS authoredon_month,
        NULL AS display,
        mr.subject.reference AS subject_ref,
        cm.code AS rx_code,
        cm.code_system AS rx_code_system,
        cm.display AS rx_display,
        mrc.code AS category_code,
        mrc.code_system AS category_code_system
    FROM medicationrequest AS mr
    INNER JOIN core__medication AS cm ON mr.id = cm.id
    LEFT JOIN core__medicationrequest_dn_category AS mrc ON mr.id = mrc.id
    WHERE cm.code_system = 'http://www.nlm.nih.gov/research/umls/rxnorm'
)

SELECT
    id,
    status,
    intent,
    authoredon,
    authoredon_month,
    category_code,
    category_code_system,
    rx_code_system,
    rx_code,
    rx_display,
    subject_ref
FROM temp_mr