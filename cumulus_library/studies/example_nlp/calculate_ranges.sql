CREATE TABLE example_nlp__range_labels AS
SELECT
    src.note_ref,
    src.subject_ref,
    CASE
        -- Based on MeSH age groups: https://www.ncbi.nlm.nih.gov/mesh/68009273
        WHEN src.result.age < 0 OR src.result.age > 120 THEN 'unknown'
        WHEN src.result.age < 2 THEN 'infant (0-1)'
        WHEN src.result.age < 13 THEN 'child (2-12)'
        WHEN src.result.age < 19 THEN 'adolescent (13-18)'
        WHEN src.result.age < 25 THEN 'young adult (19-24)'
        WHEN src.result.age < 45 THEN 'adult (25-44)'
        WHEN src.result.age < 65 THEN 'middle aged (45-64)'
        ELSE 'aged (65+)'
    END AS label
FROM example_nlp__age AS src
