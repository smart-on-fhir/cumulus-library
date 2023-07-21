-- ###########################################################
CREATE TABLE core__count_patient AS
WITH powerset AS (
    SELECT
        count(DISTINCT subject_ref) AS cnt_subject,

        age,
        gender,
        race_display,
        ethnicity_display
    FROM core__patient
    GROUP BY cube(age, gender, race_display, ethnicity_display)
)

SELECT
    cnt_subject AS cnt,
    age,
    gender,
    race_display,
    ethnicity_display
FROM powerset
WHERE cnt_subject >= 10;

-- ###########################################################
CREATE TABLE core__count_encounter_month AS
WITH powerset AS (
    SELECT
        count(DISTINCT subject_ref) AS cnt_subject,
        count(DISTINCT encounter_ref) AS cnt_encounter,
        start_month,
        enc_class_display,
        age_at_visit,
        gender,
        race_display,
        ethnicity_display
    FROM core__encounter
    GROUP BY
        cube(
            start_month,
            enc_class_display,
            age_at_visit,
            gender,
            race_display,
            ethnicity_display
        )
)

SELECT
    cnt_encounter AS cnt,
    start_month,
    enc_class_display,
    age_at_visit,
    gender,
    race_display,
    ethnicity_display
FROM powerset
WHERE cnt_subject >= 10;

-- ###########################################################
CREATE TABLE core__count_encounter_type AS
WITH powerset AS (
    SELECT
        count(DISTINCT subject_ref) AS cnt_subject,
        count(DISTINCT encounter_ref) AS cnt_encounter,
        enc_class_display,
        enc_type_display,
        enc_service_display,
        enc_priority_display
    FROM core__encounter_type
    GROUP BY
        cube(
            enc_class_display,
            enc_type_display,
            enc_service_display,
            enc_priority_display
        )
)

SELECT
    cnt_encounter AS cnt,
    enc_class_display,
    enc_type_display,
    enc_service_display,
    enc_priority_display
FROM powerset
WHERE cnt_subject >= 10;

-- ###########################################################
CREATE TABLE core__count_encounter_type_month AS
WITH powerset AS (
    SELECT
        count(DISTINCT subject_ref) AS cnt_subject,
        count(DISTINCT encounter_ref) AS cnt_encounter,
        enc_class_display,
        enc_type_display,
        enc_service_display,
        enc_priority_display,
        start_month
    FROM core__encounter_type
    GROUP BY
        cube(
            enc_class_display,
            enc_type_display,
            enc_service_display,
            enc_priority_display,
            start_month
        )
)

SELECT
    cnt_encounter AS cnt,
    enc_class_display,
    enc_type_display,
    enc_service_display,
    enc_priority_display,
    start_month
FROM powerset
WHERE cnt_subject >= 10;


-- ###########################################################
CREATE TABLE core__count_encounter_enc_type_month AS
WITH powerset AS (
    SELECT
        count(DISTINCT subject_ref) AS cnt_subject,
        count(DISTINCT encounter_ref) AS cnt_encounter,
        enc_class_display,
        enc_type_display,
        start_month
    FROM core__encounter_type
    GROUP BY cube(enc_class_display, enc_type_display, start_month)
)

SELECT
    cnt_encounter AS cnt,
    enc_class_display,
    enc_type_display,
    start_month
FROM powerset
WHERE cnt_subject >= 10;

-- ###########################################################
CREATE TABLE core__count_encounter_service_month AS
WITH powerset AS (
    SELECT
        count(DISTINCT subject_ref) AS cnt_subject,
        count(DISTINCT encounter_ref) AS cnt_encounter,
        enc_class_display,
        enc_service_display,
        start_month
    FROM core__encounter_type
    GROUP BY cube(enc_class_display, enc_service_display, start_month)
)

SELECT
    cnt_encounter AS cnt,
    enc_class_display,
    enc_service_display,
    start_month
FROM powerset
WHERE cnt_subject >= 10;

-- ###########################################################
CREATE TABLE core__count_encounter_priority_month AS
WITH powerset AS (
    SELECT
        count(DISTINCT subject_ref) AS cnt_subject,
        count(DISTINCT encounter_ref) AS cnt_encounter,
        enc_class_display,
        enc_priority_display,
        start_month
    FROM core__encounter_type
    GROUP BY cube(enc_class_display, enc_priority_display, start_month)
)

SELECT
    cnt_encounter AS cnt,
    enc_class_display,
    enc_priority_display,
    start_month
FROM powerset
WHERE cnt_subject >= 10;
