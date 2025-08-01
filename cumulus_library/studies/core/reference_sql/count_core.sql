-- noqa: disable=all
-- This sql was autogenerated as a reference example using the library
-- CLI. Its format is tied to the specific database it was run against,
-- and it may not be correct for all databases. Use the CLI's build 
-- option to derive the best SQL for your dataset.

-- ###########################################################

CREATE TABLE core__count_allergyintolerance_month AS (
    WITH
    filtered_table AS (
        SELECT
            s.patient_ref,
            --noqa: disable=RF03, AL02
            s."category",
            s."recordedDate_month",
            s."code_display",
            s."reaction_manifestation_display"
            --noqa: enable=RF03, AL02
        FROM core__allergyintolerance AS s
    ),
    
    null_replacement AS (
        SELECT
            patient_ref,
            coalesce(
                cast(category AS varchar),
                'cumulus__none'
            ) AS category,
            coalesce(
                cast(recordedDate_month AS varchar),
                'cumulus__none'
            ) AS recordedDate_month,
            coalesce(
                cast(code_display AS varchar),
                'cumulus__none'
            ) AS code_display,
            coalesce(
                cast(reaction_manifestation_display AS varchar),
                'cumulus__none'
            ) AS reaction_manifestation_display
        FROM filtered_table
    ),

    powerset AS (
        SELECT
            count(DISTINCT patient_ref) AS cnt_subject_ref,
            "category",
            "recordedDate_month",
            "code_display",
            "reaction_manifestation_display",
            concat_ws(
                '-',
                COALESCE("category",''),
                COALESCE("recordedDate_month",''),
                COALESCE("code_display",''),
                COALESCE("reaction_manifestation_display",'')
            ) AS id
        FROM null_replacement
        GROUP BY
            cube(
            "category",
            "recordedDate_month",
            "code_display",
            "reaction_manifestation_display"
            )
    )

    SELECT
        p.cnt_subject_ref AS cnt,
        p."category",
        p."recordedDate_month",
        p."code_display",
        p."reaction_manifestation_display"
    FROM powerset AS p
    WHERE 
        p.cnt_subject_ref >= 10
);

-- ###########################################################

CREATE TABLE core__count_condition_month AS (
    WITH
    filtered_table AS (
        SELECT
            s.subject_ref,
            s.encounter_ref,
            --noqa: disable=RF03, AL02
            s."category_code",
            s."recordedDate_month",
            s."code_display",
            s."code"
            --noqa: enable=RF03, AL02
        FROM core__condition AS s
    ),
    
    null_replacement AS (
        SELECT
            subject_ref,
            encounter_ref,
            coalesce(
                cast(category_code AS varchar),
                'cumulus__none'
            ) AS category_code,
            coalesce(
                cast(recordedDate_month AS varchar),
                'cumulus__none'
            ) AS recordedDate_month,
            coalesce(
                cast(code_display AS varchar),
                'cumulus__none'
            ) AS code_display,
            coalesce(
                cast(code AS varchar),
                'cumulus__none'
            ) AS code
        FROM filtered_table
    ),
    secondary_powerset AS (
        SELECT
            count(DISTINCT encounter_ref) AS cnt_encounter_ref,
            "category_code",
            "recordedDate_month",
            "code_display",
            "code",
            concat_ws(
                '-',
                COALESCE("category_code",''),
                COALESCE("recordedDate_month",''),
                COALESCE("code_display",''),
                COALESCE("code",'')
            ) AS id
        FROM null_replacement
        WHERE encounter_ref IS NOT NULL
        GROUP BY
            cube(
            "category_code",
            "recordedDate_month",
            "code_display",
            "code"
            )
    ),

    powerset AS (
        SELECT
            count(DISTINCT subject_ref) AS cnt_subject_ref,
            "category_code",
            "recordedDate_month",
            "code_display",
            "code",
            concat_ws(
                '-',
                COALESCE("category_code",''),
                COALESCE("recordedDate_month",''),
                COALESCE("code_display",''),
                COALESCE("code",'')
            ) AS id
        FROM null_replacement
        GROUP BY
            cube(
            "category_code",
            "recordedDate_month",
            "code_display",
            "code"
            )
    )

    SELECT
        s.cnt_encounter_ref AS cnt,
        p."category_code",
        p."recordedDate_month",
        p."code_display",
        p."code"
    FROM powerset AS p
    JOIN secondary_powerset AS s on s.id = p.id
    WHERE 
        p.cnt_subject_ref >= 10
        AND s.cnt_encounter_ref >= 10
);

-- ###########################################################

CREATE TABLE core__count_diagnosticreport_month AS (
    WITH
    filtered_table AS (
        SELECT
            s.subject_ref,
            --noqa: disable=RF03, AL02
            s."category_display",
            s."code_display",
            s."issued_month"
            --noqa: enable=RF03, AL02
        FROM core__diagnosticreport AS s
    ),
    
    null_replacement AS (
        SELECT
            subject_ref,
            coalesce(
                cast(category_display AS varchar),
                'cumulus__none'
            ) AS category_display,
            coalesce(
                cast(code_display AS varchar),
                'cumulus__none'
            ) AS code_display,
            coalesce(
                cast(issued_month AS varchar),
                'cumulus__none'
            ) AS issued_month
        FROM filtered_table
    ),

    powerset AS (
        SELECT
            count(DISTINCT subject_ref) AS cnt_subject_ref,
            "category_display",
            "code_display",
            "issued_month",
            concat_ws(
                '-',
                COALESCE("category_display",''),
                COALESCE("code_display",''),
                COALESCE("issued_month",'')
            ) AS id
        FROM null_replacement
        GROUP BY
            cube(
            "category_display",
            "code_display",
            "issued_month"
            )
    )

    SELECT
        p.cnt_subject_ref AS cnt,
        p."category_display",
        p."code_display",
        p."issued_month"
    FROM powerset AS p
    WHERE 
        p.cnt_subject_ref >= 10
);

-- ###########################################################

CREATE TABLE core__count_documentreference_month AS (
    WITH
    filtered_table AS (
        SELECT
            s.subject_ref,
            s.documentreference_ref,
            e.class_display,
            --noqa: disable=RF03, AL02
            s."type_display",
            s."author_month"
            --noqa: enable=RF03, AL02
        FROM core__documentreference AS s
        INNER JOIN core__encounter AS e
            ON s.encounter_ref = e.encounter_ref
        WHERE (s.status = 'current')
        AND (s.docStatus IS null OR s.docStatus IN ('final', 'amended'))
    ),
    
    null_replacement AS (
        SELECT
            subject_ref,
            documentreference_ref,
            coalesce(
                cast(class_display AS varchar), 
                'cumulus__none'
            ) AS class_display,
            coalesce(
                cast(type_display AS varchar),
                'cumulus__none'
            ) AS type_display,
            coalesce(
                cast(author_month AS varchar),
                'cumulus__none'
            ) AS author_month
        FROM filtered_table
    ),
    secondary_powerset AS (
        SELECT
            count(DISTINCT documentreference_ref) AS cnt_documentreference_ref,
            "type_display",
            "author_month",
            class_display
            ,
            concat_ws(
                '-',
                COALESCE("type_display",''),
                COALESCE("author_month",''),
                COALESCE(class_display,'')
                
            ) AS id
        FROM null_replacement
        WHERE documentreference_ref IS NOT NULL
        GROUP BY
            cube(
            "type_display",
            "author_month",
            class_display
            
            )
    ),

    powerset AS (
        SELECT
            count(DISTINCT subject_ref) AS cnt_subject_ref,
            "type_display",
            "author_month",
            class_display
            ,
            concat_ws(
                '-',
                COALESCE("type_display",''),
                COALESCE("author_month",''),
                COALESCE(class_display,'')
                
            ) AS id
        FROM null_replacement
        GROUP BY
            cube(
            "type_display",
            "author_month",
            class_display
            
            )
    )

    SELECT
        s.cnt_documentreference_ref AS cnt,
        p."type_display",
        p."author_month",
        p.class_display
    FROM powerset AS p
    JOIN secondary_powerset AS s on s.id = p.id
    WHERE 
        p.cnt_subject_ref >= 10
        AND s.cnt_documentreference_ref >= 10
);

-- ###########################################################

CREATE TABLE core__count_encounter_month AS (
    WITH
    filtered_table AS (
        SELECT
            s.subject_ref,
            s.encounter_ref,
            --noqa: disable=RF03, AL02
            s."period_start_month",
            s."class_display",
            s."age_at_visit",
            s."gender",
            s."race_display",
            s."ethnicity_display"
            --noqa: enable=RF03, AL02
        FROM core__encounter AS s
        WHERE s.status = 'finished'
    ),
    
    null_replacement AS (
        SELECT
            subject_ref,
            encounter_ref,
            coalesce(
                cast(period_start_month AS varchar),
                'cumulus__none'
            ) AS period_start_month,
            coalesce(
                cast(class_display AS varchar),
                'cumulus__none'
            ) AS class_display,
            coalesce(
                cast(age_at_visit AS varchar),
                'cumulus__none'
            ) AS age_at_visit,
            coalesce(
                cast(gender AS varchar),
                'cumulus__none'
            ) AS gender,
            coalesce(
                cast(race_display AS varchar),
                'cumulus__none'
            ) AS race_display,
            coalesce(
                cast(ethnicity_display AS varchar),
                'cumulus__none'
            ) AS ethnicity_display
        FROM filtered_table
    ),
    secondary_powerset AS (
        SELECT
            count(DISTINCT encounter_ref) AS cnt_encounter_ref,
            "period_start_month",
            "class_display",
            "age_at_visit",
            "gender",
            "race_display",
            "ethnicity_display",
            concat_ws(
                '-',
                COALESCE("period_start_month",''),
                COALESCE("class_display",''),
                COALESCE("age_at_visit",''),
                COALESCE("gender",''),
                COALESCE("race_display",''),
                COALESCE("ethnicity_display",'')
            ) AS id
        FROM null_replacement
        WHERE encounter_ref IS NOT NULL
        GROUP BY
            cube(
            "period_start_month",
            "class_display",
            "age_at_visit",
            "gender",
            "race_display",
            "ethnicity_display"
            )
    ),

    powerset AS (
        SELECT
            count(DISTINCT subject_ref) AS cnt_subject_ref,
            "period_start_month",
            "class_display",
            "age_at_visit",
            "gender",
            "race_display",
            "ethnicity_display",
            concat_ws(
                '-',
                COALESCE("period_start_month",''),
                COALESCE("class_display",''),
                COALESCE("age_at_visit",''),
                COALESCE("gender",''),
                COALESCE("race_display",''),
                COALESCE("ethnicity_display",'')
            ) AS id
        FROM null_replacement
        GROUP BY
            cube(
            "period_start_month",
            "class_display",
            "age_at_visit",
            "gender",
            "race_display",
            "ethnicity_display"
            )
    )

    SELECT
        s.cnt_encounter_ref AS cnt,
        p."period_start_month",
        p."class_display",
        p."age_at_visit",
        p."gender",
        p."race_display",
        p."ethnicity_display"
    FROM powerset AS p
    JOIN secondary_powerset AS s on s.id = p.id
    WHERE 
        p.cnt_subject_ref >= 10
        AND s.cnt_encounter_ref >= 10
);

-- ###########################################################

CREATE TABLE core__count_encounter_all_types AS (
    WITH
    filtered_table AS (
        SELECT
            s.subject_ref,
            s.encounter_ref,
            --noqa: disable=RF03, AL02
            s."class_display",
            s."type_display",
            s."serviceType_display",
            s."priority_display"
            --noqa: enable=RF03, AL02
        FROM core__encounter AS s
        WHERE s.status = 'finished'
    ),
    
    null_replacement AS (
        SELECT
            subject_ref,
            encounter_ref,
            coalesce(
                cast(class_display AS varchar),
                'cumulus__none'
            ) AS class_display,
            coalesce(
                cast(type_display AS varchar),
                'cumulus__none'
            ) AS type_display,
            coalesce(
                cast(serviceType_display AS varchar),
                'cumulus__none'
            ) AS serviceType_display,
            coalesce(
                cast(priority_display AS varchar),
                'cumulus__none'
            ) AS priority_display
        FROM filtered_table
    ),
    secondary_powerset AS (
        SELECT
            count(DISTINCT encounter_ref) AS cnt_encounter_ref,
            "class_display",
            "type_display",
            "serviceType_display",
            "priority_display",
            concat_ws(
                '-',
                COALESCE("class_display",''),
                COALESCE("type_display",''),
                COALESCE("serviceType_display",''),
                COALESCE("priority_display",'')
            ) AS id
        FROM null_replacement
        WHERE encounter_ref IS NOT NULL
        GROUP BY
            cube(
            "class_display",
            "type_display",
            "serviceType_display",
            "priority_display"
            )
    ),

    powerset AS (
        SELECT
            count(DISTINCT subject_ref) AS cnt_subject_ref,
            "class_display",
            "type_display",
            "serviceType_display",
            "priority_display",
            concat_ws(
                '-',
                COALESCE("class_display",''),
                COALESCE("type_display",''),
                COALESCE("serviceType_display",''),
                COALESCE("priority_display",'')
            ) AS id
        FROM null_replacement
        GROUP BY
            cube(
            "class_display",
            "type_display",
            "serviceType_display",
            "priority_display"
            )
    )

    SELECT
        s.cnt_encounter_ref AS cnt,
        p."class_display",
        p."type_display",
        p."serviceType_display",
        p."priority_display"
    FROM powerset AS p
    JOIN secondary_powerset AS s on s.id = p.id
    WHERE 
        p.cnt_subject_ref >= 10
        AND s.cnt_encounter_ref >= 10
);

-- ###########################################################

CREATE TABLE core__count_encounter_all_types_month AS (
    WITH
    filtered_table AS (
        SELECT
            s.subject_ref,
            s.encounter_ref,
            --noqa: disable=RF03, AL02
            s."class_display",
            s."type_display",
            s."serviceType_display",
            s."priority_display",
            s."period_start_month"
            --noqa: enable=RF03, AL02
        FROM core__encounter AS s
        WHERE s.status = 'finished'
    ),
    
    null_replacement AS (
        SELECT
            subject_ref,
            encounter_ref,
            coalesce(
                cast(class_display AS varchar),
                'cumulus__none'
            ) AS class_display,
            coalesce(
                cast(type_display AS varchar),
                'cumulus__none'
            ) AS type_display,
            coalesce(
                cast(serviceType_display AS varchar),
                'cumulus__none'
            ) AS serviceType_display,
            coalesce(
                cast(priority_display AS varchar),
                'cumulus__none'
            ) AS priority_display,
            coalesce(
                cast(period_start_month AS varchar),
                'cumulus__none'
            ) AS period_start_month
        FROM filtered_table
    ),
    secondary_powerset AS (
        SELECT
            count(DISTINCT encounter_ref) AS cnt_encounter_ref,
            "class_display",
            "type_display",
            "serviceType_display",
            "priority_display",
            "period_start_month",
            concat_ws(
                '-',
                COALESCE("class_display",''),
                COALESCE("type_display",''),
                COALESCE("serviceType_display",''),
                COALESCE("priority_display",''),
                COALESCE("period_start_month",'')
            ) AS id
        FROM null_replacement
        WHERE encounter_ref IS NOT NULL
        GROUP BY
            cube(
            "class_display",
            "type_display",
            "serviceType_display",
            "priority_display",
            "period_start_month"
            )
    ),

    powerset AS (
        SELECT
            count(DISTINCT subject_ref) AS cnt_subject_ref,
            "class_display",
            "type_display",
            "serviceType_display",
            "priority_display",
            "period_start_month",
            concat_ws(
                '-',
                COALESCE("class_display",''),
                COALESCE("type_display",''),
                COALESCE("serviceType_display",''),
                COALESCE("priority_display",''),
                COALESCE("period_start_month",'')
            ) AS id
        FROM null_replacement
        GROUP BY
            cube(
            "class_display",
            "type_display",
            "serviceType_display",
            "priority_display",
            "period_start_month"
            )
    )

    SELECT
        s.cnt_encounter_ref AS cnt,
        p."class_display",
        p."type_display",
        p."serviceType_display",
        p."priority_display",
        p."period_start_month"
    FROM powerset AS p
    JOIN secondary_powerset AS s on s.id = p.id
    WHERE 
        p.cnt_subject_ref >= 10
        AND s.cnt_encounter_ref >= 10
);

-- ###########################################################

CREATE TABLE core__count_encounter_type_month AS (
    WITH
    filtered_table AS (
        SELECT
            s.subject_ref,
            s.encounter_ref,
            --noqa: disable=RF03, AL02
            s."class_display",
            s."type_display",
            s."period_start_month"
            --noqa: enable=RF03, AL02
        FROM core__encounter AS s
        WHERE s.status = 'finished'
    ),
    
    null_replacement AS (
        SELECT
            subject_ref,
            encounter_ref,
            coalesce(
                cast(class_display AS varchar),
                'cumulus__none'
            ) AS class_display,
            coalesce(
                cast(type_display AS varchar),
                'cumulus__none'
            ) AS type_display,
            coalesce(
                cast(period_start_month AS varchar),
                'cumulus__none'
            ) AS period_start_month
        FROM filtered_table
    ),
    secondary_powerset AS (
        SELECT
            count(DISTINCT encounter_ref) AS cnt_encounter_ref,
            "class_display",
            "type_display",
            "period_start_month",
            concat_ws(
                '-',
                COALESCE("class_display",''),
                COALESCE("type_display",''),
                COALESCE("period_start_month",'')
            ) AS id
        FROM null_replacement
        WHERE encounter_ref IS NOT NULL
        GROUP BY
            cube(
            "class_display",
            "type_display",
            "period_start_month"
            )
    ),

    powerset AS (
        SELECT
            count(DISTINCT subject_ref) AS cnt_subject_ref,
            "class_display",
            "type_display",
            "period_start_month",
            concat_ws(
                '-',
                COALESCE("class_display",''),
                COALESCE("type_display",''),
                COALESCE("period_start_month",'')
            ) AS id
        FROM null_replacement
        GROUP BY
            cube(
            "class_display",
            "type_display",
            "period_start_month"
            )
    )

    SELECT
        s.cnt_encounter_ref AS cnt,
        p."class_display",
        p."type_display",
        p."period_start_month"
    FROM powerset AS p
    JOIN secondary_powerset AS s on s.id = p.id
    WHERE 
        p.cnt_subject_ref >= 10
        AND s.cnt_encounter_ref >= 10
);

-- ###########################################################

CREATE TABLE core__count_encounter_service_month AS (
    WITH
    filtered_table AS (
        SELECT
            s.subject_ref,
            s.encounter_ref,
            --noqa: disable=RF03, AL02
            s."class_display",
            s."serviceType_display",
            s."period_start_month"
            --noqa: enable=RF03, AL02
        FROM core__encounter AS s
        WHERE s.status = 'finished'
    ),
    
    null_replacement AS (
        SELECT
            subject_ref,
            encounter_ref,
            coalesce(
                cast(class_display AS varchar),
                'cumulus__none'
            ) AS class_display,
            coalesce(
                cast(serviceType_display AS varchar),
                'cumulus__none'
            ) AS serviceType_display,
            coalesce(
                cast(period_start_month AS varchar),
                'cumulus__none'
            ) AS period_start_month
        FROM filtered_table
    ),
    secondary_powerset AS (
        SELECT
            count(DISTINCT encounter_ref) AS cnt_encounter_ref,
            "class_display",
            "serviceType_display",
            "period_start_month",
            concat_ws(
                '-',
                COALESCE("class_display",''),
                COALESCE("serviceType_display",''),
                COALESCE("period_start_month",'')
            ) AS id
        FROM null_replacement
        WHERE encounter_ref IS NOT NULL
        GROUP BY
            cube(
            "class_display",
            "serviceType_display",
            "period_start_month"
            )
    ),

    powerset AS (
        SELECT
            count(DISTINCT subject_ref) AS cnt_subject_ref,
            "class_display",
            "serviceType_display",
            "period_start_month",
            concat_ws(
                '-',
                COALESCE("class_display",''),
                COALESCE("serviceType_display",''),
                COALESCE("period_start_month",'')
            ) AS id
        FROM null_replacement
        GROUP BY
            cube(
            "class_display",
            "serviceType_display",
            "period_start_month"
            )
    )

    SELECT
        s.cnt_encounter_ref AS cnt,
        p."class_display",
        p."serviceType_display",
        p."period_start_month"
    FROM powerset AS p
    JOIN secondary_powerset AS s on s.id = p.id
    WHERE 
        p.cnt_subject_ref >= 10
        AND s.cnt_encounter_ref >= 10
);

-- ###########################################################

CREATE TABLE core__count_encounter_priority_month AS (
    WITH
    filtered_table AS (
        SELECT
            s.subject_ref,
            s.encounter_ref,
            --noqa: disable=RF03, AL02
            s."class_display",
            s."priority_display",
            s."period_start_month"
            --noqa: enable=RF03, AL02
        FROM core__encounter AS s
        WHERE s.status = 'finished'
    ),
    
    null_replacement AS (
        SELECT
            subject_ref,
            encounter_ref,
            coalesce(
                cast(class_display AS varchar),
                'cumulus__none'
            ) AS class_display,
            coalesce(
                cast(priority_display AS varchar),
                'cumulus__none'
            ) AS priority_display,
            coalesce(
                cast(period_start_month AS varchar),
                'cumulus__none'
            ) AS period_start_month
        FROM filtered_table
    ),
    secondary_powerset AS (
        SELECT
            count(DISTINCT encounter_ref) AS cnt_encounter_ref,
            "class_display",
            "priority_display",
            "period_start_month",
            concat_ws(
                '-',
                COALESCE("class_display",''),
                COALESCE("priority_display",''),
                COALESCE("period_start_month",'')
            ) AS id
        FROM null_replacement
        WHERE encounter_ref IS NOT NULL
        GROUP BY
            cube(
            "class_display",
            "priority_display",
            "period_start_month"
            )
    ),

    powerset AS (
        SELECT
            count(DISTINCT subject_ref) AS cnt_subject_ref,
            "class_display",
            "priority_display",
            "period_start_month",
            concat_ws(
                '-',
                COALESCE("class_display",''),
                COALESCE("priority_display",''),
                COALESCE("period_start_month",'')
            ) AS id
        FROM null_replacement
        GROUP BY
            cube(
            "class_display",
            "priority_display",
            "period_start_month"
            )
    )

    SELECT
        s.cnt_encounter_ref AS cnt,
        p."class_display",
        p."priority_display",
        p."period_start_month"
    FROM powerset AS p
    JOIN secondary_powerset AS s on s.id = p.id
    WHERE 
        p.cnt_subject_ref >= 10
        AND s.cnt_encounter_ref >= 10
);

-- ###########################################################

CREATE TABLE core__count_medicationrequest_month AS (
    WITH
    filtered_table AS (
        SELECT
            s.subject_ref,
            --noqa: disable=RF03, AL02
            s."status",
            s."intent",
            s."authoredon_month",
            s."medication_display"
            --noqa: enable=RF03, AL02
        FROM core__medicationrequest AS s
    ),
    
    null_replacement AS (
        SELECT
            subject_ref,
            coalesce(
                cast(status AS varchar),
                'cumulus__none'
            ) AS status,
            coalesce(
                cast(intent AS varchar),
                'cumulus__none'
            ) AS intent,
            coalesce(
                cast(authoredon_month AS varchar),
                'cumulus__none'
            ) AS authoredon_month,
            coalesce(
                cast(medication_display AS varchar),
                'cumulus__none'
            ) AS medication_display
        FROM filtered_table
    ),

    powerset AS (
        SELECT
            count(DISTINCT subject_ref) AS cnt_subject_ref,
            "status",
            "intent",
            "authoredon_month",
            "medication_display",
            concat_ws(
                '-',
                COALESCE("status",''),
                COALESCE("intent",''),
                COALESCE("authoredon_month",''),
                COALESCE("medication_display",'')
            ) AS id
        FROM null_replacement
        GROUP BY
            cube(
            "status",
            "intent",
            "authoredon_month",
            "medication_display"
            )
    )

    SELECT
        p.cnt_subject_ref AS cnt,
        p."status",
        p."intent",
        p."authoredon_month",
        p."medication_display"
    FROM powerset AS p
    WHERE 
        p.cnt_subject_ref >= 10
);

-- ###########################################################

CREATE TABLE core__count_observation_lab_month AS (
    WITH
    filtered_table AS (
        SELECT
            s.subject_ref,
            s.observation_ref,
            e.class_display,
            --noqa: disable=RF03, AL02
            s."effectiveDateTime_month",
            s."observation_code",
            s."valueCodeableConcept_display"
            --noqa: enable=RF03, AL02
        FROM core__observation_lab AS s
        INNER JOIN core__encounter AS e
            ON s.encounter_ref = e.encounter_ref
        WHERE (s.status = 'final' OR s.status= 'amended')
    ),
    
    null_replacement AS (
        SELECT
            subject_ref,
            observation_ref,
            coalesce(
                cast(class_display AS varchar), 
                'cumulus__none'
            ) AS class_display,
            coalesce(
                cast(effectiveDateTime_month AS varchar),
                'cumulus__none'
            ) AS effectiveDateTime_month,
            coalesce(
                cast(observation_code AS varchar),
                'cumulus__none'
            ) AS observation_code,
            coalesce(
                cast(valueCodeableConcept_display AS varchar),
                'cumulus__none'
            ) AS valueCodeableConcept_display
        FROM filtered_table
    ),
    secondary_powerset AS (
        SELECT
            count(DISTINCT observation_ref) AS cnt_observation_ref,
            "effectiveDateTime_month",
            "observation_code",
            "valueCodeableConcept_display",
            class_display
            ,
            concat_ws(
                '-',
                COALESCE("effectiveDateTime_month",''),
                COALESCE("observation_code",''),
                COALESCE("valueCodeableConcept_display",''),
                COALESCE(class_display,'')
                
            ) AS id
        FROM null_replacement
        WHERE observation_ref IS NOT NULL
        GROUP BY
            cube(
            "effectiveDateTime_month",
            "observation_code",
            "valueCodeableConcept_display",
            class_display
            
            )
    ),

    powerset AS (
        SELECT
            count(DISTINCT subject_ref) AS cnt_subject_ref,
            "effectiveDateTime_month",
            "observation_code",
            "valueCodeableConcept_display",
            class_display
            ,
            concat_ws(
                '-',
                COALESCE("effectiveDateTime_month",''),
                COALESCE("observation_code",''),
                COALESCE("valueCodeableConcept_display",''),
                COALESCE(class_display,'')
                
            ) AS id
        FROM null_replacement
        GROUP BY
            cube(
            "effectiveDateTime_month",
            "observation_code",
            "valueCodeableConcept_display",
            class_display
            
            )
    )

    SELECT
        s.cnt_observation_ref AS cnt,
        p."effectiveDateTime_month",
        p."observation_code",
        p."valueCodeableConcept_display",
        p.class_display
    FROM powerset AS p
    JOIN secondary_powerset AS s on s.id = p.id
    WHERE 
        p.cnt_subject_ref >= 10
        AND s.cnt_observation_ref >= 10
);

-- ###########################################################

CREATE TABLE core__count_procedure_month AS (
    WITH
    filtered_table AS (
        SELECT
            s.subject_ref,
            --noqa: disable=RF03, AL02
            s."category_display",
            s."code_display",
            s."performedDateTime_month"
            --noqa: enable=RF03, AL02
        FROM core__procedure AS s
    ),
    
    null_replacement AS (
        SELECT
            subject_ref,
            coalesce(
                cast(category_display AS varchar),
                'cumulus__none'
            ) AS category_display,
            coalesce(
                cast(code_display AS varchar),
                'cumulus__none'
            ) AS code_display,
            coalesce(
                cast(performedDateTime_month AS varchar),
                'cumulus__none'
            ) AS performedDateTime_month
        FROM filtered_table
    ),

    powerset AS (
        SELECT
            count(DISTINCT subject_ref) AS cnt_subject_ref,
            "category_display",
            "code_display",
            "performedDateTime_month",
            concat_ws(
                '-',
                COALESCE("category_display",''),
                COALESCE("code_display",''),
                COALESCE("performedDateTime_month",'')
            ) AS id
        FROM null_replacement
        GROUP BY
            cube(
            "category_display",
            "code_display",
            "performedDateTime_month"
            )
    )

    SELECT
        p.cnt_subject_ref AS cnt,
        p."category_display",
        p."code_display",
        p."performedDateTime_month"
    FROM powerset AS p
    WHERE 
        p.cnt_subject_ref >= 10
);

-- ###########################################################

CREATE TABLE core__count_patient AS (
    WITH
    filtered_table AS (
        SELECT
            s.subject_ref,
            --noqa: disable=RF03, AL02
            s."gender",
            s."race_display",
            s."ethnicity_display"
            --noqa: enable=RF03, AL02
        FROM core__patient AS s
    ),
    
    null_replacement AS (
        SELECT
            subject_ref,
            coalesce(
                cast(gender AS varchar),
                'cumulus__none'
            ) AS gender,
            coalesce(
                cast(race_display AS varchar),
                'cumulus__none'
            ) AS race_display,
            coalesce(
                cast(ethnicity_display AS varchar),
                'cumulus__none'
            ) AS ethnicity_display
        FROM filtered_table
    ),

    powerset AS (
        SELECT
            count(DISTINCT subject_ref) AS cnt_subject_ref,
            "gender",
            "race_display",
            "ethnicity_display",
            concat_ws(
                '-',
                COALESCE("gender",''),
                COALESCE("race_display",''),
                COALESCE("ethnicity_display",'')
            ) AS id
        FROM null_replacement
        GROUP BY
            cube(
            "gender",
            "race_display",
            "ethnicity_display"
            )
    )

    SELECT
        p.cnt_subject_ref AS cnt,
        p."gender",
        p."race_display",
        p."ethnicity_display"
    FROM powerset AS p
    WHERE 
        p.cnt_subject_ref >= 10
);
