-- ###########################################################
CREATE TABLE core__count_patient AS 
    with powerset as
    (
        select
        count(distinct subject_ref)   as cnt_subject
        
        , age, gender, race_display, ethnicity_display        
        FROM core__patient
        group by CUBE
        ( age, gender, race_display, ethnicity_display )
    )
    select
          cnt_subject as cnt 
        , age, gender, race_display, ethnicity_display
    from powerset 
    WHERE cnt_subject >= 10 
    ;

-- ###########################################################
CREATE TABLE core__count_encounter_month AS 
    with powerset as
    (
        select
        count(distinct subject_ref)   as cnt_subject
        , count(distinct encounter_ref)   as cnt_encounter
        , start_month, enc_class_display, age_at_visit, gender, race_display, ethnicity_display        
        FROM core__encounter
        group by CUBE
        ( start_month, enc_class_display, age_at_visit, gender, race_display, ethnicity_display )
    )
    select
          cnt_encounter  as cnt 
        , start_month, enc_class_display, age_at_visit, gender, race_display, ethnicity_display
    from powerset 
    WHERE cnt_subject >= 10 
    ;

-- ###########################################################
CREATE TABLE core__count_encounter_type AS 
    with powerset as
    (
        select
        count(distinct subject_ref)   as cnt_subject
        , count(distinct encounter_ref)   as cnt_encounter
        , enc_class_display, enc_type_display, enc_service_display, enc_priority_display        
        FROM core__encounter_type
        group by CUBE
        ( enc_class_display, enc_type_display, enc_service_display, enc_priority_display )
    )
    select
          cnt_encounter  as cnt 
        , enc_class_display, enc_type_display, enc_service_display, enc_priority_display
    from powerset 
    WHERE cnt_subject >= 10 
    ;

-- ###########################################################
CREATE TABLE core__count_encounter_type_month AS 
    with powerset as
    (
        select
        count(distinct subject_ref)   as cnt_subject
        , count(distinct encounter_ref)   as cnt_encounter
        , enc_class_display, enc_type_display, enc_service_display, enc_priority_display, start_month        
        FROM core__encounter_type
        group by CUBE
        ( enc_class_display, enc_type_display, enc_service_display, enc_priority_display, start_month )
    )
    select
          cnt_encounter  as cnt 
        , enc_class_display, enc_type_display, enc_service_display, enc_priority_display, start_month
    from powerset 
    WHERE cnt_subject >= 10 
    ;

-- ###########################################################
CREATE TABLE core__count_encounter_enc_type_month AS 
    with powerset as
    (
        select
        count(distinct subject_ref)   as cnt_subject
        , count(distinct encounter_ref)   as cnt_encounter
        , enc_class_display, enc_type_display, start_month        
        FROM core__encounter_type
        group by CUBE
        ( enc_class_display, enc_type_display, start_month )
    )
    select
          cnt_encounter  as cnt 
        , enc_class_display, enc_type_display, start_month
    from powerset 
    WHERE cnt_subject >= 10 
    ;

-- ###########################################################
CREATE TABLE core__count_encounter_service_month AS 
    with powerset as
    (
        select
        count(distinct subject_ref)   as cnt_subject
        , count(distinct encounter_ref)   as cnt_encounter
        , enc_class_display, enc_service_display, start_month        
        FROM core__encounter_type
        group by CUBE
        ( enc_class_display, enc_service_display, start_month )
    )
    select
          cnt_encounter  as cnt 
        , enc_class_display, enc_service_display, start_month
    from powerset 
    WHERE cnt_subject >= 10 
    ;

-- ###########################################################
CREATE TABLE core__count_encounter_priority_month AS 
    with powerset as
    (
        select
        count(distinct subject_ref)   as cnt_subject
        , count(distinct encounter_ref)   as cnt_encounter
        , enc_class_display, enc_priority_display, start_month        
        FROM core__encounter_type
        group by CUBE
        ( enc_class_display, enc_priority_display, start_month )
    )
    select
          cnt_encounter  as cnt 
        , enc_class_display, enc_priority_display, start_month
    from powerset 
    WHERE cnt_subject >= 10 
    ;
