config_type = "counts"
[tables.basic_count]
source_table = "core__patient"
table_cols = ["gender","birthdate","postalcode_3"]

[tables.wheres]
source_table = "core__patient"
table_cols = ["gender","birthdate","postalcode_3"]
where_clauses = [
    "gender = 'female'"
]

[tables.wheres_min_subject]
source_table = "core__patient"
table_cols = ["gender","birthdate","postalcode_3"]
where_clauses = [
    "gender = 'female'"
]
min_subject = 5

[tables.primary_id]
source_table = "core__observation"
table_cols = ["status","observation_code"]
primary_id = "observation_ref"

[tables.secondary_table]
source_table = "core__patient"
table_cols = ["gender","birthdate","postalcode_3"]
secondary_table = 'core__encounter'
secondary_cols = ["status"]

[tables.annotated]
source_table = "core__condition"
table_cols = ["code", "clinicalstatus_code"]
min_subject = 2

[tables.annotated.annotation]
field = "code"
join_table = '"main"."snomed"'
join_field = "code"
columns = [
    ["system","varchar"],
    ["display","varchar", "display_name"]
]

[tables.filtered]
source_table = "core__condition"
table_cols = ["code", "clinicalstatus_code"]
min_subject = 2
filter_status = true
filter_cols = [
    ["clinicalstatus_code",["resolved"],false]
]
