## study_python_valid count tables

### study_python_valid__count_table

|Column| Type  |Description|
|------|-------|-----------|
|test  |INTEGER|           |


## study_python_valid base tables

### study_python_valid__lib_build_source

|Column| Type  |Description|
|------|-------|-----------|
|stage |VARCHAR|           |
|name  |VARCHAR|           |
|type  |VARCHAR|           |


### study_python_valid__lib_ref_summary

|   Column    |  Type   |Description|
|-------------|---------|-----------|
|table_name   |VARCHAR  |           |
|ref_type     |VARCHAR  |           |
|ref_count    |INTEGER  |           |
|delta_percent|DOUBLE   |           |
|event_time   |TIMESTAMP|           |


### study_python_valid__lib_transactions

|    Column     |  Type   |Description|
|---------------|---------|-----------|
|study_name     |VARCHAR  |           |
|library_version|VARCHAR  |           |
|status         |VARCHAR  |           |
|event_time     |TIMESTAMP|           |
|message        |VARCHAR  |           |


### study_python_valid__table

|Column| Type  |Description|
|------|-------|-----------|
|test  |INTEGER|           |


