This folder holds a small selection of results from Synthea.

The selection is designed to be big enough to get past most of the 10-patient
bucketing cutoffs, while small enough to not overwhelm the repo with fake patient data.


## Pyarrow caching

It takes a little bit for duckdb to load in ndjson files and create tables. To get around
this, we create a pyarrow cache table. This is then re-injected in the mock db fixtures.

This cache gets automatically regenerated any time it's not valid. This works great most
of the time, except when we're using different kinds of json to generate data (which we
don't do often).