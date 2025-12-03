This folder holds a small selection of results from Synthea.

The selection is designed to be big enough to get past most of the 10-patient
bucketing cutoffs, while small enough to not overwhelm the repo with fake patient data.

If you change the data in this folder, don't forget to rerun update_pyarrow_cache.py -
We use the cache to speed up DB initialization during unit tests. Tests won't fail if
you forget, but the cache will get rebuilt during each test using the database.