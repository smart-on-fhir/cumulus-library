"""A list of files/filename partials to be allowed to be uploaded to an aggregator

While a valid generation target, `.archive.` should never be part of this list, since
that explicitly contains line level data.
"""

ALLOWED_UPLOADS = [".cube.", ".annotated_cube.", ".flat.", ".meta.", "manifest.toml"]


"""ProtectedTableBuilder configurations that may need to be looked up elsewhere"""

TRANSACTIONS_COLS = ["study_name", "library_version", "status", "event_time", "message"]
TRANSACTION_COLS_TYPES = ["varchar", "varchar", "varchar", "timestamp", "varchar"]
# while it may seem redundant, study_name and view_name are included as a column for
# ease of constructing a view of multiple transaction tables
STATISTICS_COLS = [
    "study_name",
    "library_version",
    "table_type",
    "table_name",
    "view_name",
    "created_on",
]
STATISTICS_COLS_TYPES = [
    "varchar",
    "varchar",
    "varchar",
    "varchar",
    "varchar",
    "timestamp",
]
BUILD_SOURCE_COLS = ["stage", "name", "type"]
BUILD_SOURCE_COLS_ATHENA_TYPE = ["string", "string", "string"]
BUILD_SOURCE_COLS_SQL_TYPE = ["varchar", "varchar", "varchar"]

REF_SUMMARY_COLS = ["table_name", "ref_type", "ref_count", "delta_percent", "event_time"]
REF_SUMMARY_COLS_TYPES = ["varchar", "varchar", "integer", "double", "timestamp"]
