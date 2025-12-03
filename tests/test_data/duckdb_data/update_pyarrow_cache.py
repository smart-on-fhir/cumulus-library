import pathlib

import rich

from cumulus_library import databases

data_path = pathlib.Path(__file__).parent
db, _ = databases.create_db_backend(
    {
        "db_type": "duckdb",
        "database": ":memory:",
        "load_ndjson_dir": data_path,
    }
)
cache = db.connection.execute(
    f"COPY pyarrow_cache TO '{data_path}/pyarrow_cache.parquet' (FORMAT parquet)"
)
# Paranoia check - is what we just created still readable?
db.connection.execute("DROP TABLE pyarrow_cache")
db.connection.execute(
    f"CREATE TABLE pyarrow_cache AS SELECT * FROM read_parquet('{data_path}/pyarrow_cache.parquet')"
)
rich.print(
    f"cache table size: {db.connection.execute('SELECT count(*) FROM pyarrow_cache').fetchone()[0]}"
)
