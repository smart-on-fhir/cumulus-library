"""Database abstraction layer for DuckDB

See base.py for design rules of thumb.

Since duckdb tends to be more malleable than a cloud-based database, if you
need to paper over differences, it's best to use the connection.create_function
pattern here to get sql flavors in alignment.
"""

import base64
import collections
import datetime
import json
import pathlib
import re
import time
from concurrent import futures

import duckdb
import pandas
import pyarrow
from rich import progress

from cumulus_library import base_utils
from cumulus_library.databases import base, utils


class DuckDatabaseBackend(base.DatabaseBackend):
    """Database backend that uses local files via duckdb"""

    def __init__(
        self,
        db_file: str,
        schema_name: str | None = None,
        max_concurrent: int | None = None,
        pyarrow_cache_path: str | None = None,
    ):
        super().__init__("main")
        self.db_type = "duckdb"
        self.db_file = db_file
        self.connection = None
        self.max_concurrent = max_concurrent or 20
        self.pyarrow_cache_path = pyarrow_cache_path

    def init_errors(self):
        return ["Binder Error", "Catalog Error"]

    def connect(self):
        """Connects to the local duckdb database"""
        # As of the 1.0 duckdb release, local scopes, where schema names can be provided
        # as configuration to duckdb.connect, are not supported.
        # https://duckdb.org/docs/sql/statements/set.html#syntax
        # This is where the connection config would be supplied when it is supported
        self.connection = duckdb.connect(self.db_file)
        # Aliasing Athena's as_pandas to duckDB's df cast
        duckdb.DuckDBPyConnection.as_pandas = duckdb.DuckDBPyConnection.df

        # Ignore order of NDJSON that we load in. It saves memory and order doesn't matter for us.
        self.connection.execute("SET preserve_insertion_order = false;")

        # Are we getting a cached copy of the pyarrow schema when the class was created?
        # Usually this means we're running unit tests and trying to save time
        if self.pyarrow_cache_path and pathlib.Path(self.pyarrow_cache_path).is_file():
            self.connection.execute(
                "CREATE TABLE IF NOT EXISTS pyarrow_cache AS "  # noqa: S608
                f"SELECT * FROM read_parquet('{self.pyarrow_cache_path}')"
            )

        # Paper over some syntax differences between Athena and DuckDB
        self.connection.create_function(
            # DuckDB's version is array_to_string -- seems there is no standard here.
            "array_join",
            self._compat_array_join,
            None,
            duckdb.sqltypes.VARCHAR,
        )
        self.connection.create_function(
            # DuckDB's version is regexp_matches.
            "regexp_like",
            self._compat_regexp_like,
            None,
            duckdb.sqltypes.BOOLEAN,
        )
        self.connection.create_function(
            "from_iso8601_timestamp",
            self._compat_from_iso8601_timestamp,
            None,
            # Note: DuckDB provides a timestamp aware column type, TIMESTAMP_TZ, but
            # as of this writing on version 1.4.1, it is doing some casting to local
            # offset time rather than timezone, which we're electing to not deal with
            duckdb.sqltypes.TIMESTAMP,
        )
        self.connection.create_function(
            # When trying to calculate an MD5 hash in Trino/Athena, the implementation
            # expects to recieve a varbinary type, so if you're hashing a string,
            # you would invoke it like `SELECT md5(to_utf8(string_col)) from table`.
            #
            # DuckDB's md5() function accepts a varchar instead, and does not have a
            # to_utf() function or varbinary type, so we patch this with a UDF that
            # just provides back the original string. As a result, these functions
            # have different signatures, but for cases like this where you're
            # conforming an argument to another function, it provides appropriate
            # function mocking
            #
            # NOTE: currently we do not have a use case beyond experimentation where
            # using MD5 hashes provide a benefit. Until we do, it is not required to
            # support this in other DatabaseBackend implementations.
            "to_utf8",
            self._compat_to_utf8,
            None,
            duckdb.sqltypes.VARCHAR,
        )

    def insert_tables(self, tables: dict[dict[str, str]]) -> None:
        """Ingests all ndjson data from a folder tree.

        This function will write a cache of pyarrow datasets to the database.
        The next time this database is started, if we get passed the same set
        of files and they haven't changed, we'll use the cache. Otherwise
        we'll make a new set.

        The data loaded in this way is often from the output folder of Cumulus ETL

        :param tables: A dict describing table info (generated by utils.get_ndjson_files())"""

        self.connection.execute(
            "CREATE TABLE IF NOT EXISTS pyarrow_cache "
            "(table_name VARCHAR, file_array VARCHAR, schema VARCHAR)"
        )

        cache_valid = self.is_cache_valid(tables)

        if not cache_valid:
            # Our cache is out of date, let's get fresh table datasets - this may take a bit,
            # since read_ndjson_dir has to scan through all the files.
            datasets = utils.read_ndjson_dir(None, tables)
            self.connection.execute("DELETE FROM pyarrow_cache")
        else:
            # Our cache is still good. We'll pipe back in the pyarrow schemas we serialized
            # out to the database, and use that to create the dataset interfaces.
            datasets = self.get_cached_datasets()

        for name, dataset in datasets.items():
            self.connection.register(f"{name}", dataset)

            # If we've got new datasets, let's stash information about the expected files
            # and the schemas we found in the db for the next run.
            if not cache_valid:
                self.cache_dataset(name, dataset)

    def is_cache_valid(self, tables) -> bool:
        """Checks if ndjson has been updated since the last time we cached it"""
        # 'tables' contains a group of fhir tables, as well as etl completion tables;
        # we don't care about that distinction here, so we'll just ignore the first
        # layer of keys.
        for subtables in tables.values():
            for name, fragments in subtables.items():
                # See if we've got a cache of this table, and if it's still valid
                cached_files = self.connection.execute(
                    f"SELECT file_array FROM pyarrow_cache WHERE table_name = '{name.lower()}'"  # noqa: S608
                ).fetchone()
                if cached_files is not None:
                    cached_files = json.loads(cached_files[0])
                else:
                    cached_files = []

                fragment_infos = []
                for fragment in fragments:
                    fragment_modtime = pathlib.Path(fragment).lstat().st_mtime
                    fragment_infos.append([fragment, fragment_modtime])
                if cached_files != fragment_infos:
                    return False
        return True

    def cache_dataset(self, name: str, dataset: pyarrow.dataset.Dataset):
        """serializes a pyarrow Dataset and stashes it in the cache table"""
        fragment_infos = []
        fragments = dataset.get_fragments()
        for fragment in fragments:
            fragment_path = fragment.path
            fragment_modtime = pathlib.Path(fragment_path).lstat().st_mtime
            fragment_infos.append((fragment_path, fragment_modtime))
        schema = base64.b64encode(dataset.schema.serialize().to_pybytes()).decode("ascii")
        self.connection.execute(
            "INSERT INTO pyarrow_cache "  # noqa: S608
            f"VALUES ('{name}', '{json.dumps(fragment_infos)}', '{schema}')"
        )

    def get_cached_datasets(self):
        """Deserializes pyarrow datasets from a database cache"""
        datasets = {}
        schemas = self.connection.execute(
            "SELECT table_name, file_array, schema FROM pyarrow_cache"
        ).fetchall()
        for row in schemas:
            schema = pyarrow.ipc.read_schema(pyarrow.py_buffer(base64.b64decode(row[2])))
            files = []
            for source in json.loads(row[1]):
                files.append(source[0])
            datasets[row[0]] = pyarrow.dataset.dataset(
                files, schema=schema, format=utils._json_format()
            )
        return datasets

    @staticmethod
    def _compat_array_join(value: list[str | None], delimiter: str | None) -> str:
        if delimiter is None or delimiter == "None":
            # This is exercised locally on unit tests but is not in CI. Not sure why,
            # and not sure it's worth debugging
            delimiter = ""  # pragma: no cover
        return delimiter.join(v for v in value if v is not None)

    @staticmethod
    def _compat_regexp_like(string: str | None, pattern: str | None) -> bool:
        match = re.search(pattern, string)
        return match is not None

    @staticmethod
    def _compat_to_utf8(value: str | None) -> datetime.date | None:
        """See the create_function() call for to_utf8 for more background"""
        # This is exercised locally on unit tests but is not in CI. Not sure why,
        # and not sure it's worth debugging
        return value  # pragma: no cover

    @staticmethod
    def _compat_from_iso8601_timestamp(
        value: str,
    ) -> datetime.datetime:
        # handle partial dates like 1970 or 1980-12 (which spec allows)
        if len(value) < 10:
            pieces = value.split("-")
            if len(pieces) == 1:
                return datetime.datetime(int(pieces[0]), 1, 1, tzinfo=datetime.UTC)
            else:
                return datetime.datetime(int(pieces[0]), int(pieces[1]), 1, tzinfo=datetime.UTC)

        dt = datetime.datetime.fromisoformat(value)
        if not dt.tzinfo:
            return dt.replace(tzinfo=datetime.UTC)
        return dt.astimezone(datetime.UTC)

    def cursor(self) -> duckdb.DuckDBPyConnection:
        return self.connection

    def pandas_cursor(self) -> duckdb.DuckDBPyConnection:
        # Since this is not provided, return the vanilla cursor
        return self.connection

    def execute_as_pandas(
        self, sql: str, chunksize: int | None = None
    ) -> (pandas.DataFrame | collections.abc.Iterator[pandas.DataFrame], list[tuple]):
        # We call convert_dtypes here in case there are integer columns.
        # Pandas will normally cast nullable-int as a float type unless
        # we call this to convert to its nullable int column type.
        # PyAthena seems to do this correctly for us, but not DuckDB.
        result = self.connection.execute(sql)
        if chunksize:
            return iter([result.df().convert_dtypes()]), result.description
        return result.df().convert_dtypes(), result.description

    def parser(self) -> base.DatabaseParser:
        return DuckDbParser()

    def operational_errors(self) -> tuple[type[Exception], ...]:
        return (
            duckdb.OperationalError,
            duckdb.BinderException,
        )

    def export_table_as_parquet(
        self, table_name: str, file_name: str, location: pathlib.Path, *args, **kwargs
    ) -> bool:
        parquet_path = location / f"{file_name}"
        table_size = self.connection.execute(f"SELECT count(*) FROM {table_name}").fetchone()  # noqa: S608
        if table_size[0] == 0:
            return False
        query = f"""COPY
            (SELECT * FROM {table_name})
            TO '{parquet_path}'
            (FORMAT parquet)
            """  # noqa: S608
        self.connection.execute(query)
        return True

    def _write_thread(self, query, verbose, progress_bar, task, query_console_output, datasets):
        thread_con = self.connection.cursor()
        # Since registrations are per cursor, we'll use our cache
        # of pyarrow datasets again to re-initialize ndjson tables
        for name, dataset in datasets.items():
            thread_con.register(f"{name}", dataset)
        with query_console_output(verbose, query, progress_bar, task):
            thread_con.execute(query)

    def parallel_write(
        self,
        queries: list[str],
        verbose: bool,
        progress_bar: progress.Progress,
        task: progress.Task,
    ):
        datasets = self.get_cached_datasets()
        with futures.ThreadPoolExecutor(max_workers=self.max_concurrent) as executor:
            res = []
            for query in queries:
                res.append(
                    (
                        query,
                        executor.submit(
                            self._write_thread,
                            query,
                            verbose,
                            progress_bar,
                            task,
                            base_utils.query_console_output,
                            datasets,
                        ),
                    )
                )
            utils.handle_concurrent_errors(res, self.db_type)

        # This is a temporary workaround for cases where duckdb is still doing :something:
        # after the threads have resolved, where if you try to recreate a table it may throw
        # an error, which is only affecting the --statistics flag ¯\_(ツ)_/¯
        time.sleep(0.25)

    def create_schema(self, schema_name):
        """Creates a new schema object inside the database"""
        schemas = self.connection.sql(
            "SELECT schema_name FROM information_schema.schemata"
        ).fetchall()
        if (schema_name,) not in schemas:
            self.connection.sql(f"CREATE SCHEMA {schema_name}")

    def close(self) -> None:
        if self.connection is not None:
            self.connection.close()


class DuckDbParser(base.DatabaseParser):
    """Table parser for DuckDB schemas"""

    def parse_found_schema(self, schema: dict[str, str]) -> dict:
        """parses a information_schema.tables query response for valid columns"""
        parsed = {}

        for key, value in schema.items():
            if isinstance(value, str):
                # DuckDB provides a parser to go from string -> type objects
                value = duckdb.sqltypes.DuckDBPyType(value)

            # Collapse lists to the contained value
            if value.id == "list":
                value = value.children[0][1]  # [('child', CONTAINED_TYPE)]

            if value.id == "struct":
                result = self.parse_found_schema(dict(value.children))
            else:
                result = {}
            parsed[key.lower()] = result

        return parsed
