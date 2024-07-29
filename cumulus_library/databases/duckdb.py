"""Database abstraction layer for DuckDB

See base.py for design rules of thumb.

Since duckdb tends to be more malleable than a cloud-based database, if you
need to paper over differences, it's best to use the connection.create_function
pattern here to get sql flavors in alignment.
"""

import collections
import datetime
import re

import duckdb
import pandas
import pyarrow

from cumulus_library import errors
from cumulus_library.databases import base


class DuckDatabaseBackend(base.DatabaseBackend):
    """Database backend that uses local files via duckdb"""

    def __init__(self, db_file: str, schema_name: str | None = None):
        super().__init__("main")
        self.db_type = "duckdb"
        # As of the 1.0 duckdb release, local scopes, where schema names can be provided
        # as configuration to duckdb.connect, are not supported.
        # https://duckdb.org/docs/sql/statements/set.html#syntax

        # This is where the connection config would be supplied when it is supported
        self.connection = duckdb.connect(db_file)
        # Aliasing Athena's as_pandas to duckDB's df cast
        duckdb.DuckDBPyConnection.as_pandas = duckdb.DuckDBPyConnection.df

        # Paper over some syntax differences between Athena and DuckDB
        self.connection.create_function(
            # DuckDB's version is array_to_string -- seems there is no standard here.
            "array_join",
            self._compat_array_join,
            None,
            duckdb.typing.VARCHAR,
        )
        self.connection.create_function(
            # DuckDB's version is regexp_matches.
            "regexp_like",
            self._compat_regexp_like,
            None,
            duckdb.typing.BOOLEAN,
        )
        self.connection.create_function(
            # We frequently use Athena's date() function because it's easier than
            # the more widely-supported way of CAST(x AS DATE).
            # Rather than convert all of our SQL to the longer version,
            # we'll just make our own version of date().
            "date",
            self._compat_date,
            None,
            duckdb.typing.DATE,
        )
        self.connection.create_function(
            "from_iso8601_timestamp",
            self._compat_from_iso8601_timestamp,
            None,
            duckdb.typing.TIMESTAMP,
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
            duckdb.typing.VARCHAR,
        )

    def insert_tables(self, tables: dict[str, pyarrow.Table]) -> None:
        """Ingests all ndjson data from a folder tree.

        This is often the output folder of Cumulus ETL"""
        for name, table in tables.items():
            self.connection.register(name, table)

    @staticmethod
    def _compat_array_join(value: list[str | None] | None, delimiter: str | None) -> str | None:
        if value is None:
            return None
        if delimiter is None or delimiter == "None":
            # This is exercised locally on unit tests but is not in CI. Not sure why,
            # and not sure it's worth debugging
            delimiter = ""  # pragma: no cover
        return delimiter.join(v for v in value if v is not None)

    @staticmethod
    def _compat_regexp_like(string: str | None, pattern: str | None) -> bool:
        if string is None or string == "None" or pattern is None or pattern == "None":
            return None
        match = re.search(pattern, string)
        return match is not None

    @staticmethod
    def _compat_date(
        value: str | datetime.datetime | datetime.date | None,
    ) -> datetime.date | None:
        if value is None:
            return None
        elif isinstance(value, str):
            return datetime.date.fromisoformat(value)
        elif isinstance(value, datetime.datetime):
            return value.date()
        elif isinstance(value, datetime.date):
            return value
        else:
            raise ValueError("Unexpected date() argument:", type(value), value)

    @staticmethod
    def _compat_to_utf8(value: str | None) -> datetime.date | None:
        """See the create_function() call for to_utf8 for more background"""
        # This is exercised locally on unit tests but is not in CI. Not sure why,
        # and not sure it's worth debugging
        return value

    @staticmethod
    def _compat_from_iso8601_timestamp(
        value: str | None,
    ) -> datetime.datetime | None:
        if value is None:
            return None

        # handle partial dates like 1970 or 1980-12 (which spec allows)
        if len(value) < 10:
            pieces = value.split("-")
            if len(pieces) == 1:
                return datetime.datetime(int(pieces[0]), 1, 1)
            else:
                return datetime.datetime(int(pieces[0]), int(pieces[1]), 1)

        # Until we depend on Python 3.11+, manually handle Z
        value = value.replace("Z", "+00:00")

        # TODO: return timezone-aware datetimes, like Athena does
        #       (this currently generates naive datetimes, in UTC local time)
        return datetime.datetime.fromisoformat(value)

    def cursor(self) -> duckdb.DuckDBPyConnection:
        # Don't actually create a new connection,
        # because then we'd have to re-register our json tables.
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

    def col_pyarrow_types_from_sql(self, columns: list[tuple]) -> list:
        output = []
        for column in columns:
            match column[1]:
                case "STRING":
                    output.append((column[0], pyarrow.string()))
                case "INTEGER":
                    output.append((column[0], pyarrow.int64()))
                case "NUMBER":
                    output.append((column[0], pyarrow.float64()))
                case "DOUBLE":
                    output.append((column[0], pyarrow.float64()))
                case "boolean" | "bool":
                    output.append((column[0], pyarrow.bool_()))
                case "Date":
                    output.append((column[0], pyarrow.date64()))
                case "TIMESTAMP" | "DATETIME":
                    output.append((column[0], pyarrow.timestamp("s")))
                case _:
                    raise errors.CumulusLibraryError(
                        f"{column[0],column[1]} does not have a conversion type"
                    )
        return output

    def parser(self) -> base.DatabaseParser:
        return DuckDbParser()

    def operational_errors(self) -> tuple[Exception]:
        return (
            duckdb.OperationalError,
            duckdb.BinderException,
        )

    def create_schema(self, schema_name):
        """Creates a new schema object inside the database"""
        schemas = self.connection.sql(
            "SELECT schema_name FROM information_schema.schemata"
        ).fetchall()
        if (schema_name,) not in schemas:
            self.connection.sql(f"CREATE SCHEMA {schema_name}")

    def close(self) -> None:
        self.connection.close()


class DuckDbParser(base.DatabaseParser):
    """Table parser for DuckDB schemas"""

    def parse_found_schema(self, schema: dict[str, str]) -> dict:
        """parses a information_schema.tables query response for valid columns"""
        parsed = {}

        for key, value in schema.items():
            if isinstance(value, str):
                # DuckDB provides a parser to go from string -> type objects
                value = duckdb.typing.DuckDBPyType(value)

            # Collapse lists to the contained value
            if value.id == "list":
                value = value.children[0][1]  # [('child', CONTAINED_TYPE)]

            if value.id == "struct":
                result = self.parse_found_schema(dict(value.children))
            else:
                result = {}
            parsed[key.lower()] = result

        return parsed
