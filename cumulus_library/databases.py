"""Abstraction layers for supported database backends (e.g. AWS & DuckDB)

By convention, to maintain this as a relatively light wrapper layer, if you have
to chose between a convenience function in a specific library (as an example, the
[pyathena to_sql function](https://github.com/laughingman7743/PyAthena/#to-sql))
or using raw sql directly in some form, you should do the latter. This not a law;
if there's a compelling reason to do so, just make sure you add an appropriate
wrapper method in one of DatabaseCursor or DatabaseBackend.
"""

import abc
import datetime
import json
import os
import sys
from pathlib import Path
from typing import Protocol

import cumulus_fhir_support
import duckdb
import pandas
import pyarrow
import pyathena
from pyathena.common import BaseCursor as AthenaCursor
from pyathena.pandas.cursor import PandasCursor as AthenaPandasCursor


class DatabaseCursor(Protocol):
    """Protocol for a PEP-249 compatible cursor"""

    def execute(self, sql: str) -> None:
        pass

    def fetchone(self) -> list | None:
        pass

    def fetchmany(self, size: int | None) -> list[list] | None:
        pass

    def fetchall(self) -> list[list] | None:
        pass


class DatabaseParser(abc.ABC):
    """Parses information_schema results from a database"""

    def _parse_found_schema(
        self, expected: dict[dict[list]], schema: dict[list]
    ) -> dict:
        """Checks for presence of field for each column in a table

        :param expected: A nested dict describing the expected data format of
        a table. Expected format is like this:
            {
                "object_col":["member_a", "member_b"]
                "primitive_col": []
            }
        :param schema: the results of a query from the get_column_datatype method
        of the template_sql.templates function. It looks like this:
            [
                ('object_col', 'row(member_a VARCHAR, member_b DATE)'),
                ('primitive_col', 'VARCHAR')
            ]

        The actual contents of schema are database dependent. This naive method
        is a first pass, ignoring complexities of differing database variable
        types, just iterating through looking for column names.

        TODO: on a per database instance, consider a more nuanced approach if needed
              (compared to just checking if the schema contains the field name)
        """
        output = {}

        for column, fields in expected.items():
            column_lower = column.lower()

            # is this an object column? (like: "subject": ["reference"])
            if fields:
                col_schema = schema.get(column_lower, "").lower()
                output[column] = {
                    # TODO: make this check more robust
                    field: field.lower() in col_schema
                    for field in fields
                }

            # otherwise this is a primitive col (like: "recordedDate": None)
            else:
                output[column] = column_lower in schema

        return output

    @abc.abstractmethod
    def validate_table_schema(
        self, expected: dict[str, list[str]], schema: list[tuple]
    ) -> dict:
        """Public interface for investigating if fields are in a table schema.

        This method should lightly format results and pass them to
        _parse_found_schema(), or a more bespoke table analysis function if needed.
        """


class DatabaseBackend(abc.ABC):
    """A generic database backend, supporting basic cursor operations"""

    def __init__(self, schema_name: str):
        """Create connection to a database backend

        :param schema_name: the database name ('schema' is Athena-speak for a database)
        """
        self.schema_name = schema_name

    @abc.abstractmethod
    def cursor(self) -> DatabaseCursor:
        """Returns a connection to the backing database"""

    @abc.abstractmethod
    def pandas_cursor(self) -> DatabaseCursor:
        """Returns a connection to the backing database optimized for dataframes

        If your database does not provide an optimized cursor, this should function the
        same as a vanilla cursor.
        """

    @abc.abstractmethod
    def execute_as_pandas(self, sql: str) -> pandas.DataFrame:
        """Returns a pandas.DataFrame version of the results from the provided SQL"""

    @abc.abstractmethod
    def parser(self) -> DatabaseParser:
        """Returns parser object for interrogating DB schemas"""

    @abc.abstractmethod
    def close(self) -> None:
        """Clean up any resources necessary"""


class AthenaDatabaseBackend(DatabaseBackend):
    """Database backend that can talk to AWS Athena"""

    def __init__(self, region: str, workgroup: str, profile: str, schema_name: str):
        super().__init__(schema_name)

        connect_kwargs = {}
        for aws_env_name in [
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_SESSION_TOKEN",
        ]:
            if aws_env_val := os.environ.get(aws_env_name):
                connect_kwargs[aws_env_name.lower()] = aws_env_val
        # the profile may not be required, provided the above three AWS env vars
        # are set. If both are present, the env vars take precedence
        if profile is not None:
            connect_kwargs["profile_name"] = profile

        self.connection = pyathena.connect(
            region_name=region,
            work_group=workgroup,
            schema_name=self.schema_name,
            **connect_kwargs,
        )
        self.pandas_cursor = self.connection.cursor(cursor=AthenaPandasCursor)

    def cursor(self) -> AthenaCursor:
        return self.connection.cursor()

    def pandas_cursor(self) -> AthenaPandasCursor:
        return self.pandas_cursor

    def execute_as_pandas(self, sql: str) -> pandas.DataFrame:
        return self.pandas_cursor.execute(sql).as_pandas()

    def parser(self) -> DatabaseParser:
        return AthenaParser()

    def close(self) -> None:
        return self.connection.close()


class AthenaParser(DatabaseParser):
    def validate_table_schema(
        self, expected: dict[dict[list]], schema: list[list]
    ) -> dict:
        schema = dict(schema)
        return self._parse_found_schema(expected, schema)


class DuckDatabaseBackend(DatabaseBackend):
    """Database backend that uses local files via duckdb"""

    def __init__(self, db_file: str):
        super().__init__("main")
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
    def _compat_array_join(
        value: list[str | None] | None, delimiter: str
    ) -> str | None:
        if value is None:
            return None
        return delimiter.join(v for v in value if v is not None)

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

    def execute_as_pandas(self, sql: str) -> pandas.DataFrame:
        # We call convert_dtypes here in case there are integer columns.
        # Pandas will normally cast nullable-int as a float type unless
        # we call this to convert to its nullable int column type.
        # PyAthena seems to do this correctly for us, but not DuckDB.
        return self.connection.execute(sql).df().convert_dtypes()

    def parser(self) -> DatabaseParser:
        return DuckDbParser()

    def close(self) -> None:
        self.connection.close()


class DuckDbParser(DatabaseParser):
    """Table parser for DuckDB schemas"""

    def validate_table_schema(
        self, expected: dict[dict[list]], schema: list[tuple]
    ) -> dict:
        """parses a information_schema.tables query response for valid columns"""
        schema = dict(schema)
        # since we're defaulting to athena naming conventions elsewhere,
        # we'll lower case all the keys
        schema = {k.lower(): v for k, v in schema.items()}

        return self._parse_found_schema(expected, schema)


def read_ndjson_dir(path: str) -> dict[str, pyarrow.Table]:
    """Loads a directory tree of raw ndjson into schema-ful tables.

    :param path: a directory path
    :returns: dictionary of table names (like 'documentreference') to table
      data (with schema)
    """
    all_tables = {}

    # Manually specify the list of resources because we want to create each table
    # even if the folder does not exist.
    resources = [
        "AllergyIntolerance",
        "Condition",
        "Device",
        "DiagnosticReport",
        "DocumentReference",
        "Encounter",
        "Immunization",
        "Medication",
        "MedicationRequest",
        "Observation",
        "Patient",
        "Procedure",
        "ServiceRequest",
    ]
    for resource in resources:
        table_name = resource.lower()

        # Grab filenames to load (ignoring .meta files and handling missing folders)
        folder = Path(f"{path}/{table_name}")
        filenames = []
        if folder.exists():
            filenames = sorted(
                str(x) for x in folder.iterdir() if x.name.endswith(".ndjson")
            )

        # Read all ndjson directly into memory
        rows = []
        for filename in filenames:
            with open(filename, encoding="utf8") as f:
                for line in f:
                    rows.append(json.loads(line))

        # Make a pyarrow table with full schema from the data
        schema = cumulus_fhir_support.pyarrow_schema_from_rows(resource, rows)
        all_tables[table_name] = pyarrow.Table.from_pylist(rows, schema)

    return all_tables


def create_db_backend(args: dict[str, str]) -> DatabaseBackend:
    db_type = args["db_type"]
    database = args["schema_name"]
    load_ndjson_dir = args.get("load_ndjson_dir")

    if db_type == "duckdb":
        backend = DuckDatabaseBackend(database)  # `database` is path name in this case
        if load_ndjson_dir:
            backend.insert_tables(read_ndjson_dir(load_ndjson_dir))
    elif db_type == "athena":
        backend = AthenaDatabaseBackend(
            args["region"],
            args["workgroup"],
            args["profile"],
            database,
        )
        if load_ndjson_dir:
            sys.exit("Loading an ndjson dir is not supported with --db-type=athena.")
    else:
        raise ValueError(f"Unexpected --db-type value '{db_type}'")

    return backend
