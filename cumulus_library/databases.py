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
import pathlib
import sys
from pathlib import Path
from typing import Any, Protocol

import boto3
import cumulus_fhir_support
import duckdb
import pandas
import pyarrow
import pyathena
from pyathena.common import BaseCursor as AthenaCursor
from pyathena.pandas.cursor import PandasCursor as AthenaPandasCursor

from cumulus_library import db_config, errors


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

    @abc.abstractmethod
    def parse_found_schema(self, schema: dict[str, str]) -> dict:
        """Parses a database-provided schema string.

        :param schema: the results of a query from the get_column_datatype method
        of the template_sql.templates function. It looks like this (for athena at
        least, but the values are opaque strings and database-provider-specific):
            {
                'object_col': 'row(member_a varchar, member_b date)',
                'primitive_col': 'varchar',
            }

        :returns: a dictionary with an entry for every field present in the schema.
        For the above example, this should return:
            {
                'object_col': {
                    'member_a': {},
                    'member_b': {},
                },
                'primitive_col': {},
            }
        """

    def _recursively_validate(
        self, expected: dict[str, Any], schema: dict[str, Any]
    ) -> dict[str, Any]:
        schema = schema or {}
        output = {}

        for column, fields in expected.items():
            col_schema = schema.get(column.lower())

            # Is `fields` an falsy? (like: "recordedDate": None or [] or {})
            # This means we just want to check existance of `column`
            # otherwise this is a primitive col
            if not fields:
                output[column] = col_schema is not None

            # Is `fields` a list? (like: "subject": ["reference"])
            # This means we should check existance of all mentioned children.
            elif isinstance(fields, list):
                for field in fields:
                    subschema = self._recursively_validate({field: None}, col_schema)
                    output.setdefault(column, {}).update(subschema)

            # Is `fields` a dict?
            # Like: "component": {"valueQuantity": ["unit", "value"]}
            # This means we should descend one level
            elif isinstance(fields, dict):
                subschema = self._recursively_validate(fields, col_schema)
                output[column] = subschema

            else:
                raise ValueError("Bad expected schema provided")

        return output

    def validate_table_schema(
        self, expected: dict[str, list], schema: list[tuple]
    ) -> dict:
        """Public interface for investigating if fields are in a table schema.

        expected is a dictionary of string column names to *something*:
        - falsy (like None or []): just check that the column exists
        - list of strings: check all the column children exist
        - dict of a new child 'expected' dictionary, with same above rules

        This method should lightly format results and pass them to
        parse_found_schema(), or a more bespoke table analysis function if needed.
        """
        parsed_schema = self.parse_found_schema(dict(schema))
        return self._recursively_validate(expected, parsed_schema)


class DatabaseBackend(abc.ABC):
    """A generic database backend, supporting basic cursor operations"""

    def __init__(self, schema_name: str):
        """Create connection to a database backend

        :param schema_name: the database name ('schema' is Athena-speak for a database)
        """
        self.schema_name = schema_name
        # db_type, while perhaps feeling redundant, is intended to be a value that is
        # passed to jinja templates for creating valid sql for a particular database's
        # technology
        self.db_type = None

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

    def upload_file(
        self,
        *,
        file: pathlib.Path,
        study: str,
        topic: str,
        remote_filename: str | None = None,
        force_upload=False,
    ) -> str | None:
        """Handler for remote database file upload.

        By default, this should return None. Only override this for databases that
        have an API for file upload (i.e. cloud databases)"""
        return None

    @abc.abstractmethod
    def close(self) -> None:
        """Clean up any resources necessary"""


class AthenaDatabaseBackend(DatabaseBackend):
    """Database backend that can talk to AWS Athena"""

    def __init__(self, region: str, work_group: str, profile: str, schema_name: str):
        super().__init__(schema_name)

        self.db_type = "athena"
        self.region = region
        self.work_group = work_group
        self.profile = profile
        self.schema_name = schema_name
        # the profile may not be required, provided the above three AWS env vars
        # are set. If both are present, the env vars take precedence
        connect_kwargs = {}
        if self.profile is not None:
            connect_kwargs["profile_name"] = self.profile

        for aws_env_name in [
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_SESSION_TOKEN",
        ]:
            if aws_env_val := os.environ.get(aws_env_name):
                connect_kwargs[aws_env_name.lower()] = aws_env_val
        self.connection = pyathena.connect(
            region_name=self.region,
            work_group=self.work_group,
            schema_name=self.schema_name,
            **connect_kwargs,
        )

    def cursor(self) -> AthenaCursor:
        return self.connection.cursor()

    def pandas_cursor(self) -> AthenaPandasCursor:
        return self.connection.cursor(cursor=AthenaPandasCursor)

    def execute_as_pandas(self, sql: str) -> pandas.DataFrame:
        return self.pandas_cursor().execute(sql).as_pandas()

    def parser(self) -> DatabaseParser:
        return AthenaParser()

    def upload_file(
        self,
        *,
        file: pathlib.Path,
        study: str,
        topic: str,
        remote_filename: str | None = None,
        force_upload=False,
    ) -> str | None:
        # We'll investigate the connection to get the relevant S3 upload path.
        wg_conf = self.connection._client.get_work_group(WorkGroup=self.work_group)[
            "WorkGroup"
        ]["Configuration"]["ResultConfiguration"]
        s3_path = wg_conf["OutputLocation"]
        bucket = "/".join(s3_path.split("/")[2:3])
        key_prefix = "/".join(s3_path.split("/")[3:])
        encryption_type = wg_conf.get("EncryptionConfiguration", {}).get(
            "EncryptionOption", {}
        )
        if encryption_type != "SSE_KMS":
            raise errors.AWSError(
                f"Bucket {bucket} has unexpected encryption type {encryption_type}."
                "AWS KMS encryption is expected for Cumulus buckets"
            )
        kms_arn = wg_conf.get("EncryptionConfiguration", {}).get("KmsKey", None)
        s3_key = (
            f"{key_prefix}cumulus_user_uploads/{self.schema_name}/" f"{study}/{topic}"
        )
        if not remote_filename:
            remote_filename = file

        session = boto3.Session(profile_name=self.connection.profile_name)
        s3_client = session.client("s3")
        if not force_upload:
            res = s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix=f"{s3_key}/{remote_filename}",
            )
            if res["KeyCount"] > 0:
                return f"s3://{bucket}/{s3_key}"
        with open(file, "rb") as b_file:
            s3_client.put_object(
                Bucket=bucket,
                Key=f"{s3_key}/{remote_filename}",
                Body=b_file,
                ServerSideEncryption="aws:kms",
                SSEKMSKeyId=kms_arn,
            )
        return f"s3://{bucket}/{s3_key}"

    def close(self) -> None:
        return self.connection.close()


class AthenaParser(DatabaseParser):
    def _find_type_len(self, row: str) -> int:
        """Finds the end of a type string like row(...) or array(row(...))"""
        # Note that this assumes the string is well formatted.
        depth = 0
        for i in range(len(row)):
            match row[i]:
                case ",":
                    if depth == 0:
                        break
                case "(":
                    depth += 1
                case ")":
                    depth -= 1
        return i

    def _split_row(self, row: str) -> dict[str, str]:
        # Must handle "name type, name row(...), name type, name row(...)"
        result = {}
        # Real athena doesn't add extra spaces, but our unit tests do for
        # readability, so let's strip out whitespace as we parse.
        while row := row.strip():
            name, remainder = row.split(" ", 1)
            type_len = self._find_type_len(remainder)
            result[name] = remainder[0:type_len]
            row = remainder[type_len + 2 :]  # skip comma and space
        return result

    def parse_found_schema(self, schema: dict[str, str]) -> dict:
        # A sample response for table `observation`, column `component`:
        #   array(row(code varchar, display varchar)),
        #             text varchar, id varchar)
        parsed = {}

        for key, value in schema.items():
            # Strip arrays out, they don't affect the shape of our schema.
            while value.startswith("array("):
                value = value.removeprefix("array(")
                value = value.removesuffix(")")

            if value.startswith("row("):
                value = value.removeprefix("row(")
                value = value.removesuffix(")")
                parsed[key] = self.parse_found_schema(self._split_row(value))
            else:
                parsed[key] = {}

        return parsed


class DuckDatabaseBackend(DatabaseBackend):
    """Database backend that uses local files via duckdb"""

    def __init__(self, db_file: str):
        super().__init__("main")
        self.db_type = "duckdb"
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


def _read_rows_from_table_dir(path: str) -> list[dict]:
    # Grab filenames to load (ignoring .meta files and handling missing folders)
    folder = Path(path)
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

    return rows


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
        rows = _read_rows_from_table_dir(f"{path}/{table_name}")

        # Make a pyarrow table with full schema from the data
        schema = cumulus_fhir_support.pyarrow_schema_from_rows(resource, rows)
        all_tables[table_name] = pyarrow.Table.from_pylist(rows, schema)

    # And now some special support for a few ETL tables.
    metadata_tables = [
        "etl__completion",
        "etl__completion_encounters",
    ]
    for metadata_table in metadata_tables:
        rows = _read_rows_from_table_dir(f"{path}/{metadata_table}")
        if rows:
            # Auto-detecting the schema works for these simple tables
            all_tables[metadata_table] = pyarrow.Table.from_pylist(rows)

    return all_tables


def create_db_backend(args: dict[str, str]) -> DatabaseBackend:
    db_config.db_type = args["db_type"]
    database = args["schema_name"]
    load_ndjson_dir = args.get("load_ndjson_dir")

    if db_config.db_type == "duckdb":
        backend = DuckDatabaseBackend(database)  # `database` is path name in this case
        if load_ndjson_dir:
            backend.insert_tables(read_ndjson_dir(load_ndjson_dir))
    elif db_config.db_type == "athena":
        backend = AthenaDatabaseBackend(
            args["region"],
            args["workgroup"],
            args["profile"],
            database,
        )
        if load_ndjson_dir:
            sys.exit("Loading an ndjson dir is not supported with --db-type=athena.")
    else:
        raise ValueError(f"Unexpected --db-type value '{db_config.db_type}'")

    return backend
