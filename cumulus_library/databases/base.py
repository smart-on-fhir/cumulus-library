"""Base Abstraction layer for interfacing with databases

By convention, to maintain this as a relatively light wrapper layer, if you have
to chose between a convenience function in a specific library (as an example, the
[pyathena to_sql function](https://github.com/laughingman7743/PyAthena/#to-sql))
or using raw sql directly in some form, you should do the latter. This not a law;
if there's a compelling reason to do so, just make sure you add an appropriate
wrapper method in one of DatabaseCursor or DatabaseBackend.
"""

import abc
import collections
import pathlib
from typing import Any, Protocol

import pandas


class DatabaseCursor(Protocol):
    """Protocol for a PEP-249 compatible cursor"""

    def execute(self, sql: str) -> None:
        pass  # pragma: no cover

    def fetchone(self) -> list | None:
        pass  # pragma: no cover

    def fetchmany(self, size: int | None) -> list[list] | None:
        pass  # pragma: no cover

    def fetchall(self) -> list[list] | None:
        pass  # pragma: no cover


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

    def validate_table_schema(self, expected: dict[str, list], schema: list[tuple]) -> dict:
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
    def init_errors(self) -> list:
        """A list of errors indicating a database may not have been initialized with the ETL"""

    @abc.abstractmethod
    def connect(self):
        """Initiates connection configuration of the database"""

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
    def execute_as_pandas(
        self, sql: str, chunksize: int | None = None
    ) -> (pandas.DataFrame | collections.abc.Iterator[pandas.DataFrame], list[tuple]):
        """Returns a pandas.DataFrame version of the results from the provided SQL"""

    @abc.abstractmethod
    def parser(self) -> DatabaseParser:
        """Returns parser object for interrogating DB schemas"""

    def operational_errors(self) -> tuple[type[Exception], ...]:
        """Returns a tuple of operational exception classes

        An operational error is something that went wrong while performing a database
        query. So something like "table doesn't exist" but not like a network or
        syntax error.

        This is designed to be used in an `except` clause.
        """
        return ()  # pragma: no cover

    def col_parquet_types_from_pandas(self, field_types: list) -> list:
        """Returns appropriate types for creating tables based from parquet.

        By default, returns an empty list (which assumes that the DB infers directly
        from parquet data types). Only override if your DB uses an explicit SerDe
        format, or otherwise needs a modified typing to inject directly into a query."""

        # The following example shows the types we're expecting to catch with this
        # approach and the rough type to cast them to.
        # TODO: consider handling complex types.
        # output = []
        # for field in field_types:
        #     match field:
        #         case numpy.dtypes.ObjectDType():
        #             output.append('string')
        #         case pandas.core.arrays.integer.Int64Dtype():
        #             output.append('int')
        #         case numpy.dtypes.Float64DType():
        #             output.append('float')
        #         case numpy.dtypes.BoolDType():
        #             output.append('bool')
        #         case numpy.dtypes.DateTime64DType():
        #             output.append('date')
        #         case _:
        #             raise errors.CumulusLibraryError(
        #                 f"Unsupported type {type(field)} found."
        #             )
        return []

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
    def export_table_as_parquet(
        self, table_name: str, file_name: str, location: pathlib.Path, *args, **kwargs
    ) -> pathlib.Path | None:
        """Gets a parquet file from a specified table.

        This is intended as a way to get the most database native parquet export possible,
        so we don't have to infer schema information. Only do schema inferring if your
        DB engine does not support parquet natively. If a table is empty, return None."""

    @abc.abstractmethod
    def create_schema(self, schema_name):
        """Creates a new schema object inside the catalog"""

    @abc.abstractmethod
    def close(self) -> None:
        """Clean up any resources necessary"""
