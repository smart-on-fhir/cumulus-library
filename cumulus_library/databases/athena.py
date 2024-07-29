"""Database abstraction layer for AWS Athena

See base.py for design rules of thumb - notably, don't use non-PEP249 database
features, like pyathena's async cursors, to simplify cross-db behavior.
"""

import collections
import os
import pathlib

import boto3
import botocore
import numpy
import pandas
import pyarrow
import pyathena
from pyathena.common import BaseCursor as AthenaCursor
from pyathena.pandas.cursor import PandasCursor as AthenaPandasCursor

from cumulus_library import errors
from cumulus_library.databases import base


class AthenaDatabaseBackend(base.DatabaseBackend):
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

    def execute_as_pandas(
        self, sql: str, chunksize: int | None = None
    ) -> (pandas.DataFrame | collections.abc.Iterator[pandas.DataFrame], list[tuple]):
        query = self.pandas_cursor().execute(sql, chunksize=chunksize)
        return query.as_pandas(), query.description

    def parser(self) -> base.DatabaseParser:
        return AthenaParser()

    def operational_errors(self) -> tuple[Exception]:
        return (pyathena.OperationalError,)

    def col_parquet_types_from_pandas(self, field_types: list) -> list:
        output = []
        for field in field_types:
            match field:
                case numpy.dtypes.ObjectDType():
                    output.append("STRING")
                case pandas.core.arrays.integer.Int64Dtype() | numpy.dtypes.Int64DType():
                    output.append("INT")
                case numpy.dtypes.Float64DType():
                    output.append("DOUBLE")
                case numpy.dtypes.BoolDType():
                    output.append("BOOLEAN")
                case numpy.dtypes.DateTime64DType():
                    output.append("TIMESTAMP")
                case _:
                    raise errors.CumulusLibraryError(
                        f"Unsupported pandas type {type(field)} found."
                    )
        return output

    def col_pyarrow_types_from_sql(self, columns: list[tuple]) -> list:
        output = []
        for column in columns:
            match column[1]:
                case "varchar":
                    output.append((column[0], pyarrow.string()))
                case "bigint":
                    output.append((column[0], pyarrow.int64()))
                case "integer":
                    output.append((column[0], pyarrow.int64()))
                case "double":
                    output.append((column[0], pyarrow.float64()))
                case "decimal":
                    output.append((column[0], pyarrow.decimal128(column[4], column[5])))
                case "boolean":
                    output.append((column[0], pyarrow.bool_()))
                case "date":
                    output.append((column[0], pyarrow.date64()))
                case "timestamp":
                    output.append((column[0], pyarrow.timestamp("s")))
                case _:
                    raise errors.CumulusLibraryError(f"Unsupported SQL type '{column[1]}' found.")
        return output

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
        workgroup = self.connection._client.get_work_group(WorkGroup=self.work_group)
        wg_conf = workgroup["WorkGroup"]["Configuration"]["ResultConfiguration"]
        s3_path = wg_conf["OutputLocation"]
        bucket = "/".join(s3_path.split("/")[2:3])
        key_prefix = "/".join(s3_path.split("/")[3:])
        encryption_type = wg_conf.get("EncryptionConfiguration", {}).get("EncryptionOption", {})
        if encryption_type != "SSE_KMS":
            raise errors.AWSError(
                f"Bucket {bucket} has unexpected encryption type {encryption_type}."
                "AWS KMS encryption is expected for Cumulus buckets"
            )
        kms_arn = wg_conf.get("EncryptionConfiguration", {}).get("KmsKey", None)
        s3_key = f"{key_prefix}cumulus_user_uploads/{self.schema_name}/" f"{study}/{topic}"
        if not remote_filename:
            remote_filename = file.name

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

    def create_schema(self, schema_name) -> None:
        """Creates a new schema object inside the database"""
        glue_client = boto3.client("glue")
        try:
            glue_client.get_database(Name=schema_name)
        except botocore.exceptions.ClientError:
            glue_client.create_database(DatabaseInput={"Name": schema_name})

    def close(self) -> None:
        return self.connection.close()  # pragma: no cover


class AthenaParser(base.DatabaseParser):
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
