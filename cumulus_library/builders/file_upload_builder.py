"""Class for generating tables from a list of file uploads"""

import pathlib
import re
import sys

import msgspec
import numpy
import pandas
import platformdirs
import pyarrow

from cumulus_library import BaseTableBuilder, base_utils, errors, study_manifest
from cumulus_library.template_sql import base_templates


class FileUploadTask(msgspec.Struct, forbid_unknown_fields=True):
    file: str | None = None  # deprecated
    files: list[str] | None = None
    create_mode: str = "single"
    delimiter: str | None = None
    col_types: list[str] | None = None
    always_upload: bool = False


class FileUploadWorkflow(msgspec.Struct, forbid_unknown_fields=True):
    config_type: str
    tables: dict[str, FileUploadTask]


class FileUploadBuilder(BaseTableBuilder):
    display_text = "Uploading files..."

    @staticmethod
    def _snake_case(x: str) -> str:
        """Converts either CAPS_CASE or CamelCase to snake_case"""
        return re.sub(r"([a-z])([A-Z])", r"\1_\2", x).lower()

    def __init__(self, *args, toml_config_path: pathlib.Path, **kwargs):
        super().__init__(*args, **kwargs)
        self._toml_config_path = toml_config_path
        self._toml_config_dir = toml_config_path.parent
        try:
            with open(toml_config_path, "rb") as file:
                file_bytes = file.read()
                self._workflow_config = msgspec.to_builtins(
                    msgspec.toml.decode(file_bytes, type=FileUploadWorkflow)
                )
        except msgspec.ValidationError as e:  # pragma: no cover
            sys.exit(
                f"The file upload workflow at {toml_config_path!s} contains an "
                "unexpected param: \n"
                f"{e}"
            )
        for table in self._workflow_config["tables"]:
            # migrate deprecated `file` key if found
            if file := self._workflow_config["tables"][table]["file"]:
                self._workflow_config["tables"][table]["files"] = [file]
            if mode := self._workflow_config["tables"][table]["create_mode"] not in (
                "single",
                "multiple",
            ):
                raise errors.FileUploadError(  # pragma: no cover
                    f"Create mode '{mode}' in file upload workflow "
                    f"{self._toml_config_path} is invalid."
                )

    def get_pandas_read_params(
        self, df: pandas.DataFrame, pandas_types: list | None, task_name: str
    ) -> (dict, list):
        """Sets up proper typing for reading data into a dataframe

        :param df: a dataframe, read with naieve typing
        :param pandas_types: a list, cast from table['col_types'], of types to use
        :param task_name: the name of the task being run, for error messages
        :returns: a type dict, and a list of date columns

        Due to how pandas wants to handle nullable date casting, when we detect a date
        column, we will cast it as a string object, and then use the date column list
        later to perform conversions after load.
        """
        dtype_dict = {}
        date_cols = []
        if pandas_types is None:
            pandas_types = [pandas.StringDtype() for x in list(df.columns)]
        if len(pandas_types) != len(df.columns):
            raise errors.FileUploadError(
                f"{task_name} has {len(df.columns)} columns, but the provided "
                f"col_types has {len(pandas_types)} entries."
            )
        for index in range(0, len(df.columns)):
            if pandas_types[index] == numpy.datetime64:
                dtype_dict[df.columns[index]] = pandas.StringDtype()
                date_cols.append(df.columns[index])
            else:
                dtype_dict[df.columns[index]] = pandas_types[index]
        return dtype_dict, date_cols

    def reformat_dates(self, df, date_cols):
        for col in date_cols:
            df[col] = pandas.to_datetime(df[col])
        return df

    def prepare_queries(
        self,
        config: base_utils.StudyConfig,
        manifest: study_manifest.StudyManifest,
        *args,
        **kwargs,
    ):
        cache_dir = (
            pathlib.Path(platformdirs.user_cache_dir("cumulus-library", "smart-on-fhir"))
            / f"file_uploads/{manifest.get_study_prefix()}"
        )
        cache_dir.mkdir(parents=True, exist_ok=True)

        with base_utils.get_progress_bar() as progress:
            task = progress.add_task(
                "Uploading static files...",
                total=len(self._workflow_config["tables"]),
            )
            for task_name in self._workflow_config["tables"]:
                table = self._workflow_config["tables"][task_name]

                # we want to generate one query per task - if we hit a case where
                # multiple files are backing a similar table, we'll skip appending
                # copies of the same query later on.
                generated_query = False

                # To handle cases where we're using a folder of data to back either a single
                # table with multiple files, or creation of multiple tables, we'll scan for
                # directories and replace them with their contents
                new_files = []
                for file_or_dir in table["files"]:
                    if (self._toml_config_dir / file_or_dir).is_dir():
                        dir_contents = [
                            f"{file_or_dir}/{x.name}"
                            for x in (self._toml_config_dir / file_or_dir).glob("**/*")
                        ]
                        new_files = new_files + dir_contents
                    else:
                        new_files.append(file_or_dir)
                table["files"] = new_files

                for file in table["files"]:
                    table_filename = pathlib.Path(file).name

                    # First, we're going to read data into a pandas dataframe.
                    # For non-parquet cases, we have to jump through some hoops to support
                    # date casting. So, we'll first check if column types were supplied.
                    # Then we're going to naievely load the file, so we can inspect it to get the
                    # names of columns and create a list of expected data types if needed (subbing
                    # out dates for objects, so we can do that postprocessing later) and the actual
                    # column names, which we don't ask the user to provide. We'll then load it a
                    # second time with the required types (getting around some type casting issues
                    # after the dataframe has been created), and then manually cast the date columns
                    # from objects to dates. Yeesh!
                    if table["col_types"] is None:
                        pandas_types = None
                    else:
                        pandas_types = base_utils.pandas_types_from_hive_types(table["col_types"])
                    if file.endswith(".md"):
                        continue
                    elif file.endswith(".xlsx"):
                        parquet_path = cache_dir / table_filename.replace(".xlsx", ".parquet")
                        df = pandas.read_excel(self._toml_config_dir / file)
                        dtype_dict, date_cols = self.get_pandas_read_params(
                            df, pandas_types, task_name
                        )
                        df = pandas.read_excel(self._toml_config_dir / file, dtype=pandas_types)
                    elif file.endswith(".csv") or file.endswith(".tsv") or file.endswith(".bsv"):
                        parquet_path = cache_dir / f"{table_filename[:-4]}.parquet"
                        if table["delimiter"] is None:
                            match file.split(".")[-1]:
                                case "bsv":
                                    table["delimiter"] = "|"
                                case "csv":
                                    table["delimiter"] = ","
                                case "tsv":
                                    table["delimiter"] = "\t"
                        df = pandas.read_csv(
                            self._toml_config_dir / file,
                            delimiter=table["delimiter"],
                        )
                        dtype_dict, date_cols = self.get_pandas_read_params(
                            df, pandas_types, task_name
                        )
                        df = pandas.read_csv(
                            self._toml_config_dir / file,
                            delimiter=table["delimiter"],
                            dtype=dtype_dict,
                        )

                    elif file.endswith(".parquet"):
                        parquet_path = cache_dir / table_filename
                        df = pandas.read_parquet(self._toml_config_dir / file)
                        dtype_dict, date_cols = self.get_pandas_read_params(
                            df, pandas_types, task_name
                        )

                    else:
                        raise errors.FileUploadError(
                            f"{table['file']} is not a supported upload file type.\n"
                            "Supported file types: csv, bsv, tsv, xlsx, parquet"
                        )

                    df = self.reformat_dates(df, date_cols)
                    if table["create_mode"] == "single":
                        # for single table mode, we can use the name of the task from the workflow
                        table_name = task_name
                    else:
                        # for multiple tables mode, we'll want to infer it from the filename
                        table_name = pathlib.Path(file).stem
                    if table["col_types"] is None:
                        table["col_types"] = ["STRING" for x in df.columns]
                    path_parts = list(parquet_path.parts)
                    path_parts.insert(-1, table_name)
                    parquet_path = pathlib.Path().joinpath(*path_parts)
                    local_location = parquet_path.parent

                    df = df.rename(self._snake_case, axis="columns")

                    # Now we're going to do a type dance :again: to get the types we want
                    # for athena, mostly to distinguish dates from timestamps. We don't start
                    # with this because we would need to know the exact date format before we
                    # tried to read if we had a pyarrow only solution. So we'll get a different
                    # type system, create an arrow table using it, and use that to write out the
                    # parquet for writing to athena.
                    type_dict = {}
                    pyarrow_types = base_utils.pyarrow_types_from_hive_types(table["col_types"])
                    arrow_table = pyarrow.Table.from_pandas(df, preserve_index=False)
                    for pos in range(0, len(pyarrow_types)):
                        type_dict[df.columns[pos]] = pyarrow_types[pos]
                    schema = pyarrow.schema([pyarrow.field(x[0], x[1]) for x in type_dict.items()])
                    arrow_table = arrow_table.cast(schema)
                    parquet_path.parent.mkdir(parents=True, exist_ok=True)
                    pyarrow.parquet.write_table(arrow_table, parquet_path)
                    remote_path = config.db.upload_file(
                        file=parquet_path,
                        study=manifest.get_study_prefix(),
                        topic=table_name,
                        force_upload=table["always_upload"] or config.force_upload,
                    )
                    if not generated_query or table["create_mode"] == "multiple":
                        self.queries.append(
                            base_templates.get_ctas_from_parquet_query(
                                schema_name=config.schema,
                                table_name=f"{manifest.get_study_prefix()}__{table_name}",
                                local_location=local_location,
                                remote_location=remote_path,
                                table_cols=df.columns,
                                remote_table_cols_types=table["col_types"],
                            )
                        )
                        generated_query = True
                progress.advance(task)
