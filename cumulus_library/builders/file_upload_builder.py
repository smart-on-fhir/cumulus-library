"""Class for generating tables from a list of file uploads"""

import pathlib
import re
import sys

import msgspec
import pandas
import platformdirs

from cumulus_library import BaseTableBuilder, base_utils, errors, study_manifest
from cumulus_library.template_sql import base_templates

"""
A file upload config looks like this

type="file_upload"
[tables]
[table.name] # the table in db will be called 'name', snaked cased for sql compatibility
files = ["relative/path/from/config/file.csv"]
# The rest of these are optional
delimiter = "|" # comma by default, only applies to csv/tsv/bsv
col_types =["STRING","DATE","DOUBLE"] # default is all strings. Only relevant for Athena.
# See https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types for valid types
[table.other_name]
file = "other/path/file.csv"
"""


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
                if table["create_mode"] == "single":
                    # For single table mode with multiple files, we'll track the dataframe
                    # column types after the first file is ingested, and throw an error if
                    # we have a mismatch, rather than waiting for the error to happen at the
                    # SQL level. If there's one file, this will get set but then never used
                    # for validation
                    types = None

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
                    if file.endswith(".md"):
                        continue
                    elif file.endswith(".xlsx"):
                        parquet_path = cache_dir / table_filename.replace(".xlsx", ".parquet")
                        df = pandas.read_excel(self._toml_config_dir / file)

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
                            self._toml_config_dir / file, delimiter=table["delimiter"]
                        )

                    elif file.endswith(".parquet"):
                        parquet_path = cache_dir / table_filename
                        df = pandas.read_parquet(self._toml_config_dir / file)

                    else:
                        raise errors.FileUploadError(
                            f"{table['file']} is not a supported upload file type.\n"
                            "Supported file types: csv, bsv, tsv, xlsx, parquet"
                        )
                    if table["create_mode"] == "single":
                        if types is None:
                            types = df.dtypes
                        elif list(types.items()) != list(df.dtypes.items()):
                            raise errors.FileUploadError(
                                f"{table_filename} does not match the schema of the other files"
                            )
                        # for single table mode, we can use the name of the task from the workflow
                        table_name = task_name
                    else:
                        # for multiple tables mode, we'll want to infer it from the filename
                        table_name = pathlib.Path(file).stem
                    path_parts = list(parquet_path.parts)
                    path_parts.insert(-1, table_name)
                    parquet_path = pathlib.Path().joinpath(*path_parts)
                    local_location = parquet_path.parent

                    df = df.rename(self._snake_case, axis="columns")
                    if table["col_types"] is None:
                        table["col_types"] = ["STRING" for x in df.columns]
                    if len(table["col_types"]) != len(df.columns):
                        raise errors.FileUploadError(
                            f"{task_name} has {len(df.columns)} columns, but the provided "
                            f"col_types has {len(table['col_types'])} entries."
                        )
                    type_dict = {}
                    numpy_types = base_utils.numpy_types_from_hive_types(table["col_types"])
                    for pos in range(0, len(numpy_types)):
                        type_dict[df.columns[pos]] = numpy_types[pos]
                    df = df.astype(type_dict)
                    parquet_path.parent.mkdir(parents=True, exist_ok=True)
                    df.to_parquet(parquet_path)
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
