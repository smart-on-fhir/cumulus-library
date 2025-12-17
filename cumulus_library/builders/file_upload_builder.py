"""Class for generating tables from a list of file uploads"""

import pathlib
import re

import pandas
import platformdirs

from cumulus_library import BaseTableBuilder, base_utils, errors, study_manifest
from cumulus_library.template_sql import base_templates

"""
A file upload config looks like this

type="file_upload"
[tables]
[table.name] # the table in db will be called 'name', snaked cased for sql compatibility
file = "relative/path/from/config/file.csv"
# The rest of these are optional
delimiter = "|" # comma by default, only applies to csv/tsv/bsv
col_types =["STRING","DATE","DOUBLE"] # default is all strings. Only relevant for Athena.
# See https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types for valid types
[table.other_name]
file = "other/path/file.csv"
"""


class FileUploadBuilder(BaseTableBuilder):
    display_text = "Uploading files..."

    @staticmethod
    def snake_case(x: str) -> str:
        """Converts either CAPS_CASE or CamelCase to snake_case"""
        return re.sub(r"([a-z])([A-Z])", r"\1_\2", x).lower()

    def __init__(self, *args, workflow_config: dict, toml_config_path: pathlib.Path, **kwargs):
        self.workflow_config = workflow_config
        self.toml_config_path = toml_config_path
        self.toml_config_dir = toml_config_path.parent
        super().__init__(*args, **kwargs)

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
                total=len(self.workflow_config["tables"]),
            )
            for table_name in self.workflow_config["tables"]:
                table = self.workflow_config["tables"][table_name]
                table_filename = pathlib.Path(table["file"]).name

                if table["file"].endswith(".xlsx"):
                    parquet_path = cache_dir / table_filename.replace(".xlsx", ".parquet")
                    df = pandas.read_excel(self.toml_config_dir / table["file"])

                elif (
                    table["file"].endswith(".csv")
                    or table["file"].endswith(".tsv")
                    or table["file"].endswith(".bsv")
                ):
                    parquet_path = cache_dir / f"{table_filename[:-4]}.parquet"
                    if "delimiter" not in table:
                        match table["file"].split(".")[-1]:
                            case "bsv":
                                table["delimiter"] = "|"
                            case "csv":
                                table["delimiter"] = ","
                            case "tsv":
                                table["delimiter"] = "\t"
                    df = pandas.read_csv(
                        self.toml_config_dir / table["file"], delimiter=table["delimiter"]
                    )

                elif table["file"].endswith(".parquet"):
                    parquet_path = cache_dir / table_filename
                    df = pandas.read_parquet(self.toml_config_dir / table["file"])

                else:
                    raise errors.FileUploadError(
                        f"{table['file']} is not a supported upload file type.\n"
                        "Supported file types: csv, bsv, tsv, xlsx, parquet"
                    )
                if "always_upload" not in table:
                    table["always_upload"] = True
                df = df.rename(self.snake_case, axis="columns")
                df.to_parquet(parquet_path)
                remote_path = config.db.upload_file(
                    file=parquet_path,
                    study=manifest.get_study_prefix(),
                    topic=parquet_path.stem,
                    force_upload=table["always_upload"] or config.force_upload,
                )
                if table.get("col_types") is None:
                    table["col_types"] = ["STRING" for x in df.columns]
                elif len(table["col_types"]) != len(df.columns):
                    raise errors.FileUploadError(
                        f"{table_name} has {len(df.columns)} columns, but the provided "
                        f"col_types has {len(table['col_types'])} entries."
                    )
                self.queries.append(
                    base_templates.get_ctas_from_parquet_query(
                        schema_name=config.schema,
                        table_name=f"{manifest.get_study_prefix()}__{table_name}",
                        local_location=parquet_path,
                        remote_location=remote_path,
                        table_cols=df.columns,
                        remote_table_cols_types=table["col_types"],
                    )
                )

                progress.advance(task)
