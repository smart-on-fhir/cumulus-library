"""Module for directly loading ICD bsvs into athena tables"""

import pathlib

import pandas

from cumulus_library import base_table_builder, base_utils
from cumulus_library.template_sql import base_templates


class VocabIcdRunner(base_table_builder.BaseTableBuilder):
    display_text = "Creating ICD vocab..."
    partition_size = 1200

    @staticmethod
    def clean_row(row, filename):
        """Removes non-SQL safe charatcers from the input row."""
        for i in range(len(row)):
            cell = str(row[i]).replace("'", "").replace(";", ",")
            row[i] = cell
        return row

    def prepare_queries(
        self,
        cursor: object,
        schema: str,
        *args,
        config: base_utils.StudyConfig,
        **kwargs,
    ):
        """Creates queries for populating ICD vocab

        TODO: this would be a lot faster if we converted the bsv to parquet,
        uploaded that, and then created the table from an external datasource

        :param cursor: A database cursor object
        :param schema: the schema/db name, matching the cursor
        """

        table_name = "vocab__icd"
        path = pathlib.Path(__file__).parent
        icd_files = path.glob("icd/*.bsv")
        headers = ["CUI", "TTY", "CODE", "SAB", "STR"]
        header_types = ["STRING", "STRING", "STRING", "STRING", "STRING"]
        for file in icd_files:
            parquet_path = path / f"icd/{file.stem}.parquet"
            if not parquet_path.is_file():
                df = pandas.read_csv(file, delimiter="|", names=headers)
                df.to_parquet(parquet_path)
            remote_path = config.db.upload_file(
                file=parquet_path,
                study="vocab",
                topic="icd",
                remote_filename=f"{file.stem}.parquet",
                force_upload=config.force_upload,
            )
        # Since we are building one table from these three files, it's fine to just
        # use the last value of remote location
        self.queries.append(
            base_templates.get_ctas_from_parquet_query(
                schema_name=schema,
                table_name=table_name,
                local_location=path / "icd",
                remote_location=remote_path,
                table_cols=headers,
                remote_table_cols_types=header_types,
            )
        )
