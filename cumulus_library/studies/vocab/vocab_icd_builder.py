"""Module for directly loading ICD bsvs into athena tables"""

import pathlib

import pandas

import cumulus_library
from cumulus_library.template_sql import base_templates


class VocabIcdRunner(cumulus_library.BaseTableBuilder):
    display_text = "Creating ICD vocab..."

    def prepare_queries(
        self,
        config: cumulus_library.StudyConfig,
        *args,
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
                schema_name=config.schema,
                table_name=table_name,
                local_location=path / "icd",
                remote_location=remote_path,
                table_cols=headers,
                remote_table_cols_types=header_types,
            )
        )
