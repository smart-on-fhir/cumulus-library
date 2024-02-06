""" Module for directly loading ICD bsvs into athena tables """

import csv
import pathlib

from cumulus_library import base_table_builder
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

    def prepare_queries(self, cursor: object, schema: str, *args, **kwargs):
        """Creates queries for populating ICD vocab

        TODO: this would be a lot faster if we converted the bsv to parquet,
        uploaded that, and then created the table from an external datasource

        :param cursor: A database cursor object
        :param schema: the schema/db name, matching the cursor
        """

        table_name = "vocab__icd"
        icd_files = ["ICD10CM_2023AA", "ICD10PCS_2023AA", "ICD9CM_2023AA"]
        path = pathlib.Path(__file__).parent

        headers = ["CUI", "TTY", "CODE", "SAB", "STR"]
        rows_processed = 0
        dataset = []
        created = False
        for filename in icd_files:
            with open(f"{path}/{filename}.bsv") as file:
                # For the first row in the dataset, we want to coerce types from
                # varchar(len(item)) athena default to to an unrestricted varchar, so
                # we'll create a table with one row - this make the recast faster, and
                # lets us set the partition_size a little higher by limiting the
                # character bloat to keep queries under athena's limit of 262144.
                reader = csv.reader(file, delimiter="|")
                if not created:
                    row = self.clean_row(next(reader), filename)
                    self.queries.append(
                        base_templates.get_ctas_query(
                            schema_name=schema,
                            table_name=table_name,
                            dataset=[row],
                            table_cols=headers,
                        )
                    )
                    created = True
                for row in reader:
                    row = self.clean_row(row, filename)
                    dataset.append(row)
                    rows_processed += 1
                    if rows_processed == self.partition_size:
                        self.queries.append(
                            base_templates.get_insert_into_query(
                                table_name=table_name,
                                table_cols=headers,
                                dataset=dataset,
                            )
                        )
                        dataset = []
                        rows_processed = 0
                if rows_processed > 0:
                    self.queries.append(
                        base_templates.get_insert_into_query(
                            table_name=table_name, table_cols=headers, dataset=dataset
                        )
                    )
