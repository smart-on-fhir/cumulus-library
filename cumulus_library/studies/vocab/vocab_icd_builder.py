""" Module for directly loading ICD bsvs into athena tables """
import csv

from pathlib import Path

from cumulus_library.base_runner import BaseRunner
from cumulus_library.helper import query_console_output, get_progress_bar
from cumulus_library.template_sql.templates import (
    get_ctas_query,
    get_insert_into_query,
)


class VocabIcdRunner(BaseRunner):
    def run_executor(self, cursor: object, schema: str, verbose: bool):
        self.create_icd_legend(self, cursor, schema, verbose)

    @staticmethod
    def clean_row(row, filename):
        """Removes non-SQL safe charatcers from the input row."""
        for i in range(len(row)):
            cell = str(row[i]).replace("'", "").replace(";", ",")
            row[i] = cell
        return row

    def create_icd_legend(
        self, cursor: object, schema: str, verbose: bool, partition_size: int = 1200
    ):
        """input point from make.execute_sql_template.

        :param cursor: A database cursor object
        :param schema: the schema/db name, matching the cursor
        :param verbose: if true, outputs raw query, else displays progress bar
        :partition_size: number of lines to read. Athena queries have a char limit.
        """
        table_name = "vocab__icd"
        icd_files = ["ICD10CM_2023AA", "ICD10PCS_2023AA", "ICD9CM_2023AA"]
        path = Path(__file__).parent
        query_count = 1  # accounts for static CTAS query
        for filename in icd_files:
            query_count += (
                sum(1 for i in open(f"{path}/{filename}.bsv", "rb")) / partition_size
            )

        with get_progress_bar(disable=verbose) as progress:
            task = progress.add_task(
                f"Uploading {table_name} data...",
                total=query_count,
                visible=not verbose,
            )
            self.build_vocab_icd(
                self,
                cursor,
                schema,
                verbose,
                partition_size,
                table_name,
                path,
                icd_files,
                progress,
                task,
            )

    @staticmethod
    def build_vocab_icd(
        self,
        cursor,
        schema,
        verbose,
        partition_size,
        table_name,
        path,
        icd_files,
        progress,
        task,
    ):
        """Constructs queries and posts to athena."""
        headers = ["CUI", "TTY", "CODE", "SAB", "STR"]
        header_types = [f"{x} string" for x in headers]
        rows_processed = 0
        dataset = []
        created = False
        for filename in icd_files:
            with open(f"{path}/{filename}.bsv", "r") as file:
                # For the first row in the dataset, we want to coerce types from
                # varchar(len(item)) athena default to to an unrestricted varchar, so
                # we'll create a table with one row - this make the recast faster, and
                # lets us set the partition_size a little higher by limiting the
                # character bloat to keep queries under athena's limit of 262144.
                reader = csv.reader(file, delimiter="|")
                if not created:
                    row = self.clean_row(next(reader), filename)
                    ctas_query = get_ctas_query(
                        schema_name=schema,
                        table_name=table_name,
                        dataset=[row],
                        table_cols=headers,
                    )
                    cursor.execute(ctas_query)
                    query_console_output(verbose, ctas_query, progress, task)
                    created = True
                for row in reader:
                    row = self.clean_row(row, filename)
                    dataset.append(row)
                    rows_processed += 1
                    if rows_processed == partition_size:
                        insert_into_query = get_insert_into_query(
                            table_name=table_name, table_cols=headers, dataset=dataset
                        )
                        query_console_output(verbose, insert_into_query, progress, task)
                        cursor.execute(insert_into_query)
                        dataset = []
                        rows_processed = 0
                if rows_processed > 0:
                    insert_into_query = get_insert_into_query(
                        table_name=table_name, table_cols=headers, dataset=dataset
                    )
                    cursor.execute(insert_into_query)
                    query_console_output(verbose, insert_into_query, progress, task)
