import csv
import dataclasses
import pathlib

import pandas

from cumulus_library import BaseTableBuilder, base_utils, study_manifest
from cumulus_library.builders.valueset import valueset_utils, vsac
from cumulus_library.template_sql import base_templates


@dataclasses.dataclass(kw_only=True)
class TableConfig:
    """Convenience class for holding params for configuring tables from flat files"""

    file_path: str
    delimiter: str
    table_name: str
    headers: list[str]
    dtypes: dict
    parquet_types: list[str]
    filtered_path: str | None = None
    ignore_header: bool = False
    map_cols: list[dict] | None = None


class StaticBuilder(BaseTableBuilder):
    def get_table_configs(self, prefix):
        configs = []
        if self.valueset_config.keyword_file is not None:
            configs += [
                TableConfig(
                    file_path=self.study_path / self.valueset_config.keyword_file,
                    delimiter="\t",
                    table_name=f"{prefix}keywords",
                    headers=["STR"],
                    dtypes={"STR": "str"},
                    parquet_types=["STRING"],
                    filtered_path=self.data_path / "./keywords.filtered.tsv",
                )
            ]
        if self.valueset_config.rules_file:
            rules_path = self.study_path / self.valueset_config.rules_file
        else:
            rules_path = (
                pathlib.Path(__file__).resolve().parents[0]
                / "lookup_drug_from_ingredient_rules.tsv"
            )
        # TODO: get this table to write its parquet to the valueset_data folder
        configs += [
            TableConfig(
                file_path=rules_path,
                delimiter="\t",
                table_name=f"{prefix}search_rules",
                headers=[
                    "TTY1",
                    "RELA",
                    "TTY2",
                    "rule",
                ],
                dtypes={"TTY1": "str", "RELA": "str", "TTY2": "str", "rule": "str"},
                parquet_types=["STRING", "STRING", "STRING", "STRING", "BOOLEAN"],
                ignore_header=True,
                map_cols=[
                    {
                        "from": "rule",
                        "to": "include",
                        "map_dict": {"yes": True, "no": False},
                    }
                ],
            )
        ]
        return configs

    def filter_duplicated_meds(
        self, path: pathlib.Path, delimiter: str, filtered_path: pathlib.Path
    ):
        """Given a dataset, returns the set of shortest unique substrings from that set

        As an example, given a dataset like this:
            'Hydone Formula Liquid'
            'Hydone Formula Liquid Oral Liquid Product'
            'Hydone Formula Liquid Oral Product'
            'Novahistine Expectorant'
            'Novahistine Expectorant, 10 mg-100 mg-30 mg/5 mL oral liquid'
            'Novahistine Expectorant Oral Liquid Product'
            'Novahistine Expectorant Oral Product'
            'Codeine'
            'aconite / codeine / Erysimum preparation'
            'codeine / potassium'

        This function should return:
            'Codiene'
            'Hydone Formula Liquid'
            'Novahistine Expectorant'

        :param path: path to a file (expected to be two cols, with data in second col)
        :param delimiter: the character used to separate columns in that file
        :param filtered_path: The name of the file to write the filtered dataset to

        """
        target = self.data_path / f"{path.stem}.filtered.tsv"
        with open(path) as file:
            reader = csv.reader(file, delimiter=delimiter)
            keywords = sorted((row[0] for row in reader), key=len)
        unique_keywords = {}  # casefold -> normal
        for keyword in keywords:
            folded = keyword.casefold()
            if not any(x in folded for x in unique_keywords):
                unique_keywords[folded] = keyword

        df = pandas.DataFrame(unique_keywords.values())
        df.to_csv(target, sep="\t", index=False, header=False)

    def prepare_queries(
        self,
        *args,
        config: base_utils.StudyConfig,
        manifest: study_manifest.StudyManifest,
        valueset_config: valueset_utils.ValuesetConfig,
        **kwargs,
    ):
        self.valueset_config = valueset_config
        self.study_path = manifest._study_path
        self.data_path = manifest._study_path / "valueset_data"
        prefix = self.valueset_config.table_prefix
        if prefix:
            prefix += "_"
        # fetch and add vsac tables
        self.tables = self.get_table_configs(prefix)
        vsac_df = pandas.DataFrame(columns=["rxcui", "display", "steward", "oid"])
        for key in valueset_config.vsac_stewards:
            vsac.download_oid_data(
                steward=key,
                oid=valueset_config.vsac_stewards[key],
                config=config,
                path=self.data_path,
            )
            steward_df = pandas.read_csv(
                self.data_path / f"{key}.tsv", delimiter="\t", names=["rxcui", "display"]
            )
            steward_df["steward"] = key
            steward_df["oid"] = valueset_config.vsac_stewards[key]
            vsac_df = pandas.concat([vsac_df, steward_df])
        vsac_df.to_csv(self.data_path / "all_vsac.tsv", sep="\t", index=False)
        self.tables.append(
            TableConfig(
                file_path=self.data_path / "all_vsac.tsv",
                delimiter="\t",
                table_name=f"{prefix}vsac_valuesets",
                headers=["rxcui", "str", "steward", "oid"],
                dtypes={"rxcui": "str", "display": "str", "steward": "str", "oid": "str"},
                parquet_types=["STRING", "STRING", "STRING", "STRING"],
                ignore_header=True,
            )
        )

        with base_utils.get_progress_bar() as progress:
            task = progress.add_task("Uploading static files...", total=len(self.tables))

            for table in self.tables:
                # Determine what we're using as a source file
                if table.filtered_path:
                    self.filter_duplicated_meds(
                        table.file_path, table.delimiter, table.filtered_path
                    )
                    path = self.data_path / table.filtered_path
                else:
                    path = self.data_path / table.file_path
                parquet_path = path.with_suffix(".parquet")

                # Read the file, using lots of the TableConfig params, and generate
                # a parquet file

                df = pandas.read_csv(
                    path,
                    delimiter=table.delimiter,
                    names=table.headers,
                    header=0 if table.ignore_header else None,
                    dtype=table.dtypes,
                    index_col=False,
                    na_values=["\\N"],
                )
                if table.map_cols:
                    for mapping in table.map_cols:
                        df[mapping["to"]] = df[mapping["from"]].str.lower().map(mapping["map_dict"])
                        table.headers.append(mapping["to"])
                df.to_parquet(parquet_path)
                # Upload to S3 and create a table that reads from it
                prefix = manifest.get_study_prefix()
                remote_path = config.db.upload_file(
                    file=parquet_path,
                    study=prefix,
                    topic=parquet_path.stem,
                    force_upload=config.force_upload,
                )
                self.queries.append(
                    base_templates.get_ctas_from_parquet_query(
                        schema_name=config.schema,
                        table_name=f"{prefix}__{table.table_name}",
                        local_location=parquet_path,
                        remote_location=remote_path,
                        table_cols=table.headers,
                        remote_table_cols_types=table.parquet_types,
                    )
                )
                progress.advance(task)
