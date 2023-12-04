# Module for generating Propensity Score matching cohorts

import numpy as np
import pandas
import toml

from psmpy import PsmPy


import json
from pathlib import PosixPath
from dataclasses import dataclass

from cumulus_library.cli import StudyBuilder
from cumulus_library.databases import DatabaseCursor
from cumulus_library.base_table_builder import BaseTableBuilder
from cumulus_library.template_sql.templates import (
    get_ctas_query_from_df,
    get_drop_view_table,
)
from cumulus_library.template_sql.statistics.psm_templates import (
    get_distinct_ids,
    get_create_covariate_table,
)


@dataclass
class PsmConfig:
    """Provides expected values for PSM execution"""

    classification_json: str
    pos_source_table: str
    neg_source_table: str
    target_table: str
    primary_ref: str
    count_ref: str
    count_table: str
    dependent_variable: str
    pos_sample_size: int
    neg_sample_size: int
    join_cols_by_table: dict[str, dict]
    seed: int = 1234567890


class PsmBuilder(BaseTableBuilder):
    """TableBuilder for creating PSM tables"""

    display_text = "Building PSM tables..."

    def __init__(self, toml_config_path: str):
        """Loads PSM job details from a psm TOML file"""
        with open(toml_config_path, encoding="UTF-8") as file:
            toml_config = toml.load(file)
        self.config = PsmConfig(
            classification_json=f"{PosixPath(toml_config_path).parent}/{toml_config['classification_json']}",
            pos_source_table=toml_config["pos_source_table"],
            neg_source_table=toml_config["neg_source_table"],
            target_table=toml_config["target_table"],
            primary_ref=toml_config["primary_ref"],
            dependent_variable=toml_config["dependent_variable"],
            pos_sample_size=toml_config["pos_sample_size"],
            neg_sample_size=toml_config["neg_sample_size"],
            join_cols_by_table=toml_config.get("join_cols_by_table", {}),
            count_ref=toml_config.get("count_ref", None),
            count_table=toml_config.get("count_table", None),
            seed=toml_config.get("seed", 123),
        )
        super().__init__()

    def _get_symptoms_dict(self, path: str) -> dict:
        """convenience function for loading symptoms dictionaries from a json file"""
        with open(path) as f:
            symptoms = json.load(f)
        return symptoms

    def _get_sampled_ids(
        self,
        cursor: DatabaseCursor,
        schema: str,
        query: str,
        sample_size: int,
        dependent_variable: str,
        is_positive: bool,
    ):
        """Creates a table containing randomly sampled patients for PSM analysis

        To use this, it is assumed you have already identified a cohort of positively
        IDed patients as a manual process.
        :param cursor: A valid DatabaseCusror:
        :param schema: the schema/database name where the data exists
        :param query: a query generated from the psm_dsitinct_ids template
        :param sample_size: the number of records to include in the random sample.
            This should generally be >= 20.
        :param dependent_variable: the name to use for your filtering column
        :param is_positive: defines the value to be used for your filtering column
        """
        df = cursor.execute(query).as_pandas()
        df = (
            df.sort_values(by=[self.config.primary_ref])
            .reset_index()
            .drop("index", axis=1)
        )
        df = (
            # TODO: remove replace behavior after increasing data sample size
            df.sample(n=sample_size, random_state=self.config.seed, replace=True)
            .sort_values(by=[self.config.primary_ref])
            .reset_index()
            .drop("index", axis=1)
        )

        df[dependent_variable] = is_positive
        return df

    def _create_covariate_table(self, cursor: DatabaseCursor, schema: str):
        """Creates a covariate table from the loaded toml config"""
        # checks for primary & link ref being the same
        source_refs = list(
            {self.config.primary_ref, self.config.count_ref} - set([None])
        )
        pos_query = get_distinct_ids(source_refs, self.config.pos_source_table)
        pos = self._get_sampled_ids(
            cursor,
            schema,
            pos_query,
            self.config.pos_sample_size,
            self.config.dependent_variable,
            1,
        )
        neg_query = get_distinct_ids(
            source_refs,
            self.config.neg_source_table,
            join_id=self.config.primary_ref,
            filter_table=self.config.pos_source_table,
        )
        neg = self._get_sampled_ids(
            cursor,
            schema,
            neg_query,
            self.config.neg_sample_size,
            self.config.dependent_variable,
            0,
        )
        cohort = pandas.concat([pos, neg])
        # Replace table (if it exists)
        # TODO - replace with timestamp prepended table
        drop = get_drop_view_table(
            f"{self.config.pos_source_table}_sampled_ids", "TABLE"
        )
        cursor.execute(drop)
        ctas_query = get_ctas_query_from_df(
            schema,
            f"{self.config.pos_source_table}_sampled_ids",
            cohort,
        )
        self.queries.append(ctas_query)
        # TODO - replace with timestamp prepended table
        drop = get_drop_view_table(self.config.target_table, "TABLE")
        cursor.execute(drop)
        dataset_query = get_create_covariate_table(
            target_table=self.config.target_table,
            pos_source_table=self.config.pos_source_table,
            neg_source_table=self.config.neg_source_table,
            primary_ref=self.config.primary_ref,
            dependent_variable=self.config.dependent_variable,
            join_cols_by_table=self.config.join_cols_by_table,
            count_ref=self.config.count_ref,
            count_table=self.config.count_table,
        )
        self.queries.append(dataset_query)

    def generate_psm_analysis(self, cursor: object, schema: str):
        """Runs PSM statistics on generated tables"""
        df = cursor.execute(f"select * from {self.config.target_table}").as_pandas()
        symptoms_dict = self._get_symptoms_dict(self.config.classification_json)
        for dependent_variable, codes in symptoms_dict.items():
            df[dependent_variable] = df["code"].apply(lambda x: 1 if x in codes else 0)
        df = df.drop(columns="code")
        # instance_count present but unused for PSM if table contains a count_ref input
        df = df.drop(columns="instance_count", errors="ignore")
        columns = []
        if self.config.join_cols_by_table is not None:
            for table_key in self.config.join_cols_by_table:
                for column in self.config.join_cols_by_table[table_key][
                    "included_cols"
                ]:
                    if len(column) == 2:
                        columns.append(column[1])
                    else:
                        columns.append(column[0])
        for column in columns:
            encoded_df = pandas.get_dummies(df[column])
            df = pandas.concat([df, encoded_df], axis=1)
            df = df.drop(column, axis=1)
        df = df.reset_index()
        try:
            psm = PsmPy(
                df,
                treatment=self.config.dependent_variable,
                indx=self.config.primary_ref,
                exclude=[],
            )
            # This function populates the psm.predicted_data element, which is required
            # for things like the knn_matched() function call
            psm.logistic_ps(balance=True)
            print(psm.predicted_data)
            # This function populates the psm.df_matched element
            # TODO: flip replacement to false after increasing sample data size
            psm.knn_matched(
                matcher="propensity_logit",
                replacement=True,
                caliper=None,
                drop_unmatched=True,
            )
            print(psm.df_matched)
        except ZeroDivisionError:
            print(
                "Encountered a divide by zero error during statistical graph generation. Try increasing your sample size."
            )
        except ValueError:
            print(
                "Encountered a value error during KNN matching. Try increasing your sample size."
            )

    def prepare_queries(self, cursor: object, schema: str):
        self._create_covariate_table(cursor, schema)

    def execute_queries(
        self,
        cursor: object,
        schema: str,
        verbose: bool,
        drop_table: bool = False,
    ):
        super().execute_queries(cursor, schema, verbose, drop_table)
        self.comment_queries()
        self.write_queries()
        self.generate_psm_analysis(cursor, schema)
