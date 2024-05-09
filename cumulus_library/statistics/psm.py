# Module for generating Propensity Score matching cohorts

import json
import os
import pathlib
import sys
import warnings
from dataclasses import dataclass

import matplotlib.pyplot as plt
import pandas
import seaborn as sns
import toml
from psmpy import PsmPy

# these imports are mimicing PsmPy imports for re-implemented functions
from psmpy.functions import cohenD

from cumulus_library import base_table_builder, databases
from cumulus_library.statistics.statistics_templates import psm_templates
from cumulus_library.template_sql import base_templates


@dataclass
class PsmConfig:
    """Provides expected values for PSM execution

    These values should be read in from a toml configuration file.
    See docs/statistics/propensity-score-matching.md for an example with details about
    the expected values for these fields.

    A word of caution about sampling: the assumptions around PSM analysis require
    that any sampling should not use replacement, so do not turn on panda's dataframe
    replacement. This will mean that very small population sizes (i.e. < 20ish)
    may cause errors to be generated.
    """

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
    seed: int


class PsmBuilder(base_table_builder.BaseTableBuilder):
    """TableBuilder for creating PSM tables"""

    display_text = "Building PSM tables..."

    def __init__(self, toml_config_path: str, data_path: pathlib.Path, **kwargs):
        """Loads PSM job details from a PSM configuration file"""
        super().__init__()
        # We're stashing the toml path for error reporting later
        self.toml_path = toml_config_path
        self.data_path = data_path
        try:
            with open(self.toml_path, encoding="UTF-8") as file:
                toml_config = toml.load(file)

        except OSError:
            sys.exit(f"PSM configuration not found at {self.toml_path}")
        try:
            self.config = PsmConfig(
                classification_json=f"{pathlib.Path(self.toml_path).parent}/{toml_config['classification_json']}",
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
        except KeyError:
            sys.exit(
                f"PSM configuration {toml_config_path} contains missing/invalid keys."
                "Check the PSM documentation for an example config with more details:\n"
                "https://docs.smarthealthit.org/cumulus/library/statistics/propensity-score-matching.html"
            )

    def _get_symptoms_dict(self, path: str) -> dict:
        """convenience function for loading symptoms dictionaries from a json file"""
        with open(path, encoding="UTF-8") as f:
            symptoms = json.load(f)
        return symptoms

    def _get_sampled_ids(
        self,
        cursor: databases.DatabaseCursor,
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
        df = df.sort_values(by=[self.config.primary_ref])
        df = (
            df.sample(n=sample_size, random_state=self.config.seed)
            .sort_values(by=[self.config.primary_ref])
            .reset_index()
            .drop("index", axis=1)
        )

        df[dependent_variable] = is_positive
        return df

    def _create_covariate_table(
        self, cursor: databases.DatabaseCursor, schema: str, table_suffix: str
    ):
        """Creates a covariate table from the loaded toml config"""
        # checks for primary & link ref being the same
        source_refs = list({self.config.primary_ref, self.config.count_ref} - {None})
        pos_query = psm_templates.get_distinct_ids(
            source_refs, self.config.pos_source_table
        )
        pos = self._get_sampled_ids(
            cursor,
            schema,
            pos_query,
            self.config.pos_sample_size,
            self.config.dependent_variable,
            1,
        )
        neg_query = psm_templates.get_distinct_ids(
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
        ctas_query = base_templates.get_ctas_query_from_df(
            schema,
            f"{self.config.pos_source_table}_sampled_ids_{table_suffix}",
            cohort,
        )
        self.queries.append(ctas_query)

        dataset_query = psm_templates.get_create_covariate_table(
            target_table=f"{self.config.target_table}_{table_suffix}",
            pos_source_table=self.config.pos_source_table,
            neg_source_table=self.config.neg_source_table,
            table_suffix=table_suffix,
            primary_ref=self.config.primary_ref,
            dependent_variable=self.config.dependent_variable,
            join_cols_by_table=self.config.join_cols_by_table,
            count_ref=self.config.count_ref,
            count_table=self.config.count_table,
        )
        self.queries.append(dataset_query)

    def psm_plot_match(
        self,
        psm,
        matched_entity="propensity_logit",
        Title="Side by side matched controls",
        Ylabel="Number of patients",
        Xlabel="propensity logit",
        names=["positive_cohort", "negative_cohort"],  # noqa: B006
        colors=["#E69F00", "#56B4E9"],  # noqa: B006
        save=True,
        filename="propensity_match.png",
    ):
        """Plots knn match data

        This function re-implements psm.plot_match, with the only changes
        allowing for specifiying a filename/location for saving plots to,
        and passing in the psm object instead of assuming a call from inside
        the PsmPy class.
        """
        dftreat = psm.df_matched[psm.df_matched[psm.treatment] == 1]
        dfcontrol = psm.df_matched[psm.df_matched[psm.treatment] == 0]
        x1 = dftreat[matched_entity]
        x2 = dfcontrol[matched_entity]
        colors = colors
        names = names
        sns.set_style("white")
        plt.hist([x1, x2], color=colors, label=names)
        plt.legend()
        plt.xlabel(Xlabel)
        plt.ylabel(Ylabel)
        plt.title(Title)
        if save:
            plt.savefig(filename, dpi=250)

    def psm_effect_size_plot(
        self,
        psm,
        title="Standardized Mean differences accross covariates before and after matching",  # noqa: E501
        before_color="#FCB754",
        after_color="#3EC8FB",
        save=False,
        filename="effect_size.png",
    ):
        """Plots effect size of variables for positive/negative matches

        This function re-implements psm.effect_size_plot, with the only changes
        allowing for specifiying a filename/location for saving plots to,
        and passing in the psm object instead of assuming a call from inside
        the PsmPy class.
        """
        df_preds_after = psm.df_matched[[psm.treatment] + psm.xvars]  # noqa: RUF005
        df_preds_b4 = psm.data[[psm.treatment] + psm.xvars]  # noqa: RUF005
        df_preds_after_float = df_preds_after.astype(float)
        df_preds_b4_float = df_preds_b4.astype(float)

        data = []
        for cl in psm.xvars:
            data.append([cl, "before", cohenD(df_preds_b4_float, psm.treatment, cl)])
            data.append([cl, "after", cohenD(df_preds_after_float, psm.treatment, cl)])
        psm.effect_size = pandas.DataFrame(
            data, columns=["Variable", "matching", "Effect Size"]
        )
        sns.set_style("white")
        sns_plot = sns.barplot(
            data=psm.effect_size,
            y="Variable",
            x="Effect Size",
            hue="matching",
            palette=[before_color, after_color],
            orient="h",
        )
        sns_plot.set(title=title)
        if save:
            sns_plot.figure.savefig(filename, dpi=250, bbox_inches="tight")

    def generate_psm_analysis(
        self, cursor: databases.DatabaseCursor, schema: str, table_suffix: str
    ):
        stats_table = f"{self.config.target_table}_{table_suffix}"
        """Runs PSM statistics on generated tables"""
        cursor.execute(
            base_templates.get_alias_table_query(stats_table, self.config.target_table)
        )
        df = cursor.execute(
            base_templates.get_select_all_query(self.config.target_table)
        ).as_pandas()
        symptoms_dict = self._get_symptoms_dict(self.config.classification_json)
        for dependent_variable, codes in symptoms_dict.items():
            df[dependent_variable] = df["code"].apply(
                lambda x, codes=codes: 1 if x in codes else 0
            )
        df = df.drop(columns="code")
        # instance_count present but unused for PSM if table contains a count_ref input
        # (it's intended for manual review)
        df = df.drop(columns="instance_count", errors="ignore")

        columns = []
        if self.config.join_cols_by_table is not None:
            for table_config in self.config.join_cols_by_table.values():
                for column in table_config["included_cols"]:
                    # If there are two elements, it's a SQL column that has been
                    # aliased, so we'll look for the alias name
                    if len(column) == 2:
                        columns.append(column[1])
                    # If there is one element, it's a straight SQL column we can
                    # use with no modification
                    elif len(column) == 1:
                        columns.append(column[0])
                    else:
                        sys.exit(
                            f"PSM config at {self.toml_path} contains an "
                            f"unexpected SQL column definition: {column}."
                            "Check the PSM documentation for valid usages."
                        )

        # This code block is replacing a column which may contain several categories
        # (like male/female/other/unknown for AdministrativeGender), and converts
        # it into a series of 1-hot columns for each distinct value in that column,
        for column in columns:
            encoded_df = pandas.get_dummies(df[column])
            df = pandas.concat([df, encoded_df], axis=1)
            df = df.drop(column, axis=1)
        try:
            psm = PsmPy(
                df,
                treatment=self.config.dependent_variable,
                indx=self.config.primary_ref,
                exclude=[],
            )

            # we expect psmpy to autodrop non-matching values, so we'll surpress it
            # mentioning workarounds for this behavior.
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", category=UserWarning)
                # This function populates the psm.predicted_data element, which is
                # required for things like the knn_matched() function call
                psm.logistic_ps(balance=True)
                # This function populates the psm.df_matched element
                psm.knn_matched(
                    matcher="propensity_logit",
                    replacement=False,
                    caliper=None,
                    drop_unmatched=True,
                )
                os.makedirs(self.data_path, exist_ok=True)
                self.psm_plot_match(
                    psm,
                    save=True,
                    filename=self.data_path / f"{stats_table}_propensity_match.png",
                )
                self.psm_effect_size_plot(
                    psm,
                    save=True,
                    filename=self.data_path / f"{stats_table}_effect_size.png",
                )
        except ZeroDivisionError:
            sys.exit(
                "Encountered a divide by zero error during statistical graph "
                "generation. Try increasing your sample size."
            )
        except ValueError:
            sys.exit(
                "Encountered a value error during KNN matching. Try increasing "
                "your sample size."
            )

    def prepare_queries(
        self, cursor: object, schema: str, table_suffix: str, *args, **kwargs
    ):
        self._create_covariate_table(cursor, schema, table_suffix)

    def post_execution(
        self,
        cursor: object,
        schema: str,
        *args,
        verbose: bool = False,
        drop_table: bool = False,
        table_suffix: str | None = None,
        **kwargs,
    ):
        # super().execute_queries(cursor, schema, verbose, drop_table)
        self.generate_psm_analysis(cursor, schema, table_suffix)
