""" Collection of jinja template getters for common SQL queries """
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional

from jinja2 import Template
from pandas import DataFrame

from cumulus_library.errors import CumulusLibraryError


def get_distinct_ids(
    columns: list[str], source_table: str, join_id: str = None, filter_table: str = None
) -> str:
    """Gets distinct ids from a table, optionally filtering by ids in another table

    This is expected to be used in two ways:
    - To retrieve ids from a study cohort table
    - To retreive ids from a core FHIR table, excluding ids in a cohort study table

    It is expected that, if supplying optional parameters, all of them must be set

    :param columns: a list of ids to request
    :param source_table: the table to retrieve ids from
    :param join_id: the id column to use for joining. Expected to exist in both source and filter tables
    :param filter_table: a table containing ids you want to exclude
    """
    if (join_id is None and filter_table is not None) or (
        join_id is not None and filter_table is None
    ):
        raise CumulusLibraryError(
            "psm_templates.get_distinct_ids expects all optional parameters to be defined if supplied"
        )

    path = Path(__file__).parent
    with open(f"{path}/psm_distinct_ids.sql.jinja") as distinct_ids:
        return Template(distinct_ids.read()).render(
            columns=columns,
            source_table=source_table,
            join_id=join_id,
            filter_table=filter_table,
        )


def get_create_covariate_table(
    target_table: str,
    pos_source_table: str,
    neg_source_table: str,
    primary_ref: str,
    dependent_variable: str,
    join_cols_by_table: dict,
    count_ref: str = None,
    count_table: str = None,
) -> str:
    """Gets a query to create a covariate table for PSM analysis

    :param target_table: the name of the table to create
    :param pos_source_table: the table defining your positive cohort
    :param neg_source_table: the table you defined your positive cohort against,
        containing the full population
    :primary_ref: the ID field to use for IDing members of your cohort
    :dependent_variable: the name for the condition you are investigating, which
        will differentiate your positive and negative sources
    :join_cols_by_table: A dict defining extra data to join. See the psm_config.toml
        in tests/test_data/psm for more details
    :count_ref: optional ID to count records with for validation
    :count_table: optional table to use as the source of the count_refs
    """
    if (count_ref is None and count_table is not None) or (
        count_ref is not None and count_table is None
    ):
        raise CumulusLibraryError(
            "psm_templates.get_create_covariate_table expects all count parameters to be defined if supplied"
        )

    path = Path(__file__).parent
    with open(f"{path}/psm_create_covariate_table.sql.jinja") as create_covariate_table:
        return Template(create_covariate_table.read()).render(
            target_table=target_table,
            pos_source_table=pos_source_table,
            neg_source_table=neg_source_table,
            primary_ref=primary_ref,
            dependent_variable=dependent_variable,
            count_ref=count_ref,
            count_table=count_table,
            join_cols_by_table=join_cols_by_table,
        )