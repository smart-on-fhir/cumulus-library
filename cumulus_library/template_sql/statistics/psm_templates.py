""" Collection of jinja template getters for common SQL queries """
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional

from jinja2 import Template
from pandas import DataFrame

from cumulus_library.errors import CumulusLibraryError


class ExtensionConfig(object):
    """convenience class for holding parameters for generating extension tables.

    :param source_table: the table to extract extensions from
    :param source_id: the id column to treat as a foreign key
    :param target_table: the name of the table to create
    :param target_col_prefix: the string to prepend code/display column names with
    :param fhir_extension: the URL of the FHIR resource to select
    :param code_systems: a list of codes, in preference order, to use to select data
    :param is_array: a boolean indicating if the targeted field is an array type
    """

    def __init__(
        self,
        source_table: str,
        source_id: str,
        target_table: str,
        target_col_prefix: str,
        fhir_extension: str,
        ext_systems: List[str],
        is_array: bool = False,
    ):
        self.source_table = source_table
        self.source_id = source_id
        self.target_table = target_table
        self.target_col_prefix = target_col_prefix
        self.fhir_extension = fhir_extension
        self.ext_systems = ext_systems
        self.is_array = is_array


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
    """ """
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


def get_extension_denormalize_query(config: ExtensionConfig) -> str:
    """extracts target extension from a table into a denormalized table

    This function is targeted at a complex extension element that is at the root
    of a FHIR resource - as an example, see the 5 codes at the root node of
    http://hl7.org/fhir/us/core/STU6/StructureDefinition-us-core-patient.html.
    The template will create a new table with the extension data, in arrays,
    mapped 1-1 to the table id. You can specify multiple systems
    in the ExtensionConfig passed to this function. For each patient, we'll
    take the data from the first extension coding system we find for each patient.

    :param config: An instance of ExtensionConfig.
    """
    path = Path(__file__).parent
    with open(f"{path}/extension_denormalize.sql.jinja") as extension_denormalize:
        return Template(extension_denormalize.read()).render(
            source_table=config.source_table,
            source_id=config.source_id,
            target_table=config.target_table,
            target_col_prefix=config.target_col_prefix,
            fhir_extension=config.fhir_extension,
            ext_systems=config.ext_systems,
            is_array=config.is_array,
        )
