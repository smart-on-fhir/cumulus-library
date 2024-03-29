import pathlib

from cumulus_library.template_sql import base_templates


def get_code_system_pairs(output_table_name: str, code_system_tables: list) -> str:
    """Extracts code system details as a standalone table"""

    # Since it's easier to wrangle data before SQL, this code block does
    # the following: given a datastructure like:
    #     [('a',list),('b', dict),('c', dict),('d', list), ('e', dict)]
    # Since data access in sql by nested dicts is just joining operators,
    # it will flatten columns together by combining dicts up to the next
    # list instance, i.e. when the next unnest would be needed:
    #     [('a',list),('b.c.d', list), ('e', dict)]
    # It also creates a display name along the way
    for table in code_system_tables:
        unnest_layer = ""
        squashed_hierarchy = []
        display_col = ""
        for column in table["column_hierarchy"]:
            unnest_layer = ".".join(x for x in [unnest_layer, column[0]] if x)
            display_col = ".".join(x for x in [display_col, column[0]] if x)
            if column[1] == list:
                squashed_hierarchy.append((unnest_layer, list))
                unnest_layer = ""
        if unnest_layer != "":
            squashed_hierarchy.append((unnest_layer, dict))
        table["column_hierarchy"] = squashed_hierarchy
        table["column_display_name"] = display_col.removesuffix(".coding")

    return base_templates.get_base_template(
        "code_system_pairs",
        pathlib.Path(__file__).parent,
        output_table_name=output_table_name,
        code_system_tables=code_system_tables,
    )
