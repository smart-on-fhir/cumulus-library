from pathlib import Path
from rich.progress import Progress
from cumulus_library.helper import query_console_output
from cumulus_library.templates import (
    get_drop_table,
)


def execute_templates(cursor, schema: str, verbose: bool):
    tables = ["default.icd_legend", "icd_legend", "umls_icd"]
    query_count = len(tables)
    path = Path(__file__).parent
    if verbose:
        build_queries(cursor, schema, verbose, tables, path, None, None)
    else:
        with Progress() as progress:
            task = progress.add_task(f"Removing UMLS tables...", total=query_count)
            build_queries(
                cursor,
                schema,
                verbose,
                tables,
                path,
                progress,
                task,
            )


def build_queries(cursor, schema, verbose, tables, path, progress, task):
    """Constructs queries and posts to athena."""
    for table_name in tables:
        drop_table = get_drop_table(table_name=table_name)
        cursor.execute(drop_table)
        query_console_output(verbose, drop_table, progress, task)
