from cumulus_library.base_table_builder import BaseTableBuilder


class ModuleOneRunner(BaseTableBuilder):
    display_text = "module1"

    def prepare_queries(self, cursor: object, schema: str, *args, **kwargs):
        self.queries.append(
            "CREATE TABLE IF NOT EXISTS study_dedicated_schema__table (test int);"
        )
