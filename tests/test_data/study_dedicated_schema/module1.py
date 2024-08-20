import cumulus_library


class ModuleOneRunner(cumulus_library.BaseTableBuilder):
    display_text = "module1"

    def prepare_queries(self, *args, **kwargs):
        self.queries.append(
            "CREATE TABLE IF NOT EXISTS study_dedicated_schema__table_1 (test int);"
        )
