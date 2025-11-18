import cumulus_library


class ModuleOneRunner(cumulus_library.BaseTableBuilder):
    display_text = "module3"

    def prepare_queries(self, *args, **kwargs):
        self.queries.append(
            "CREATE TABLE IF NOT EXISTS study_python_valid_parallel__table3 (test int);"
        )
