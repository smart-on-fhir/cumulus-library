import cumulus_library


class ModuleTwoRunner(cumulus_library.BaseTableBuilder):
    display_text = "module2"

    def prepare_queries(self, *args, **kwargs):
        self.queries.append(
            "CREATE TABLE IF NOT EXISTS study_python_valid__count_table (test int);"
        )
