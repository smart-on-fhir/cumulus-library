import cumulus_library


class ModuleOneRunner(cumulus_library.CountsBuilder):
    display_text = "module1"

    def prepare_queries(self, *args, **kwargs):
        self.queries.append(
            "CREATE TABLE IF NOT EXISTS study_python_counts_valid__table1 (test int);"
        )
