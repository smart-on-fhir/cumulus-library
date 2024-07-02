from cumulus_library.statistics.counts import CountsBuilder


class ModuleOneRunner(CountsBuilder):
    display_text = "module1"

    def __init__(self):
        super().__init__()

    def prepare_queries(self, *args, **kwargs):
        self.queries.append(
            "CREATE TABLE IF NOT EXISTS study_python_counts_valid__table1 (test int);"
        )
