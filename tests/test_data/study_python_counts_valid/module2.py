from cumulus_library.statistics.counts import CountsBuilder


class ModuleTwoRunner(CountsBuilder):
    display_text = "module2"

    def __init__(self):
        super().__init__(study_prefix="study_python_counts_valid")

    def prepare_queries(self, cursor: object, schema: str):
        self.queries.append(
            "CREATE TABLE IF NOT EXISTS study_python_counts_valid__table2 (test int);"
        )
