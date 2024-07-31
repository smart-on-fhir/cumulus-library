from cumulus_library.base_table_builder import BaseTableBuilder


class ModuleTwoRunner(BaseTableBuilder):
    display_text = "module2"

    def prepare_queries(self, *args, **kwargs):
        self.queries.append("CREATE TABLE IF NOT EXISTS study_python_s3__count_table (test int);")
