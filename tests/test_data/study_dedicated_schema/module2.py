import cumulus_library


class ModuleTwoRunner(cumulus_library.BaseTableBuilder):
    display_text = "module2"

    def prepare_queries(self, *args, **kwargs):
        self.queries.append("CREATE VIEW IF NOT EXISTS study_dedicated_schema__view_2 (test int);")
