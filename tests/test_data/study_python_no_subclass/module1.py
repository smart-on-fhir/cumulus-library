import cumulus_library


class ModuleOneRunner(cumulus_library.BaseTableBuilder):
    display_text = "Module 1"

    def prepare_queries(self, *args, **kwargs):
        self.queries.append("CREATE TABLE test_python__module_1 (test int)")
