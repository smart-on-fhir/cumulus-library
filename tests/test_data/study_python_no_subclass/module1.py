import cumulus_library


class ModuleOneRunner(cumulus_library.BaseTableBuilder):
    display_text = "Module 1"

    @classmethod
    def prepare_queries(self, *args, **kwargs):
        pass
