from cumulus_library.base_table_builder import BaseTableBuilder


class ModuleOneRunner(BaseTableBuilder):
    display_text = "Module 1"

    @classmethod
    def prepare_queries(self, *args, **kwargs):
        pass
