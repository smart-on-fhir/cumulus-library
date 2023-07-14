from cumulus_library.base_table_builder import BaseTableBuilder


class ModuleOneRunner(BaseTableBuilder):
    def __init__(self):
        super().__init__()
        self.display_text = "Module 1"

    @classmethod
    def prepare_queries(self, cursor: object, schema: str):
        pass
