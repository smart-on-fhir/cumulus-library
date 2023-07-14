from cumulus_library.base_table_builder import BaseTableBuilder


class ModuleTwoRunner(BaseTableBuilder):
    def __init__(self):
        super().__init__()
        self.display_text = "module2"

    def prepare_queries(self, cursor: object, schema: str):
        pass
