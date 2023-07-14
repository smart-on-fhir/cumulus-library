from cumulus_library.base_table_builder import BaseTableBuilder


class ModuleTwoRunner(BaseTableBuilder):

    display_text = "module2"

    def prepare_queries(self, cursor: object, schema: str):
        pass
