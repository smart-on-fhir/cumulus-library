from cumulus_library.schema.counts import CountsBuilder


class ModuleTwoRunner(CountsBuilder):
    display_text = "module2"

    def prepare_queries(self, cursor: object, schema: str):
        pass
