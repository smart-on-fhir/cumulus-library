from cumulus_library.schema.counts import CountsBuilder


class ModuleOneRunner(CountsBuilder):
    display_text = "module1"

    def prepare_queries(self, cursor: object, schema: str):
        pass
