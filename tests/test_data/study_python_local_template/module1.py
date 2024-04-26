from cumulus_library.base_table_builder import BaseTableBuilder
from tests.test_data.study_python_local_template import local_template


class ModuleOneRunner(BaseTableBuilder):
    display_text = "module1"

    def prepare_queries(self, cursor: object, schema: str, *args, **kwargs):
        self.queries.append(
            local_template.get_local_template("table", config={"field": "foo"})
        )
