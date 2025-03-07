from study_python_local_template import local_template

import cumulus_library


class ModuleOneRunner(cumulus_library.BaseTableBuilder):
    display_text = "module1"

    def prepare_queries(self, *args, **kwargs):
        self.queries.append(local_template.get_local_template("table", config={"field": "foo"}))
