"""Sets study metadata"""

import cumulus_library


class DynamicMeta(cumulus_library.BaseTableBuilder):
    def prepare_queries(self, *args, manifest: cumulus_library.StudyManifest, **kwargs):
        prefix = manifest.get_study_prefix()
        self.queries.append(
            f"CREATE TABLE {prefix}__meta_version AS SELECT 1 AS data_package_version;"
        )
