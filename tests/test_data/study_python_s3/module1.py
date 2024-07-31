from cumulus_library.base_table_builder import BaseTableBuilder


class ModuleOneRunner(BaseTableBuilder):
    display_text = "module1"

    def prepare_queries(self, *args, **kwargs):
        self.queries.append("""
CREATE EXTERNAL TABLE IF NOT EXISTS `schema`.`study_python_s3__table` (
    STR STRING
)
STORED AS PARQUET
LOCATION 's3://specific_bucket/databases/db1/study_python_s3__table/keywords.filtered'
tblproperties ("parquet.compression"="SNAPPY");""")
