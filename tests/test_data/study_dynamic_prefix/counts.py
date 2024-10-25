import cumulus_library


class DynamicCounts(cumulus_library.CountsBuilder):
    def prepare_queries(self, *args, **kwargs):
        super().prepare_queries(*args, **kwargs)
        table_name = self.get_table_name("counts")
        self.queries.append(f"CREATE TABLE IF NOT EXISTS {table_name} (test int);")
