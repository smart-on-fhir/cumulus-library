from typing import List
from pathlib import Path
from cumulus_library.schema.counts import CountsBuilder


class CoreCountsBuilder(CountsBuilder):
    display_text = "Creating core counts..."

    def __init__(self):
        super().__init__()

    def count_core_patient(self):
        table_name = self.get_table_name("count_patient")
        from_table = self.get_table_name("patient")
        cols = ["age", "gender", "race_display", "ethnicity_display"]
        return self.count_patient(table_name, from_table, cols)

    def count_core_encounter(self, duration=None):
        table_name = self.get_table_name("count_encounter", duration=duration)
        from_table = self.get_table_name("encounter")

        cols = [
            f"start_{duration}",
            "enc_class_display",
            "age_at_visit",
            "gender",
            "race_display",
            "ethnicity_display",
        ]

        return self.count_encounter(table_name, from_table, cols)

    def _count_core_encounter_type(self, table_name, cols, duration):
        """
        Encounter Type information is for every visit, and therefore this
        SQL should be precise in which fields to select (This is a BIG query).

        :param table_name: name of the view from "core__encounter_type"
        :param cols: from "core__encounter_type"
        :param duration: None or ''month', 'year'
        :return: SQL commands
        """
        table_name = self.get_table_name(table_name, duration)
        from_table = self.get_table_name("encounter_type")

        if duration:
            cols.append(f"start_{duration}")

        where = self.get_where_clauses(min_subject=10)

        return self.count_encounter(table_name, from_table, cols, where_clauses=where)

    def count_core_encounter_type(self, duration=None):
        cols = [
            "enc_class_display",
            "enc_type_display",
            "enc_service_display",
            "enc_priority_display",
        ]
        return self._count_core_encounter_type("count_encounter_type", cols, duration)

    def count_core_encounter_enc_type(self, duration="month"):
        cols = ["enc_class_display", "enc_type_display"]
        return self._count_core_encounter_type(
            "count_encounter_enc_type", cols, duration
        )

    def count_core_encounter_service(self, duration="month"):
        cols = ["enc_class_display", "enc_service_display"]
        return self._count_core_encounter_type(
            "count_encounter_service", cols, duration
        )

    def count_core_encounter_priority(self, duration="month"):
        cols = ["enc_class_display", "enc_priority_display"]
        return self._count_core_encounter_type(
            "count_encounter_priority", cols, duration
        )

    def prepare_queries(self, cursor=None, schema=None):
        self.queries = [
            self.count_core_patient(),
            self.count_core_encounter(duration="month"),
            self.count_core_encounter_type(),
            self.count_core_encounter_type(duration="month"),
            self.count_core_encounter_enc_type(duration="month"),
            self.count_core_encounter_service(duration="month"),
            self.count_core_encounter_priority(duration="month"),
        ]


if __name__ == "__main__":
    builder = CoreCountsBuilder()
    builder.write_counts(f"{Path(__file__).resolve().parent}/count_core.sql")
