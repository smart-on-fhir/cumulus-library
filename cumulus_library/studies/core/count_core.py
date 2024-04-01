from pathlib import Path

from cumulus_library.statistics import counts


class CoreCountsBuilder(counts.CountsBuilder):
    display_text = "Creating core counts..."

    def count_core_condition(self, duration: str = "month"):
        table_name = self.get_table_name("count_condition", duration=duration)
        from_table = self.get_table_name("condition")
        cols = [
            ["category_code", "varchar", None],
            [f"recordedDate_{duration}", "date", None],
            ["code_display", "varchar", None],
        ]
        return self.count_condition(table_name, from_table, cols)

    def count_core_documentreference(self, duration: str = "month"):
        table_name = self.get_table_name("count_documentreference", duration=duration)
        from_table = self.get_table_name("documentreference")
        cols = [
            ["type_display", "varchar", None],
            [f"author_{duration}", "date", None],
        ]
        return self.count_documentreference(table_name, from_table, cols)

    def count_core_encounter(self, duration: str | None = None):
        table_name = self.get_table_name("count_encounter", duration=duration)
        from_table = self.get_table_name("encounter")

        cols = [
            f"period_start_{duration}",
            "class_display",
            "age_at_visit",
            "gender",
            "race_display",
            "ethnicity_display",
        ]

        return self.count_encounter(table_name, from_table, cols)

    def _count_core_encounter_type(
        self, table_name: str, cols: list, duration: str | None = None
    ):
        """
        Encounter Type information is for every visit, and therefore this
        SQL should be precise in which fields to select (This is a BIG query).

        :param table_name: name of the view from "core__encounter_type"
        :param cols: from "core__encounter_type"
        :param duration: None or ''month', 'year'
        :return: A SQL statement as a string
        """
        table_name = self.get_table_name(table_name, duration)
        from_table = self.get_table_name("encounter")

        if duration:
            cols.append(f"period_start_{duration}")

        return self.count_encounter(table_name, from_table, cols)

    def count_core_encounter_all_types(self, duration: str | None = None):
        cols = [
            "class_display",
            "type_display",
            "serviceType_display",
            "priority_display",
        ]
        return self._count_core_encounter_type(
            "count_encounter_all_types", cols, duration
        )

    # The following encounter tables all count on one specific code type

    def count_core_encounter_enc_type(self, duration: str = "month"):
        cols = ["class_display", "type_display"]
        return self._count_core_encounter_type("count_encounter_type", cols, duration)

    def count_core_encounter_priority(self, duration: str = "month"):
        cols = ["class_display", "priority_display"]
        return self._count_core_encounter_type(
            "count_encounter_priority", cols, duration
        )

    def count_core_encounter_service(self, duration: str = "month"):
        cols = ["class_display", "serviceType_display"]
        return self._count_core_encounter_type(
            "count_encounter_service", cols, duration
        )

    def count_core_medicationrequest(self, duration: str = "month"):
        table_name = self.get_table_name("count_medicationrequest", duration=duration)
        from_table = self.get_table_name("medicationrequest")
        cols = ["status", "intent", f"authoredon_{duration}", "medication_display"]
        return self.count_medicationrequest(table_name, from_table, cols)

    def count_core_observation_lab(self, duration: str = "month"):
        table_name = self.get_table_name("count_observation_lab", duration=duration)
        from_table = self.get_table_name("observation_lab")
        cols = [
            f"effectiveDateTime_{duration}",
            "observation_code",
            "valueCodeableConcept_display",
        ]
        return self.count_observation(table_name, from_table, cols)

    def count_core_patient(self):
        table_name = self.get_table_name("count_patient")
        from_table = self.get_table_name("patient")
        cols = ["gender", "race_display", "ethnicity_display"]
        return self.count_patient(table_name, from_table, cols)

    def prepare_queries(self, cursor=None, schema=None, *args, **kwargs):
        self.queries = [
            self.count_core_condition(duration="month"),
            self.count_core_documentreference(duration="month"),
            self.count_core_encounter(duration="month"),
            self.count_core_encounter_all_types(),
            self.count_core_encounter_all_types(duration="month"),
            self.count_core_encounter_enc_type(duration="month"),
            self.count_core_encounter_service(duration="month"),
            self.count_core_encounter_priority(duration="month"),
            self.count_core_medicationrequest(duration="month"),
            self.count_core_observation_lab(duration="month"),
            self.count_core_patient(),
        ]


if __name__ == "__main__":
    builder = CoreCountsBuilder()
    builder.write_counts(f"{Path(__file__).resolve().parent}/count_core.sql")
