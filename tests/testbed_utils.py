"""Helper class to set up local files for testing"""

import json
from collections.abc import Iterable
from pathlib import Path

import duckdb

from cumulus_library import cli
from cumulus_library.databases import create_db_backend


class LocalTestbed:
    def __init__(self, path: Path, with_patient: bool = True):
        self.path = path
        self.indices: dict[str, int] = {}
        self.ids: dict[str, str] = {}  # cache of registered IDs, for convenience

        if with_patient:
            # Add a basic patient that other resources can link to.
            # Almost every resource needs a corresponding patient.
            self.add_patient("A")

    def add(self, table: str, obj: dict) -> None:
        index = self.indices.get(table, -1) + 1
        self.indices[table] = index

        if "id" in obj:
            self.ids.setdefault(table, set()).add(obj["id"])

        table_dir = self.path / table
        table_dir.mkdir(exist_ok=True)

        with open(table_dir / f"{index}.ndjson", "w", encoding="utf8") as f:
            json.dump(obj, f)

    # ** Now a bunch of resource-specific "add" functions.
    # They each take some kwargs that should:
    # - be absolutely necessary for the SQL to resolve to a core row
    #   (i.e. Encounter INNER JOINS with a patient,
    #   so you need to define a patient for it.
    #   But DocRef does not, so you don't)
    # - have sensible defaults for convenience,
    #   so that test authors don't need to think about it
    #
    # All other args can be specified as a kwarg, like add() itself does.

    def add_condition(self, row_id: str, recorded: str = "2020", **kwargs) -> None:
        """Adds a Condition with all the SQL-required fields filled out"""
        self.add(
            "condition",
            {
                "id": row_id,
                "recordedDate": recorded,
                **kwargs,
            },
        )

    def add_document_reference(
        self, row_id: str, start: str = "2020", **kwargs
    ) -> None:
        """Adds a DocumentReference with all the SQL-required fields filled out"""
        context = kwargs.pop("context", {})
        period = context.setdefault("period", {})
        period["start"] = start
        self.add(
            "documentreference",
            {
                "id": row_id,
                "context": context,
                **kwargs,
            },
        )

    def add_encounter(
        self, row_id: str, patient: str = "A", start: str = "2020", **kwargs
    ) -> None:
        """Adds an Encounter with all the SQL-required fields filled out"""
        period = kwargs.pop("period", {})
        period["start"] = start
        self.add(
            "encounter",
            {
                "id": row_id,
                "subject": {"reference": f"Patient/{patient}"},
                "period": period,
                **kwargs,
            },
        )

    def add_etl_completion(
        self,
        *,
        group: str,
        time: str,
        include: Iterable[str] | None = None,
        exclude: Iterable[str] | None = None,
    ) -> None:
        """Adds one or several etl__completion row"""
        if include is None:
            include = {
                # All required tables:
                "condition",
                "documentreference",
                "medicationrequest",
                "observation",
            }
        else:
            include = set(include)

        if exclude:
            include -= set(exclude)

        if len(time) == 4:  # allow just a year as a shorthand
            time = f"{time}-06-01T00:00:00Z"

        for table in include:
            self.add(
                "etl__completion",
                {
                    "group_name": group,
                    "table_name": table,
                    "export_time": time,
                },
            )

    def add_etl_completion_encounters(
        self, *, group: str, ids: Iterable[str], time: str
    ) -> None:
        """Adds rows to etl__completion_encounters"""

        if len(time) == 4:  # allow just a year as a shorthand
            time = f"{time}-06-01T00:00:00Z"

        for encounter_id in ids:
            self.add(
                "etl__completion_encounters",
                {
                    "group_name": group,
                    "encounter_id": encounter_id,
                    "export_time": time,
                },
            )

    def add_observation(self, row_id: str, effective: str = "2020", **kwargs) -> None:
        """Adds a Observation with all the SQL-required fields filled out"""
        self.add(
            "observation",
            {
                "id": row_id,
                "effectiveDateTime": effective,
                **kwargs,
            },
        )

    def add_patient(
        self, row_id: str, birth_date: str = "2000", gender: str = "unknown", **kwargs
    ) -> None:
        """Adds a Patient with all the SQL-required fields filled out"""
        self.add(
            "patient",
            {
                "id": row_id,
                "birthDate": birth_date,
                "gender": gender,
                # TODO: fix the core SQL to check for extensions in the schema
                #  before querying them. In the meantime, we can just ensure
                #  those fields exist, ready to be queried.
                "extension": [
                    {
                        "url": "",
                        "extension": [
                            {
                                "url": "",
                                "valueCoding": {
                                    "code": "",
                                    "display": "",
                                },
                            }
                        ],
                    }
                ],
                **kwargs,
            },
        )

    def build(self, study="core") -> duckdb.DuckDBPyConnection:
        db_file = f"{self.path}/{study}.db"
        db = create_db_backend(
            {
                "db_type": "duckdb",
                "schema_name": db_file,
                "load_ndjson_dir": str(self.path),
            }
        )
        builder = cli.StudyRunner(db, data_path=str(self.path))
        # builder.verbose = True
        builder.clean_and_build_study(
            Path(__file__).parent.parent / "cumulus_library/studies" / study,
            stats_build=False,
        )
        return duckdb.connect(db_file)
