"""Helper class to set up local files for testing"""

import json
from collections.abc import Iterable
from pathlib import Path

import duckdb

from cumulus_library import base_utils, cli
from cumulus_library.databases import create_db_backend


class LocalTestbed:
    def __init__(self, path: Path, with_patient: bool = True):
        self.path = path
        self.indices: dict[str, int] = {}
        self.ids: dict[str, set[str]] = {}  # cache of registered IDs, for convenience

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

    def add_allergy_intolerance(self, row_id: str, recorded: str = "2020", **kwargs) -> None:
        """Adds a AllergyIntolerance with all the SQL-required fields filled out"""
        self.add(
            "allergyintolerance",
            {
                "resourceType": "AllergyIntolerance",
                "id": row_id,
                "recordedDate": recorded,
                **kwargs,
            },
        )

    def add_condition(self, row_id: str, recorded: str = "2020", **kwargs) -> None:
        """Adds a Condition with all the SQL-required fields filled out"""
        self.add(
            "condition",
            {
                "resourceType": "Condition",
                "id": row_id,
                "recordedDate": recorded,
                **kwargs,
            },
        )

    def add_diagnostic_report(self, row_id: str, **kwargs) -> None:
        """Adds a DiagnosticReport with all the SQL-required fields filled out"""
        self.add(
            "diagnosticreport",
            {
                "resourceType": "DiagnosticReport",
                "id": row_id,
                **kwargs,
            },
        )

    def add_document_reference(self, row_id: str, start: str = "2020", **kwargs) -> None:
        """Adds a DocumentReference with all the SQL-required fields filled out"""
        context = kwargs.pop("context", {})
        period = context.setdefault("period", {})
        period["start"] = start
        self.add(
            "documentreference",
            {
                "resourceType": "DocumentReference",
                "id": row_id,
                "context": context,
                **kwargs,
            },
        )

    def add_encounter(self, row_id: str, patient: str = "A", start: str = "2020", **kwargs) -> None:
        """Adds an Encounter with all the SQL-required fields filled out"""
        period = kwargs.pop("period", {})
        period["start"] = start
        self.add(
            "encounter",
            {
                "resourceType": "Encounter",
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
                "allergyintolerance",
                "condition",
                "diagnosticreport",
                "documentreference",
                "medicationrequest",
                "observation",
                "procedure",
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

    def add_etl_completion_encounters(self, *, group: str, ids: Iterable[str], time: str) -> None:
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

    def add_medication_request(
        self,
        row_id: str,
        mode: str = "inline",
        codings: list[dict] | None = None,
        **kwargs,
    ) -> None:
        """Adds a MedicationRequest with all the SQL-required fields filled out"""
        if codings is None:
            codings = [
                {
                    "code": "2623378",
                    "system": "http://www.nlm.nih.gov/research/umls/rxnorm",
                }
            ]
        concept = {"coding": codings}

        match mode:
            case "inline":
                kwargs["medicationCodeableConcept"] = concept
            case "contained":
                kwargs["medicationReference"] = {"reference": "#contained"}
                kwargs["contained"] = [
                    {
                        "resourceType": "Medication",
                        "id": "contained",
                        "code": concept,
                    }
                ]
            case "external":
                kwargs["medicationReference"] = {"reference": f"Medication/med-{row_id}"}
                self.add(
                    "medication",
                    {
                        "resourceType": "Medication",
                        "id": f"med-{row_id}",
                        "code": concept,
                    },
                )
            case "custom":
                pass  # caller knows what they want
            case _:
                raise ValueError(f"Bad mode '{mode}'")

        self.add(
            "medicationrequest",
            {"resourceType": "MedicationRequest", "id": row_id, **kwargs},
        )

    def add_observation(self, row_id: str, effective: str = "2020", **kwargs) -> None:
        """Adds a Observation with all the SQL-required fields filled out"""
        self.add(
            "observation",
            {
                "resourceType": "Observation",
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
                "resourceType": "Patient",
                "id": row_id,
                "birthDate": birth_date,
                "gender": gender,
                **kwargs,
            },
        )

    def add_procedure(self, row_id: str, **kwargs) -> None:
        """Adds a Procedure with all the SQL-required fields filled out"""
        self.add(
            "procedure",
            {
                "resourceType": "Procedure",
                "id": row_id,
                **kwargs,
            },
        )

    def build(self, study="core") -> duckdb.DuckDBPyConnection:
        db_file = f"{self.path}/{study}.db"
        db, _ = create_db_backend(
            {
                "db_type": "duckdb",
                "database": db_file,
                "load_ndjson_dir": str(self.path),
                "prepare": False,
            }
        )
        config = base_utils.StudyConfig(
            db=db,
            schema="main",
            # verbose=True,
        )
        builder = cli.StudyRunner(config, data_path=str(self.path))
        builder.clean_and_build_study(
            Path(__file__).parent.parent / "cumulus_library/studies" / study,
            options={},
        )
        return duckdb.connect(db_file)
