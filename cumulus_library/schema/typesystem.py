# pylint: disable=W,C,R
from enum import Enum

from fhirclient.models.fhirdate import FHIRDate


class FHIR(Enum):
    def __init__(self, url: str):
        self.url = url


class Structure(FHIR):
    """
    FHIR Conformance (partial conformance towards USCDI)
    """

    Patient = "http://hl7.org/fhir/us/core/StructureDefinition/us-core-patient"
    Encounter = "http://hl7.org/fhir/us/core/StructureDefinition/us-core-encounter"
    DocumentReference = (
        "http://hl7.org/fhir/us/core/StructureDefinition/us-core-documentreference"
    )
    Condition = "http://hl7.org/fhir/condition-definitions.html"
    ObservationLab = (
        "http://hl7.org/fhir/us/core/StructureDefinition/us-core-observation-lab"
    )
    ObservationValue = (
        "http://hl7.org/fhir/observation-definitions.html#Observation.value_x_"
    )
    VitalSign = "http://hl7.org/fhir/us/vitals/ImplementationGuide/hl7.fhir.us.vitals"


class Vocab(FHIR):
    """
    Terminologies (mapped to UMLS) referenced by Cumulus
    https://terminology.hl7.org/
    """

    ICD9 = "http://hl7.org/fhir/sid/icd-9-cm"
    ICD10 = "http://hl7.org/fhir/sid/icd-10-cm"
    LOINC = "http://loinc.org"
    SNOMED = "http://snomed.info/sct"
    UMLS = "http://www.nlm.nih.gov/research/umls/"
    RXNORM = "http://www.nlm.nih.gov/research/umls/rxnorm"
    CPT = "http://www.ama-assn.org/go/cpt"


class Datatypes(FHIR):
    # basic datatypes
    Boolean = "https://hl7.org/fhir/datatypes.html#boolean"
    Date = "https://www.hl7.org/fhir/datatypes.html#date"
    DateTime = "https://www.hl7.org/fhir/datatypes.html#dateTime"
    Str = "https://hl7.org/fhir/datatypes.html#string"
    Int = "http://hl7.org/fhir/datatypes.html#positiveInt"
    Decimal = "https://www.hl7.org/fhir/datatypes.html#decimal"
    Code = "https://www.hl7.org/fhir/datatypes.html#code"
    Coding = "https://www.hl7.org/fhir/datatypes.html#coding"
    Range = "https://hl7.org/fhir/datatypes.html#Range"
    Period = "https://www.hl7.org/fhir/datatypes.html#Period"
    Duration = "http://hl7.org/fhir/datatypes.html#Duration"
    Count = "http://hl7.org/fhir/search.html#count"


class Period:
    def __init__(self, start=None, end=None):
        """
        :param start: date
        :param end: date
        """
        self.system = Datatypes.Period
        self.start = None
        self.end = None

        if isinstance(start, str):
            self.start = FHIRDate(start)
        else:
            self.start = self.start

        if isinstance(end, str):
            self.end = FHIRDate(end)
        else:
            self.end = end

    def as_json(self):
        return {"start": str(self.start), "end": str(self.end), "system": self.system}


class Range:
    def __init__(self, low=None, high=None):
        self.system = Datatypes.Range
        self.low = low
        self.high = high

    def as_json(self):
        return {"low": str(self.low), "high": str(self.high), "system": self.system}


class Coding(Enum):
    def __init__(self, code=None, display=None, system=None):
        self.system = system
        self.code = code
        self.display = display if display else code

    def as_json(self):
        if self.system:
            return {"code": self.code, "display": self.display, "system": self.system}
        else:
            return {"code": self.code, "display": self.display}
