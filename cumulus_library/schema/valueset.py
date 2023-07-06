# pylint: disable=W,C,R
from enum import Enum
from cumulus_library.schema.typesystem import Coding, Vocab

################################################################################
# FHIR ValueSets
################################################################################


class ValueSet(Enum):
    Gender = "http://hl7.org/fhir/ValueSet/administrative-gender"
    Race = "http://hl7.org/fhir/us/core/StructureDefinition/us-core-race"
    Ethnicity = "http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity"
    DurationUnits = "http://hl7.org/fhir/valueset-duration-units.html"
    Units = "http://hl7.org/fhir/ValueSet/ucum-units"
    PatientClass = "http://terminology.hl7.org/CodeSystem/v2-0004"
    EncounterCode = "http://terminology.hl7.org/ValueSet/v3-ActEncounterCode"
    EncounterPriority = "http://terminology.hl7.org/CodeSystem/v3-ActPriority"
    EncounterStatus = "http://hl7.org/fhir/ValueSet/encounter-status"
    EncounterType = "http://hl7.org/fhir/ValueSet/encounter-type"
    EncounterReason = "http://hl7.org/fhir/ValueSet/encounter-reason"
    EncounterLocationStatus = "http://hl7.org/fhir/ValueSet/encounter-location-status"
    ConditionCode = "http://hl7.org/fhir/ValueSet/condition-code"
    ConditionCategory = "http://hl7.org/fhir/ValueSet/condition-category"
    DocumentType = "http://hl7.org/fhir/ValueSet/c80-doc-typecodes"
    ObservationCode = "http://hl7.org/fhir/ValueSet/observation-codes"
    ObservationCategory = "http://hl7.org/fhir/ValueSet/observation-category"
    ObservationInterpretation = (
        "http://hl7.org/fhir/ValueSet/observation-interpretation"
    )

    def __init__(self, url: str):
        self.url = url


class DurationUnits(Coding):
    milliseconds = ("ms", "milliseconds")
    seconds = ("s", "seconds")
    minutes = ("min", "minutes")
    hours = ("h", "hours")
    days = ("d", "days")
    weeks = ("wk", "weeks")
    months = ("mo", "months")
    years = ("a", "years")

    def __init__(self, code, display):
        super().__init__(code, display, ValueSet.DurationUnits.url)


class Gender(Coding):
    """
    Biological Sex of patient, not "gender identity"
    http://hl7.org/fhir/valueset-administrative-gender.html
    """

    male = ("male", "Male")
    female = ("female", "Female")
    trans_male = ("other", "Other")
    unknown = ("unknown", "Unknown")

    def __init__(self, code=None, display=None):
        super().__init__(code, display, ValueSet.Gender.url)


class Race(Coding):
    """
    Race coding has 5 "root" levels, called the R5 shown below.
    http://hl7.org/fhir/r4/v3/Race/cs.html
    """

    asian = ("2028-9", "Asian")
    black = ("2054-5", "Black or African American")
    white = ("2106-3", "White")
    native = ("1002-5", "American Indian or Alaska Native")
    islander = ("2076-8", "Native Hawaiian or Other Pacific Islander")

    def __init__(self, code, display):
        super().__init__(code, display, ValueSet.Race.url)


class Ethnicity(Coding):
    """
    RWD usually has only this binary YES/NO hispanic or latino feature populated.
    For a complete list of codes, see Ethnicity.system.
    """

    hispanic_or_latino = ("2135-2", "Hispanic or Latino	Hispanic or Latino")
    not_hispanic_or_latino = ("2186-5", "Not Hispanic or Latino")

    def __init__(self, code, display):
        super().__init__(code, display, ValueSet.Ethnicity.url)


################################################################################
# Encounter
################################################################################


class EncounterCode(Coding):
    AMB = ("AMB", "ambulatory")
    EMER = ("EMER", "emergency")
    FLD = ("FLD", "field")
    HH = ("HH", "home health")
    IMP = ("IMP", "inpatient encounter")
    ACUTE = ("ACUTE", "inpatient acute")
    NONAC = ("NONAC", "inpatient non-acute")
    OBSENC = ("OBSENC", "observation encounter")
    PRENC = ("PRENC", "pre-admission")
    SS = ("SS", "short stay")
    VR = ("VR", "virtual")

    def __init__(self, code, display=None):
        super().__init__(code, display, ValueSet.EncounterCode.value)


################################################################################
# Observation
################################################################################


class ObservationInterpretationDetection(Coding):
    positive = ("POS", "Positive")
    negative = ("NEG", "Negative")
    indeterminate = ("IND", "Indeterminate")
    equivocal = ("E", "Equivocal")
    detected = ("DET", "Detected")
    not_detected = ("ND", "Not detected")

    def __init__(self, code, display):
        """
        Cumulus Library Note: PCR testing should use "POS", "NEG", and "IND" codes.
        http://hl7.org/fhir/R4/v3/ObservationInterpretation/cs.html#v3-ObservationInterpretation-ObservationInterpretationDetection

        Interpretations of the presence or absence of a component / analyte or organism in a test or of a sign in a clinical observation.
        In keeping with laboratory data processing practice, these concepts provide a categorical interpretation of the "meaning" of the quantitative value for the same observation.

        POS =
        A presence finding of the specified component / analyte, organism or clinical sign based on the established threshold of the performed test or procedure.

        NEG =
        An absence finding of the specified component / analyte, organism or clinical sign based on the established threshold of the performed test or procedure.

        IND =
        The specified component / analyte, organism or clinical sign could neither be declared positive / negative nor detected / not detected by the performed test or procedure.
        Usage Note: For example, if the specimen was degraded, poorly processed, or was missing the required anatomic structures, then "indeterminate" (i.e. "cannot be determined") is the appropriate response, not "equivocal".
        """
        super().__init__(code, display, ValueSet.ObservationInterpretation.url)
