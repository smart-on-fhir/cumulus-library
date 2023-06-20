# pylint: disable=W,C,R
from enum import Enum, EnumMeta
from cumulus_library.schema.typesystem import Datatypes, Coding, Vocab
from cumulus_library.schema.valueset import Gender, Race, Ethnicity
from cumulus_library.schema.valueset import DurationUnits
from cumulus_library.schema.valueset import EncounterCode
from cumulus_library.schema.valueset import ObservationInterpretationDetection


class ColumnEnum(Enum):
    # Counts
    cnt = Datatypes.Int, "Count"
    cnt_subject = Datatypes.Int, "Count Patients"
    cnt_encounter = Datatypes.Int, "Count Encounters"
    cnt_document = Datatypes.Int, "Count Documents"
    cnt_observation = Datatypes.Int, "Count Observations"
    cnt_lab = Datatypes.Int, "Count Lab Tests"

    # DataType Generics
    code_display = Datatypes.Str, "Code Display"
    period = Datatypes.Period, "Date Period"

    # Patient
    gender = Datatypes.Coding, "Biological sex at birth", Gender
    race = Datatypes.Coding, "Patient reported race", Race
    race_code = Datatypes.Coding, "Patient reported race", Race
    race_display = Datatypes.Str, "Patient reported race", Race
    ethnicity = Datatypes.Coding, "Patient reported ethnicity", Ethnicity
    age = Datatypes.Int, "Age in years calculated since DOB"
    postalcode3 = Datatypes.Int, "Patient 3 digit zip"

    # Encounter
    age_at_visit = Datatypes.Int, "Patient Age at Encounter"
    age_group = Datatypes.Str, "Patient Age Group at Encounter"
    enc_class = Datatypes.Coding, "Encounter Code (Healthcare Setting)", EncounterCode
    enc_class_code = (
        Datatypes.Coding,
        "Encounter Code (Healthcare Setting)",
        EncounterCode,
    )
    start_date = Datatypes.DateTime, "Day patient encounter started", DurationUnits.days
    start_week = Datatypes.Date, "Week patient encounter started", DurationUnits.weeks
    start_month = (
        Datatypes.Date,
        "Month patient encounter started",
        DurationUnits.months,
    )
    start_year = Datatypes.Date, "Year patient encounter started", DurationUnits.years
    end_date = Datatypes.DateTime, "Day patient encounter ended", DurationUnits.days
    enc_los_days = Datatypes.Int, "LOS Length Of Stay (days)", DurationUnits.days
    enc_los_weeks = Datatypes.Int, "LOS Length Of Stay (weeks)", DurationUnits.weeks

    # Condition
    cond_code = Datatypes.Coding, "Condition code"
    cond_code_display = Datatypes.Str, "Condition code"
    cond_icd10 = Vocab.ICD10, "Condition code ICD10"
    cond_snomed = Vocab.SNOMED, "Condition code SNOMED"
    cond_date = Datatypes.DateTime, "Day condition recorded", DurationUnits.days
    cond_week = Datatypes.Date, "Week condition recorded", DurationUnits.weeks
    cond_month = Datatypes.Date, "Month condition recorded", DurationUnits.months
    cond_year = Datatypes.Date, "Year condition recorded", DurationUnits.years

    # DocumentReference
    author_date = Datatypes.DateTime, "Day document was authored", DurationUnits.days
    author_week = Datatypes.Date, "Week document was authored", DurationUnits.weeks
    author_month = Datatypes.Date, "Month document was authored", DurationUnits.months
    author_year = Datatypes.Date, "Year document was authored", DurationUnits.years
    doc_type = Vocab.LOINC, "Type of Document"
    doc_type_code = Datatypes.Coding, "Type of Document (code)"
    doc_type_display = Datatypes.Str, "Type of Document (display)"
    doc_class = Vocab.LOINC, "Class of DocumentNote"
    ed_note = Datatypes.Boolean, "ED Note was documented for encounter (true/false)"

    # Observation Lab
    lab_code = Datatypes.Coding, "Laboratory Code"
    lab_code_display = Datatypes.Str, "Laboratory Code Display"
    lab_result = (
        Datatypes.Coding,
        "Laboratory result interpretation",
        ObservationInterpretationDetection,
    )
    lab_result_display = Datatypes.Str, "Laboratory result"
    lab_date = Datatypes.DateTime, "Day of lab result", DurationUnits.days
    lab_week = Datatypes.Date, "Week of lab result", DurationUnits.weeks
    lab_month = Datatypes.Date, "Month of lab result", DurationUnits.months
    lab_year = Datatypes.Date, "Year of lab result", DurationUnits.years
    analyte = Datatypes.Str, "Analyte/component"

    # Observation Lab PCR
    pcr_code = Datatypes.Coding, "PCR test code"
    pcr_result = (
        Datatypes.Coding,
        "PCR result interpretation",
        ObservationInterpretationDetection,
    )
    pcr_result_display = (
        Datatypes.Str,
        "PCR result interpretation",
        ObservationInterpretationDetection,
    )
    pcr_date = Datatypes.Date, "Day of PCR result", DurationUnits.days
    pcr_week = Datatypes.Date, "Week of PCR result", DurationUnits.weeks
    pcr_month = Datatypes.Date, "Month of PCR result", DurationUnits.months
    pcr_year = Datatypes.Date, "Year of PCR result", DurationUnits.years
