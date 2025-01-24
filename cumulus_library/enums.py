"""Holds enums used across more than one module"""

import enum


class LogStatuses(enum.Enum):
    DEBUG = "debug"
    ERROR = "error"
    FINISHED = "finished"
    INFO = "info"
    RESUMED = "resumed"
    STARTED = "started"
    WARN = "warn"


class ProtectedTables(enum.Enum):
    """Tables created by cumulus for persistence outside of study rebuilds"""

    STATISTICS = "lib_statistics"
    TRANSACTIONS = "lib_transactions"


class ProtectedTableKeywords(enum.Enum):
    """Tables with a pattern like '_{keyword}_' are not manually dropped."""

    ETL = "etl"
    LIB = "lib"
    NLP = "nlp"


class ResourceTypes(enum.StrEnum):
    ALLERGEYINTOLERANCE = "allergyintolerance"
    CONDITION = "condition"
    DIAGNOSTICREPORT = "diagnosticreport"
    DOCUMENTREFERENCE = "documentreference"
    ENCOUNTER = "encounter"
    MEDICATION = "medication"
    MEDICATIONREFERENCE = "medicationreference"
    OBSERVATION = "observation"
    PATIENT = "patient"


class StatisticsTypes(enum.Enum):
    """A subset of workflows that create statistics sampling artifacts"""

    PSM = "psm"
