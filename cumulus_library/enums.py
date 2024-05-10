"""Holds enums used across more than one module"""

from enum import Enum


class ProtectedTableKeywords(Enum):
    """Tables with a pattern like '_{keyword}_' are not manually dropped."""

    ETL = "etl"
    LIB = "lib"
    NLP = "nlp"


class ProtectedTables(Enum):
    """Tables created by cumulus for persistence outside of study rebuilds"""

    STATISTICS = "lib_statistics"
    TRANSACTIONS = "lib_transactions"
