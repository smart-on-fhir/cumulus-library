import dataclasses


@dataclasses.dataclass(kw_only=True)
class ValuesetConfig:
    """Provides expected values for creating valuesets"""

    rules_file: str = None
    keyword_file: str = None
    table_prefix: str = None
    umls_stewards: dict[str, str] = None
    vsac_stewards: dict[str, str] = None
