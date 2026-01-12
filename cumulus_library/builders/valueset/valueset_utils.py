import dataclasses
import pathlib

from cumulus_library import base_utils, study_manifest


@dataclasses.dataclass(kw_only=True)
class ValuesetConfig:
    """Provides expected values for creating valuesets"""

    rules_file: str = None
    keyword_file: str = None
    table_prefix: str = None
    umls_stewards: dict[str, str] = None
    vsac_stewards: dict[str, str] = None


def get_valueset_cache_dir(
    path: pathlib.Path | None, manifest: study_manifest.StudyManifest | None
):
    if not path:
        if manifest:
            subpath = f"{manifest.get_study_prefix()}/valueset_data"
        else:
            subpath = "vsac_generic_cache/valueset_data"  # pragma: no cover
        path = base_utils.get_user_cache_dir() / subpath
    path.mkdir(exist_ok=True, parents=True)
    return path
