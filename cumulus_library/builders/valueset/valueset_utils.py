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
    # this is not actively used much right now, but if you want things to go
    # to the user cache dir, just don't provide a path
    if not path:  # pragma: no cover
        if manifest:
            subpath = f"{manifest.get_study_prefix()}/valueset_data"
        else:
            subpath = "valueset_data"  # pragma: no cover
        path = base_utils.get_user_cache_dir() / subpath
    # since a very common use case is 'download a file from vsac, make a flat file,
    # and then have the file uploader push it to S3', this is the default write location
    else:
        path = path / "valueset_data"
    path.mkdir(exist_ok=True, parents=True)
    return path
