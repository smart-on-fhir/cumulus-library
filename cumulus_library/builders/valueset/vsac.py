import json
import pathlib

import pandas
import rich

from cumulus_library import base_utils, study_manifest
from cumulus_library.apis import umls
from cumulus_library.builders.valueset import valueset_utils


def download_oid_data(
    *,
    steward: str,
    oid: str,
    api_key: str | None = None,
    config: base_utils.StudyConfig | None = None,
    manifest: study_manifest.StudyManifest | None = None,
    force_upload: str | None = None,
    path: pathlib.Path | None = None,
) -> bool:
    """Fetches code definitions (assumed to be RXNorm coded) from VSAC
    :keyword steward: the human readable label for the valueset
    :keyword oid: the ID of the valueset in the VSAC database
    :keyword config: A StudyConfig object. If umls_key is none, will check the
        UMLS_API_KEY env var
    :keyword path: A path to write data to (default: [user cache dir]/[study]/valueset_data)
    :returns: True if file created, false otherwise (mostly for testing)
    """
    if config:
        api_key = config.umls_key
        force_upload = config.force_upload
    path = valueset_utils.get_valueset_cache_dir(path, manifest)
    if not (force_upload) and (path / f"{steward}.parquet").exists():
        rich.print(f"{steward} data present at {path}, skipping download.")
        return False
    rich.print(f"Downloading {steward} to {path}")
    api = umls.UmlsApi(api_key=api_key or api_key)
    output = []

    response = api.get_vsac_valuesets(oid=oid)
    for valueset in response:
        contains = valueset.get("expansion").get("contains", [])
        for data in contains:
            output.append([str(data["system"]), int(data["code"]), str(data["display"])])
    output_df = pandas.DataFrame(output, columns=["SAB", "RXCUI", "STR"])
    output_df.to_csv(path / f"{steward}.tsv", index=False, header=True, sep="\t")
    output_df.to_parquet(path / f"{steward}.parquet")
    with open(path / f"{steward}.json", "w") as f:
        f.write(json.dumps(response))
    return True
