import argparse
import json
import pathlib

import pandas
import rich

from cumulus_library import base_utils
from cumulus_library.apis import umls


def download_oid_data(
    *,
    steward: str,
    oid: str,
    api_key: str | None = None,
    config: base_utils.StudyConfig | None = None,
    force_upload: str | None = None,
    path: pathlib.Path | None = None,
) -> bool:
    """Fetches code definitions (assumed to be RXNorm coded) from VSAC
    :keyword steward: the human readable label for the valueset
    :keyword oid: the ID of the valueset in the VSAC database
    :keyword config: A StudyConfig object. If umls_key is none, will check the
        UMLS_API_KEY env var
    :keyword path: A path to write data to (default: ../data)
    :returns: True if file created, false otherwise (mostly for testing)
    """
    if config:
        api_key = config.umls_key
        force_upload = config.force_upload
    if not path:
        path = pathlib.Path(__file__).parent.parent / "data"  # pragma: no cover
    path.mkdir(exist_ok=True, parents=True)
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
            output.append([data["code"], data["display"]])
    output_df = pandas.DataFrame(output, columns=["RXCUI", "STR"])
    output_df.to_csv(path / f"{steward}.tsv", index=False, header=False, sep="\t")
    output_df.to_parquet(path / f"{steward}.parquet")
    with open(path / f"{steward}.json", "w") as f:
        f.write(json.dumps(response))
    return True


def main(cli_args=None):
    """Deprecated CLI interface"""
    parser = argparse.ArgumentParser()
    (
        parser.add_argument(
            "--steward", help="Human-friendly name for steward (used for filenames)", default=None
        ),
    )
    parser.add_argument("--oid", help="oid to look up codes for", default=None)
    parser.add_argument("--api-key", help="UMLS api key", default=None)
    parser.add_argument(
        "--force-upload",
        help="Force redownloading of data even if it already exists",
        action="store_true",
    )
    parser.add_argument("--path", help="optional path to write data to", default=None)
    args = parser.parse_args(cli_args)
    return download_oid_data(
        steward=args.steward,
        oid=args.oid,
        api_key=args.api_key,
        force_upload=args.force_upload,
        path=pathlib.Path(args.path),
    )


if __name__ == "__main__":
    main()  # pragma: no cover
