"""Checks export against known dataset.

This file is excluded from the pytest suite because it's finicky to
run locally at BCH:

- You need to be on the BCH VPN
- You need to have a federated token for access from the bch-aws-login script
- You need to do a fresh library build and export to ./data_export

You can elect to strategically do all of these things, but that's outside
the scope of the test suite.

This is primarily meant to be run in CI, where we handle the auth via github's
trusted OIDC token AWS endpoint. See .github/workflows/ci.yaml for more info on
this approach."""

import os
import pathlib
import sys
import zipfile

import rich
from pandas import read_parquet

VOCAB_ICD_ROW_COUNT = 403231


def regress_core():
    ref_path = pathlib.Path(__file__).resolve().parent / "reference"
    export_path = pathlib.Path(__file__).resolve().parent / "data_export/core"
    with zipfile.ZipFile(export_path / "core.zip", "r") as f:
        f.extractall(export_path)
    (export_path / "core.zip").unlink()

    references = set(os.listdir(ref_path))
    exports = set(os.listdir(export_path))

    if references != exports:
        ref_missing = references - exports
        export_missing = exports - references
        sys.exit(
            "❌ Found differences in files present: ❌\n"
            f"Files present in reference not in export: {ref_missing!s}\n"
            f"Files present in export not in reference: {export_missing!s}"
        )
    diffs = []
    for filename in references:
        if filename.endswith(".parquet"):
            ref_df = read_parquet(f"{ref_path}/{filename}")
            exp_df = read_parquet(f"{export_path}/{filename}")
            if list(ref_df.columns) != list(exp_df.columns):
                diffs.append(
                    [
                        filename,
                        (
                            "Columns differ between reference and export:\n"
                            f"Reference: {list(ref_df.columns)}\n"
                            f"Export: {list(exp_df.columns)}"
                        ),
                    ]
                )
                continue
            if ref_df.size != exp_df.size:
                diffs.append(
                    [
                        filename,
                        (
                            "Size (num rows) differ between reference and export:\n"
                            f"Reference: {ref_df.size}\n"
                            f"Export: {exp_df.size}"
                        ),
                    ]
                )
                continue
            ref_df = ref_df.sort_values(list(ref_df.columns), ignore_index=True)
            exp_df = exp_df.sort_values(list(exp_df.columns), ignore_index=True)
            compared = ref_df.compare(exp_df)
            if not compared.empty:
                diffs.append(
                    [
                        filename,
                        f"Rows differ between reference and export:\n {compared}",
                    ]
                )
    if len(diffs) > 0:
        for row in diffs:
            rich.print(f"--- {row[0]} ---")
            rich.print(row[1])
        sys.exit(f"❌ Found {len(diffs)} difference(s) in core study. ❌")
    rich.print("✅ Core study reference and export matched ✅")


if __name__ == "__main__":
    regress_core()
