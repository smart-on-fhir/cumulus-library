"""Checks export against known dataset.

This is intended to be primarily run as part of CI only, and does not leverage
pytest due to the requirements for AWS secrets/federated tokens/network 
connectivity."""
import os
import sys

from pathlib import Path

from pandas import read_parquet

ref_path = f"{Path(__file__).resolve().parent}/reference"
export_path = f"{Path(__file__).resolve().parent}/data_export/core"

references = set(os.listdir(ref_path))
exports = set(os.listdir(export_path))

if references != exports:
    ref_missing = references - exports
    export_missing = export - references
    sys.exit(
        "Found differences in files present:\n"
        f"Files present in reference not in export: {str(ref_missing)}\n"
        f"Files present in export not in reference: {str(export_missing)}"
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
                        f"Reference: {list(ref_df.columns)}"
                        f"Export: {list(exp_df.columns)}"
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
                    ("Rows differ between reference and export:\n" f"{compared}"),
                ]
            )
if len(diffs) > 0:
    for row in diffs:
        print(f"--- {row[0]} ---")
        print(row[1])
    sys.exit(f"❌ Found {len(diffs)} difference(s). ❌")
print("✅ Reference and export matched ✅")
