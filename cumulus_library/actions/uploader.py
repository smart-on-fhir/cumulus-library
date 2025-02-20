"""Handles pushing data to the aggregator"""

import sys
from pathlib import Path

import requests
import rich
from pandas import read_parquet

from cumulus_library import base_utils


def upload_data(
    progress_bar: rich.progress.Progress,
    file_upload_progress: rich.progress.TaskID,
    file_path: Path,
    version: str,
    args: dict,
):
    """Fetches presigned url and uploads file to aggregator"""
    study = file_path.parts[-2]
    file_name = file_path.parts[-1]
    c = rich.get_console()
    progress_bar.update(file_upload_progress, description=f"Uploading {study}/{file_name}")
    data_package = file_name.split(".")[0]
    prefetch_res = requests.post(
        args["url"],
        json={
            "study": study,
            "data_package": data_package,
            "data_package_version": int(float(version)),
            "filename": f"{args['user']}_{file_name}",
        },
        auth=(args["user"], args["id"]),
        timeout=60,
    )
    if args["preview"]:
        c.print("prefetch request")
        c.print("headers", prefetch_res.request.headers)
        c.print("body", prefetch_res.request.body, "\n")
        c.print("response")
        c.print(prefetch_res.json(), "\n")
    if prefetch_res.status_code != 200:
        c.print("Invalid user/site id")
        prefetch_res.raise_for_status()
    res_body = prefetch_res.json()
    with open(file_path, "rb") as data_file:
        files = {"file": (file_name, data_file)}
        upload_req = requests.Request(
            "POST", res_body["url"], data=res_body["fields"], files=files
        ).prepare()
        if not args["preview"]:
            s = requests.Session()
            upload_res = s.send(upload_req, timeout=60)
            if upload_res.status_code != 204:
                c.print(f"Error uploading {study}/{file_name}")
                upload_res.raise_for_status()
        else:
            c.print("upload_req")
            c.print("headers", upload_req.headers)
            c.print("body", upload_req.body, "\n")
    progress_bar.update(file_upload_progress, advance=1)


def upload_files(args: dict):
    """Wrapper to prep files & console output"""
    if args["data_path"] is None:
        sys.exit("No data directory provided - please provide a path to your study export folder.")
    file_paths = list(args["data_path"].glob("**/*.parquet"))
    if args["target"]:
        filtered_paths = []
        for path in file_paths:
            if any(
                path.parent.name == study and path.name.startswith(f"{study}__")
                for study in args["target"]
            ):
                filtered_paths.append(path)
        file_paths = filtered_paths

    if not args["user"] or not args["id"]:
        sys.exit("user/id not provided, please pass --user and --id")
    try:
        meta_version = next(
            filter(lambda x: str(x).endswith("__meta_version.meta.parquet"), file_paths)
        )
        version = str(read_parquet(meta_version)["data_package_version"][0])
        file_paths.remove(meta_version)
    except StopIteration:
        version = "0"
    num_uploads = len(file_paths)
    with base_utils.get_progress_bar() as progress_bar:
        file_upload_progress = progress_bar.add_task("Uploading", total=num_uploads)
        for file_path in file_paths:
            upload_data(progress_bar, file_upload_progress, file_path, version, args)
