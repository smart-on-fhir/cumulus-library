"""Handles pushing data to the aggregator"""

import sys
import zipfile
from pathlib import Path

import requests
import rich
from pandas import read_parquet

from cumulus_library import base_utils, const


def upload_data(
    progress_bar: rich.progress.Progress,
    file_upload_progress: rich.progress.TaskID,
    file_path: Path,
    version: str,
    args: dict,
) -> str:
    """Fetches presigned url and uploads file to aggregator"""
    study = file_path.parts[-2]
    file_name = file_path.parts[-1]
    c = rich.get_console()
    progress_bar.update(file_upload_progress, description=f"Uploading {study}/{file_name}")
    url = args["url"]
    if args["network"]:
        # coercion to handle optional presence of trailing slash in the url
        url = url.rstrip("/") + "/" + args["network"]
    prefetch_res = requests.post(
        url,
        json={
            "study": study,
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
    if prefetch_res.status_code == 412:
        sys.exit(str(prefetch_res.json()))
    elif prefetch_res.status_code != 200:
        c.print("Invalid user/site id")
        prefetch_res.raise_for_status()
    transaction_id = prefetch_res.headers.get("transaction-id")
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
    return transaction_id


def upload_files(args: dict):
    """Wrapper to prep files & console output"""
    if args["data_path"] is None:
        sys.exit("No data directory provided - please provide a path to your study export folder.")
    file_paths = list(args["data_path"].glob("**/*.zip"))
    filtered_paths = []
    if not args["user"] or not args["id"]:
        sys.exit("user/id not provided, please pass --user and --id")
    for target in args["target"]:
        for path in file_paths:
            if path.parent.name == target and path.name == f"{target}.zip":
                filtered_paths.append(path)
        if len(filtered_paths) == 0:
            sys.exit("No files found for upload. Is your data path/target specified correctly?")
        archive_path = filtered_paths[0]
        upload_archive = zipfile.ZipFile(archive_path)
        archive_contents = upload_archive.namelist()
        invalid_contents = []
        for file in archive_contents:
            if not any(x in file for x in const.ALLOWED_UPLOADS):
                invalid_contents.append(file)
        if len(invalid_contents) > 0:
            sys.exit(
                f"{archive_path} contains files that are not allowed:"
                f"  {invalid_contents}"
                "This likely means you tried to upload an archive containing line level data, "
                "but may also be a bug related to your study export names."
            )
        if target != "discovery":
            if not any(f"{target}__meta_date.meta.parquet" == x for x in archive_contents):
                sys.exit(
                    f"Study '{target}' does not contain a {target}__meta_date table.\n"
                    "See the documentation for more information about this required table.\n"
                    "https://docs.smarthealthit.org/cumulus/library/creating-studies.html#metadata-tables"
                )
        try:
            meta_version = next(
                filter(lambda x: str(x).endswith("__meta_version.meta.parquet"), archive_contents)
            )
            version = str(
                read_parquet(upload_archive.open(meta_version))["data_package_version"][0]
            )
        except StopIteration:
            version = "0"
        # TODO: I looked into monitoring upload progress instead of completed files and it is
        # non-trivial - potential point for improvement later
        with base_utils.get_progress_bar() as progress_bar:
            file_upload_progress = progress_bar.add_task(f"Uploading {target}...", total=1)
            upload_data(progress_bar, file_upload_progress, filtered_paths[0], version, args)
