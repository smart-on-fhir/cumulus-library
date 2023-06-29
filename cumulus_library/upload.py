""" Handles pushing data to the aggregator"""
import sys

from pathlib import Path

import requests
from rich.progress import Progress, TaskID

from cumulus_library.helper import get_progress_bar


def upload_data(
    progress: Progress, file_upload_progress: TaskID, file_path: Path, args: dict
):
    """Fetches presigned url and uploads file to aggregator"""
    study = file_path.parts[-2]
    file_name = file_path.parts[-1]
    progress.update(file_upload_progress, description=f"Uploading {study}/{file_name}")
    subscription = file_name.split(".")[0]
    prefetch_res = requests.post(
        args["url"],
        json={
            "study": study,
            "data_package": subscription,
            "filename": f"{args['user']}_{file_name}",
        },
        auth=(args["user"], args["id"]),
        timeout=60,
    )
    if args["preview"]:
        print("prefetch request")
        print("headers", prefetch_res.request.headers)
        print("body", prefetch_res.request.body, "\n")
        print("response")
        print(prefetch_res.json(), "\n")

    if prefetch_res.status_code != 200:
        print("Invalid user/site id")
        raise requests.RequestException(response=prefetch_res)
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
                print(f"Error uploading {study}/{file_name}")
                raise requests.RequestException(response=upload_res)
        else:
            print("upload_req")
            print("headers", upload_req.headers)
            print("body", upload_req.body, "\n")
    progress.update(file_upload_progress, advance=1)


def upload_files(args: dict):
    """Wrapper to prep files & console output"""
    if args["data_path"] is None:
        sys.exit(
            "No data directory provided - please provide a path to your"
            "study export folder."
        )
    file_paths = list(args["data_path"].glob("**/*.parquet"))
    num_uploads = len(file_paths)
    if not args["user"] or not args["id"]:
        sys.exit("user/id not provided, please pass --user and --id")
    with get_progress_bar() as progress:
        file_upload_progress = progress.add_task("Uploading", total=num_uploads)
        for file_path in file_paths:
            upload_data(progress, file_upload_progress, file_path, args)
