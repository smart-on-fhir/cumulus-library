#!/usr/bin/env python3
"""Upload utililty for sending data to Cumulus aggregator"""

import argparse
import os
import sys

from pathlib import Path

import requests

from rich.progress import Progress, TaskID


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


def run_uploads(args: dict):
    """Wrapper to prep files & console output"""
    base_path = Path(__file__).resolve().parent
    file_paths = list(base_path.glob("**/*.parquet"))
    num_uploads = len(file_paths)
    if not args["user"] or not args["id"]:
        print("user/id not found")
        raise KeyError
    with Progress() as progress:
        file_upload_progress = progress.add_task("Uploading", total=num_uploads)
        for file_path in file_paths:
            upload_data(progress, file_upload_progress, file_path, args)


def get_parser():
    """Provides an argument parser object for CLI interface"""
    parser = argparse.ArgumentParser(
        description="""Uploads study data to cumulus aggregator.

    Each argument can also be provided via a environment variable, following the
    pattern 'CUMUMLUS_AGGREGATOR_ARGNAME'.
    """
    )
    parser.add_argument("-u", "--user", help="Cumulus user")
    parser.add_argument("-i", "--id", help="Site ID")
    parser.add_argument(
        "--url",
        help="Upload URL",
        default="https://aggregator.smartcumulus.org/upload/",
    )
    parser.add_argument(
        "-p",
        "--preview",
        default=False,
        action="store_true",
        help="Run prefetch and prepare upload, but log output instead of sending.",
    )
    return parser


def main(cli_args=None):
    """Manages CLI arguments and envrionment variables for upload job"""
    parser = get_parser()
    args = vars(parser.parse_args(cli_args))
    if user_env := os.environ.get("CUMULUS_AGGREGATOR_USER"):
        args["user"] = user_env
    if id_env := os.environ.get("CUMULUS_AGGREGATOR_ID"):
        args["id"] = id_env
    if url_env := os.environ.get("CUMULUS_AGGREGATOR_URL"):
        args["url"] = url_env
    try:
        run_uploads(args)
    except (KeyError, requests.RequestException):
        sys.exit(1)


if __name__ == "__main__":
    main()
