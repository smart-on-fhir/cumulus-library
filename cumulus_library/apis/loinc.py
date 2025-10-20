"""Class for communicating with the umls API"""

import os
import pathlib

import requests
import rich

from cumulus_library import base_utils, errors

BASE_URL = "https://loinc.regenstrief.org/api/v1/"


class LoincApi:
    def __init__(self, *, user: str | None = None, password: str | None = None):
        """Creates a requests session for future calls

        :keyword user: the username of the loinc user
        :keyword password: the password of the loinc user

        You can request a Loinc account at https://loinc.org/join/.
        """
        if user is None:
            user = os.environ.get("LOINC_USER")
            if user is None:
                raise errors.ApiError("No LOINC user provided")
        if password is None:
            password = os.environ.get("LOINC_PASSWORD")
            if password is None:
                raise errors.ApiError("No LOINC password provided")

        self.session = requests.Session()
        self.session.auth = requests.auth.HTTPBasicAuth(user, password)

    def get_all_download_versions(self) -> list:
        """returns all available versions available for download"""
        versions = []
        res = self.session.get(f"{BASE_URL}Loinc/All")
        if res.status_code == 401:
            raise errors.ApiError("Invalid LOINC credentials")
        for record in res.json():
            versions.append(record["version"])
        return versions

    def get_download_info(self, version: str | None = None) -> (str, str):
        """gets the download info of the latest release, or the specified version
        :param version: a specific verson you'd like to download
        :returns: a tuple of the version (useful if not provided) and the download url
        """
        url = f"{BASE_URL}Loinc"
        if version is not None:
            url = f"{url}?version={version}"
        res = self.session.get(url)
        if res.status_code == 401:
            raise errors.ApiError("Invalid LOINC credentials")
        elif res.status_code == 404:
            raise errors.ApiError(f"Loinc version {version} not found")
        res = res.json()
        return res["version"], res["downloadUrl"]

    def download_loinc_dataset(
        self,
        *,
        version: str | None = None,
        download_url: str | None = None,
        path: pathlib.Path | None = None,
        unzip: bool = True,
    ):
        """Downloads a dataset from the LOINC API
        :keyword version: the data version to download
        :keyword path: the path on disk to write to
        :keyword unzip: if True, extracts the archive after download
        """

        path = path or pathlib.Path.cwd()
        if download_url is None:
            version, download_url = self.get_download_info(version=version)
        path.mkdir(parents=True, exist_ok=True)

        if any(str(x).endswith(version) for x in path.glob("*.*")):
            console = rich.get_console()
            console.print(f"Loinc version {version} already exists at {path}, skipping download")
            return
        download_res = self.session.get(download_url, stream=True)
        with open(path / f"{version}.zip", "wb") as f:
            chunks_read = 0
            with base_utils.get_progress_bar() as progress:
                task = progress.add_task(f"Downloading {version}.zip", total=None)
                for chunk in download_res.iter_content(chunk_size=1024):
                    f.write(chunk)
                    chunks_read += 1
                    progress.update(
                        task,
                        description=(f"Downloading {version}.zip: {chunks_read / 1000} MB"),
                    )
        (path / version).mkdir(parents=True, exist_ok=True)
        if unzip:
            base_utils.unzip_file(path / f"{version}.zip", path / version)
            (path / f"{version}.zip").unlink()
