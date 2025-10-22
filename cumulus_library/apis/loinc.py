"""Class for communicating with the Loinc API"""

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
        self.cache_dir = base_utils.get_user_cache_dir() / "loinc"
        self.download_dir = self.cache_dir / "downloads"

    def get_all_download_versions(self) -> list:
        """returns all available versions available for download"""
        versions = []
        res = self.session.get(f"{BASE_URL}Loinc/All")
        if res.status_code == 401:
            raise errors.ApiError("Invalid LOINC credentials")
        for record in res.json():
            versions.append(record["version"])
        res.raise_for_status()
        return versions

    def get_download_info(self, version: str | None = None) -> tuple[str, str]:
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
        res.raise_for_status()
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
        :keyword download_url: the url to download the zipfile from. Gets from API if not provided.
        :keyword path: the path on disk to write to (uses user cache dir if not provided)
        :keyword unzip: if True, extracts the archive after download (or a pre-existing download)
        """

        path = path or self.download_dir
        if download_url is None:
            version, download_url = self.get_download_info(version=version)
        path.mkdir(parents=True, exist_ok=True)

        if (path / f"{version}.zip").exists() or (path / version).exists():
            rich.print(f"Loinc version {version} already exists at {path}, skipping download")
        else:
            download_res = self.session.get(download_url, stream=True)
            with open(path / f"{version}.zip", "wb") as f:
                chunks_read = 0
                with base_utils.get_progress_bar() as progress:
                    task = progress.add_task(
                        f"Downloading {version}.zip",
                        total=(int(download_res.headers["Content-Length"]) / 1024),
                    )
                    for chunk in download_res.iter_content(chunk_size=1024):
                        f.write(chunk)
                        chunks_read += 1
                        progress.update(
                            task,
                            description=(f"Downloading {version}.zip: {chunks_read / 1000} MB"),
                            advance=1,
                        )
        if unzip and (path / f"{version}.zip").exists():
            (path / version).mkdir(parents=True, exist_ok=True)
            base_utils.unzip_file(path / f"{version}.zip", path / version)
            (path / f"{version}.zip").unlink()
