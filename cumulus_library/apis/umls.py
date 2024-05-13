"""Class for communicating with the umls API"""

import os
import pathlib

import requests

from cumulus_library import base_utils, errors

VALID_UMLS_DOWNLOADS = [
    "rxnorm-full-monthly-release",
    "rxnorm-weekly-updates",
    "rxnorm-prescribable-content-monthly-release",
    "rxnorm-prescribable-content-weekly-updates",
    "rxnav-in-a-box",
    "snomed-ct-us-edition",
    "snomed-ct-us-edition-transitive-closure-resources",
    "snomed-ct-international-edition",
    "snomed-ct-core-problem-list-subset",
    "snomed-ct-to-icd-10-cm-mapping-resources",
    "snomed-ct-spanish-edition",
    "umls-metathesaurus-full-subset",
    "umls-metathesaurus-mrconso-file",
    "umls-full-release",
]


class UmlsApi:
    def __init__(self, api_key: str | None = None, validator_key: str | None = None):
        """Creates a requests session for future calls, and validates the API key

        :keyword api_key: A UMLS API key (will check for an env var named UMLS_API_KEY
            if None)
        :keyword validator_key: A UMLS API key for the calling application. Will be
            set to the value of api_key if None (which is the current expected
            behavior, since we don't want to be distributing UMLS keys)
        """

        if api_key is None:
            api_key = os.environ.get("UMLS_API_KEY")
            if api_key is None:
                raise errors.ApiError("No UMLS API key provided")
        self.api_key = api_key
        self.validator_key = validator_key or api_key

        auth_payload = {"validatorApiKey": self.validator_key, "apiKey": self.api_key}
        self.session = requests.Session()
        response = self.session.get(
            "https://utslogin.nlm.nih.gov/validateUser", params=auth_payload
        )
        if response.status_code == 401:
            raise errors.ApiError("Invalid UMLS API validator key")
        if response.text != "true":
            raise errors.ApiError("Invalid UMLS API key")
        self.session.auth = requests.auth.HTTPBasicAuth("apikey", api_key)

    def get_vsac_valuesets(
        self, url: str | None = None, oid: str | None = None
    ) -> list[dict]:
        """Gets a valueset, and any nested valuesets, from the VSAC API

        :keyword url: an URL to target for a valueset (typically expected)
        :keyword oid: A valuset OID
        :returns: A list, containing the valueset and any referenced
            valuesets.

        Documentation on this API is available at
        https://www.nlm.nih.gov/vsac/support/usingvsac/vsacfhirapi.html


        TODO: do we need to support the FHIR operators?
        TODO: do we need to support the v2 API?
        https://www.nlm.nih.gov/vsac/support/usingvsac/vsacsvsapiv2.html
        """
        if url is None:
            url = "https://cts.nlm.nih.gov/fhir/res/ValueSet"
        if oid:
            url = f"{url}/{oid}"

        # If we're inspecting url references in a VSAC response, they come back
        # specifying a url that does not align with the actual implemented rest
        # APIs, so we do some massaging
        if "http:" in url:
            url = url.replace("http:", "https:")
        if "/res/" not in url:
            url = url.replace("/fhir/", "/fhir/res/")
        response = self.session.get(url)
        if response.status_code == 404:
            raise errors.ApiError(f"Url not found: {url}")
        all_responses = [response.json()]
        included_records = all_responses[0].get("compose", {}).get("include", [])
        for record in included_records:
            if "valueSet" in record:
                valueset = self.get_vsac_valuesets(url=record["valueSet"][0])
                all_responses.append(valueset[0])
        return all_responses

    def get_latest_umls_file_release(self, target: str):
        if target not in VALID_UMLS_DOWNLOADS:
            raise errors.ApiError(
                f"'{target}' is not a valid umls download type.\n\n"
                f"Expected values: {','.join(VALID_UMLS_DOWNLOADS)}"
            )
        release_payload = {"releaseType": target, "current": "true"}
        return self.session.get(
            "https://uts-ws.nlm.nih.gov/releases", params=release_payload
        ).json()[0]

    def download_umls_files(
        self,
        target: str = "umls-metathesaurus-full-subset",
        path: pathlib.Path | None = None,
        unzip: bool = True,
    ):
        """Downloads an available file from the UMLS Download API and unzips it
        target: the UMLS resource to download (default: the MRCONSO.RRF file)
        path: the path on disk to write to

        See https://documentation.uts.nlm.nih.gov/automating-downloads.html for more
        info about the available downloads
        """
        if target not in VALID_UMLS_DOWNLOADS:
            raise errors.ApiError(
                f"'{target}' is not a valid umls download type.\n\n"
                f"Expected values: {','.join(VALID_UMLS_DOWNLOADS)}"
            )
        if path is None:
            path = pathlib.Path.cwd()
        file_meta = self.get_latest_umls_file_release(target)

        # This particular endpoint requires the API key as a param rather than a
        # basic auth header ¯\_(ツ)_/¯.
        download_payload = {
            "url": file_meta["downloadUrl"],
            "apiKey": self.api_key,
        }
        download_res = requests.get(
            "https://uts-ws.nlm.nih.gov/download", params=download_payload, stream=True
        )

        with open(path / file_meta["fileName"], "wb") as f:
            chunks_read = 0
            with base_utils.get_progress_bar() as progress:
                task = progress.add_task(
                    f"Downloading {file_meta['fileName']}", total=None
                )
                for chunk in download_res.iter_content(chunk_size=1024):
                    f.write(chunk)
                    chunks_read += 1
                    progress.update(
                        task,
                        description=(
                            f"Downloading {file_meta['fileName']}: "
                            f"{chunks_read/1000} MB"
                        ),
                    )
        if unzip:
            base_utils.unzip_file(path / file_meta["fileName"], path)
            (path / file_meta["fileName"]).unlink()
