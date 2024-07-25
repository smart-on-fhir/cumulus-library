import os
import pathlib
from contextlib import nullcontext as does_not_raise
from unittest import mock

import pytest
import responses

from cumulus_library import errors
from cumulus_library.apis import umls

AUTH_URL = "https://utslogin.nlm.nih.gov/validateUser"
VALUESET_URL = "https://cts.nlm.nih.gov/fhir/res/ValueSet"
RELEASE_URL = "https://uts-ws.nlm.nih.gov/releases"
DOWNLOAD_URL = "https://uts-ws.nlm.nih.gov/download"
DEFINITION_SINGLE_VALUESET_OID = "2.16.840.1.113883.3.3616.200.110.102.3186"
DEFINITION_INCLUDE_VALUESET_OID = "2.16.840.1.113883.3.3616.200.110.102.7001"
EXPANSION_VALUESET_OID = "2.16.840.1.113762.1.4.1106.68"


@mock.patch.dict(
    os.environ,
    clear=True,
)
@pytest.mark.parametrize(
    "api_key,validator_key,res_text,res_status,raises",
    [
        (None, None, None, 401, pytest.raises(errors.ApiError)),
        ("valid_key", None, "true", 200, does_not_raise()),
        ("valid_key", "other_valid_key", "true", 200, does_not_raise()),
        ("invalid_key", None, None, 401, pytest.raises(errors.ApiError)),
        ("invalid_key", "other_valid_key", None, 401, pytest.raises(errors.ApiError)),
        ("valid_key", "invalid_key", "false", 200, pytest.raises(errors.ApiError)),
    ],
)
@responses.activate
def test_auth(api_key, validator_key, res_text, res_status, raises):
    with raises:
        responses.add(responses.GET, AUTH_URL, body=res_text, status=res_status)
        api = umls.UmlsApi(api_key, validator_key)
        assert api.api_key == api_key
        if validator_key is not None:
            assert api.validator_key == validator_key
        else:
            assert api.validator_key == api_key


@mock.patch.dict(
    os.environ,
    clear=True,
)
@responses.activate
def test_auth_reads_env_var():
    responses.add(responses.GET, AUTH_URL, body="true", status=200)
    os.environ["UMLS_API_KEY"] = "valid_key"
    api = umls.UmlsApi()
    assert api.api_key == "valid_key"


def get_valueset_data(file_name):
    file_path = pathlib.Path(f"./tests/test_data/apis/umls/{file_name}")
    with open(file_path) as f:
        return f.read()


@mock.patch.dict(
    os.environ,
    clear=True,
)
@pytest.mark.parametrize(
    "action,url,oid,expected_oids,raises",
    [
        (
            "definition",
            VALUESET_URL,
            DEFINITION_SINGLE_VALUESET_OID,
            [DEFINITION_SINGLE_VALUESET_OID],
            does_not_raise(),
        ),
        (
            "definition",
            None,
            DEFINITION_SINGLE_VALUESET_OID,
            [DEFINITION_SINGLE_VALUESET_OID],
            does_not_raise(),
        ),
        (
            "definition",
            VALUESET_URL + "/" + DEFINITION_SINGLE_VALUESET_OID,
            None,
            [DEFINITION_SINGLE_VALUESET_OID],
            does_not_raise(),
        ),
        (
            "definition",
            VALUESET_URL,
            DEFINITION_INCLUDE_VALUESET_OID,
            [DEFINITION_INCLUDE_VALUESET_OID, DEFINITION_SINGLE_VALUESET_OID],
            does_not_raise(),
        ),
        (
            "definition",
            None,
            DEFINITION_INCLUDE_VALUESET_OID,
            [DEFINITION_INCLUDE_VALUESET_OID, DEFINITION_SINGLE_VALUESET_OID],
            does_not_raise(),
        ),
        (
            "definition",
            VALUESET_URL + "/" + DEFINITION_INCLUDE_VALUESET_OID,
            None,
            [DEFINITION_INCLUDE_VALUESET_OID, DEFINITION_SINGLE_VALUESET_OID],
            does_not_raise(),
        ),
        (
            "expansion",
            VALUESET_URL + "/" + EXPANSION_VALUESET_OID,
            None,
            [EXPANSION_VALUESET_OID],
            does_not_raise(),
        ),
        ("definition", None, None, [], pytest.raises(errors.ApiError)),
    ],
)
@responses.activate
def test_get_valueset(action, url, oid, expected_oids, raises):
    with raises:
        responses.add(responses.GET, AUTH_URL, body="true", status=200)
        responses.add(responses.GET, VALUESET_URL, status=404)
        responses.add(
            responses.GET,
            VALUESET_URL + "/" + DEFINITION_SINGLE_VALUESET_OID,
            body=get_valueset_data("definition_single_valueset.json"),
            status=200,
        )
        responses.add(
            responses.GET,
            VALUESET_URL + "/" + DEFINITION_INCLUDE_VALUESET_OID,
            body=get_valueset_data("definition_include_valueset.json"),
            status=200,
        )
        responses.add(
            responses.GET,
            VALUESET_URL + "/" + EXPANSION_VALUESET_OID + "/$expand",
            body=get_valueset_data("expansion_valueset.json"),
            status=200,
        )
        api = umls.UmlsApi(api_key="123")
        data = api.get_vsac_valuesets(action=action, url=url, oid=oid)
        assert len(data) == len(expected_oids)
        for i in range(0, len(expected_oids)):
            assert data[i]["id"] == expected_oids[i]


@mock.patch.dict(
    os.environ,
    clear=True,
)
@responses.activate
def test_download_umls(tmp_path):
    # this zip file is just an archive made by targeting the other .json files
    # in the same directory
    with open("./tests/test_data/apis/umls/umls.zip", "rb") as download_zip:
        responses.add(
            responses.GET,
            AUTH_URL,
            body="true",
            status=200,
            content_type="application/json",
        )
        responses.add(
            responses.GET,
            RELEASE_URL,
            body="""[{
                "fileName": "umls-2023AB-mrconso.zip",
                "releaseVersion": "2023AB",
                "releaseDate": "2023-11-06",
                "downloadUrl": "https://download.nlm.nih.gov/umls/kss/2023AB/umls-2023AB-mrconso.zip",
                "releaseType": "UMLS Metathesaurus MRCONSO File",
                "product": "UMLS",
                "current": true
                }]""",
            status=200,
            content_type="application/json",
        )
        responses.add(
            responses.GET,
            DOWNLOAD_URL,
            body=download_zip.read(),
            status=200,
            content_type="application/zip",
        )
        api = umls.UmlsApi(api_key="123")
        api.download_umls_files(path=tmp_path)
        downloads = os.listdir(tmp_path)
        assert len(downloads) == 2
        for file in [
            "single_valueset.json",
            "include_valueset.json",
        ]:
            assert file in downloads
        with pytest.raises(errors.ApiError):
            api.download_umls_files(path=tmp_path, target="foo")
