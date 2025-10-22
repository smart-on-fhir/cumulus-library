import json
import os
from contextlib import nullcontext as does_not_raise
from unittest import mock

import pytest
import responses

from cumulus_library import errors
from cumulus_library.apis import loinc

LOINC_VERSIONS = [
    {
        "version": "1.1",
        "releaseDate": "2021-01-00T00:00:00",
        "relmaVersion": None,
        "numberOfLoincs": 10001,
        "maxLoinc": None,
        "downloadUrl": "https://loinc.regenstrief.org/api/v1/Loinc/Download?version=1.1",
        "downloadMD5Hash": "hashstring",
    },
    {
        "version": "1.0",
        "releaseDate": "2020-01-00T00:00:00",
        "relmaVersion": None,
        "numberOfLoincs": 10000,
        "maxLoinc": None,
        "downloadUrl": "https://loinc.regenstrief.org/api/v1/Loinc/Download?version=1.0",
        "downloadMD5Hash": "hashstring",
    },
]


@pytest.mark.parametrize(
    "user,password,env,raises",
    [
        ("user", "pass", {}, does_not_raise()),
        (None, "pass", {"LOINC_USER": "user"}, does_not_raise()),
        ("user", None, {"LOINC_PASSWORD": "pass"}, does_not_raise()),
        (None, None, {"LOINC_USER": "user", "LOINC_PASSWORD": "pass"}, does_not_raise()),
        (None, "pass", {}, pytest.raises(errors.ApiError)),
        ("user", None, {}, pytest.raises(errors.ApiError)),
    ],
)
@mock.patch.dict(
    os.environ,
    clear=True,
)
def test_api_init(user, password, env, raises):
    with raises:
        for k, v in env.items():
            os.environ[k] = v
        api = loinc.LoincApi(user=user, password=password)
        api.session.auth.username == user or os.environ["LOINC_USER"]
        api.session.auth.password == password or os.environ["LOINC_PASSWORD"]


@responses.activate
@pytest.mark.parametrize(
    "status,expected,raises",
    [(200, ["1.1", "1.0"], does_not_raise()), (401, None, pytest.raises(errors.ApiError))],
)
@mock.patch.dict(
    os.environ,
    clear=True,
)
def test_all_versions(status, expected, raises):
    payload = json.dumps(LOINC_VERSIONS)
    responses.add(responses.GET, f"{loinc.BASE_URL}Loinc/All", body=payload, status=status)
    with raises:
        api = loinc.LoincApi(user="user", password="password")
        versions = api.get_all_download_versions()
        if status == 200:
            assert versions == expected


@responses.activate
@pytest.mark.parametrize(
    "version,status,expected,raises",
    [
        (
            "1.1",
            200,
            ("1.1", "https://loinc.regenstrief.org/api/v1/Loinc/Download?version=1.1"),
            does_not_raise(),
        ),
        (
            None,
            200,
            ("1.1", "https://loinc.regenstrief.org/api/v1/Loinc/Download?version=1.1"),
            does_not_raise(),
        ),
        (
            "1.0",
            200,
            ("1.0", "https://loinc.regenstrief.org/api/v1/Loinc/Download?version=1.0"),
            does_not_raise(),
        ),
        ("bad_auth", 401, None, pytest.raises(errors.ApiError)),
        ("1.2", 404, None, pytest.raises(errors.ApiError)),
    ],
)
@mock.patch.dict(
    os.environ,
    clear=True,
)
def test_download_info(version, status, expected, raises):
    responses.add(
        responses.GET,
        f"{loinc.BASE_URL}Loinc",
        match=[responses.matchers.query_param_matcher({})],
        body=json.dumps(LOINC_VERSIONS[0]),
        status=200,
    )
    responses.add(
        responses.GET,
        f"{loinc.BASE_URL}Loinc",
        match=[responses.matchers.query_param_matcher({"version": "1.1"})],
        body=json.dumps(LOINC_VERSIONS[0]),
        status=200,
    )
    responses.add(
        responses.GET,
        f"{loinc.BASE_URL}Loinc",
        match=[responses.matchers.query_param_matcher({"version": "1.0"})],
        body=json.dumps(LOINC_VERSIONS[1]),
        status=200,
    )
    responses.add(
        responses.GET,
        f"{loinc.BASE_URL}Loinc",
        match=[responses.matchers.query_param_matcher({"version": "1.2"})],
        body=None,
        status=404,
    )
    responses.add(
        responses.GET,
        f"{loinc.BASE_URL}Loinc",
        match=[responses.matchers.query_param_matcher({"version": "bad_auth"})],
        body=None,
        status=401,
    )
    with raises:
        api = loinc.LoincApi(user="user", password="password")
        data = api.get_download_info(version=version)
        if status == 200:
            assert data == expected


@responses.activate
@mock.patch("cumulus_library.apis.loinc.LoincApi.get_download_info")
def test_download_dataset(mock_url, tmp_path):
    # we're not using a mock loinc payload here - just checking that we got a zip and opened it
    with open("./tests/test_data/upload/upload.zip", "rb") as f:
        responses.add(
            responses.GET,
            "http://mock_url",
            status=200,
            body=f.read(),
            match=[responses.matchers.request_kwargs_matcher({"stream": True})],
            headers={"Content-Length": "3656"},
        )
    mock_url.return_value = ("1.0", "http://mock_url")
    api = loinc.LoincApi(user="user", password="password")
    api.download_loinc_dataset(path=tmp_path)
    files = list(tmp_path.glob("1.0/*"))
    assert sorted([x.name for x in files]) == [
        "manifest.toml",
        "upload__count_synthea_patient.cube.parquet",
        "upload__meta_date.meta.parquet",
        "upload__meta_version.meta.parquet",
    ]
    for file in files:
        file.unlink()
    (tmp_path / "1.0").rmdir()

    api.download_loinc_dataset(
        version="1.0", download_url="http://mock_url", path=tmp_path, unzip=False
    )
    files = tmp_path.glob("*")
    assert any("1.0.zip" == x.name for x in files)

    # since we've got a download, should just unzip it
    api.download_loinc_dataset(path=tmp_path)
    files = list(tmp_path.glob("*"))
    assert not any("manifest.toml" == x.name for x in files)
