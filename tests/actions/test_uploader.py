import contextlib
import json
import pathlib
import shutil
import zipfile
from contextlib import nullcontext as does_not_raise

import pytest
import requests
import responses
from responses import matchers

from cumulus_library.actions import (
    uploader,
)


def do_upload(
    *,
    login_error: bool = False,
    user: str = "user",
    id_token: str = "id",
    network: str | None = None,
    preview: bool = True,
    raises: contextlib.AbstractContextManager = does_not_raise(),
    status: int = 204,
    version: str | None = "12345.0",
    call_count: int = 2,
    data_path: pathlib.Path | None = pathlib.Path.cwd() / "tests/test_data/upload/",
    study: str = "upload",
    transaction=None,
    transaction_mismatch: bool = False,
):
    url = "https://upload.url.test/"
    if network:
        url += network
    with raises:
        if login_error:
            responses.add(responses.POST, url, status=401)
        elif transaction_mismatch:
            responses.add(
                responses.POST, url, status=412, json=json.dumps("Study in processing, try again")
            )
        else:
            responses.add(
                responses.POST,
                url,
                match=[
                    matchers.json_params_matcher(
                        {
                            "study": study,
                            "data_package_version": int(float(version)),
                            "filename": f"{user}_{study}.zip",
                        }
                    )
                ],
                json={"url": "https://presigned.url.test", "fields": {"a": "b"}},
            )
        args = {
            "data_path": data_path,
            "id": id_token,
            "preview": preview,
            "target": [study],
            "network": network,
            "url": "https://upload.url.test/",
            "user": user,
        }
        responses.add(responses.POST, "https://presigned.url.test/", status=status)
        uploader.upload_files(args)
        responses.assert_call_count(url, call_count)


@pytest.mark.parametrize(
    "user,id_token,status,network,login_error,preview,call_count,raises",
    [
        (None, None, 204, None, False, False, None, pytest.raises(SystemExit)),
        ("user", "id", 204, None, False, False, 1, does_not_raise()),
        ("user", "id", 204, "network", False, False, 1, does_not_raise()),
        (
            "user",
            "id",
            500,
            None,
            False,
            False,
            None,
            pytest.raises(requests.exceptions.HTTPError),
        ),
        (
            "baduser",
            "badid",
            204,
            None,
            True,
            False,
            None,
            pytest.raises(requests.exceptions.HTTPError),
        ),
        (
            "user",
            "id",
            204,
            None,
            False,
            True,
            1,
            does_not_raise(),
        ),
    ],
)
@responses.activate
def test_upload_data(
    user,
    id_token,
    status,
    network,
    preview,
    login_error,
    call_count,
    raises,
    transaction=None,
    transaction_mismatch=None,
):
    do_upload(
        user=user,
        id_token=id_token,
        status=status,
        network=network,
        preview=preview,
        login_error=login_error,
        call_count=call_count,
        raises=raises,
        transaction=transaction,
        transaction_mismatch=transaction_mismatch,
    )


def test_upload_data_no_path():
    with pytest.raises(SystemExit):
        do_upload(data_path=None)


def remove_from_zip(src, dest, files):
    with zipfile.ZipFile(src) as old:
        with zipfile.ZipFile(dest, "w") as new:
            for info in old.infolist():
                if info.filename not in files:
                    new.writestr(info, old.read(info))
    src.unlink()


@responses.activate
def test_upload_data_no_version(tmp_path):
    src = pathlib.Path(__file__).resolve().parents[1] / "test_data/upload/upload.zip"
    dest = pathlib.Path(tmp_path) / "upload"
    dest.mkdir()
    shutil.copy(src, dest / "upload_tmp.zip")
    remove_from_zip(
        dest / "upload_tmp.zip", dest / "upload.zip", "upload__meta_version.meta.parquet"
    )
    do_upload(data_path=dest, version="0", call_count=1)


@responses.activate
def test_upload_data_no_meta_date(tmp_path):
    with pytest.raises(SystemExit):
        src = pathlib.Path(__file__).resolve().parents[1] / "test_data/upload/upload.zip"
        dest = pathlib.Path(tmp_path) / "upload"
        dest.mkdir()
        shutil.copy(src, dest / "upload_tmp.zip")
        remove_from_zip(
            dest / "upload_tmp.zip", dest / "upload.zip", "upload__meta_date.meta.parquet"
        )
        do_upload(data_path=dest, version="12345", call_count=1)


@responses.activate
def test_upload_data_unexpected_data(tmp_path):
    with pytest.raises(SystemExit):
        src = pathlib.Path(__file__).resolve().parents[1] / "test_data/upload/upload.zip"
        dest = pathlib.Path(tmp_path) / "upload"
        dest.mkdir()
        shutil.copy(src, dest / "upload.zip")
        with zipfile.ZipFile(dest / "upload.zip", "a") as f:
            f.writestr("foo", "test.txt")
        do_upload(data_path=dest, version="12345", call_count=1)


@responses.activate
def test_upload_discovery(tmp_path):
    src = pathlib.Path(__file__).resolve().parents[1] / "test_data/upload/upload.zip"
    dest = pathlib.Path(tmp_path) / "discovery/discovery.zip"
    dest.parent.mkdir()
    shutil.copyfile(src, dest)
    do_upload(data_path=dest.parent, call_count=1, study="discovery")


@responses.activate
def test_upload_transaction_in_progress(tmp_path):
    src = pathlib.Path(__file__).resolve().parents[1] / "test_data/upload/upload.zip"
    dest = pathlib.Path(tmp_path) / "upload"
    dest.mkdir()
    shutil.copy(src, dest)
    with pytest.raises(SystemExit):
        do_upload(
            data_path=dest,
            version="0",
            call_count=1,
            preview=False,
            transaction_mismatch=True,
        )
