import base64
import binascii
import contextlib
import io
import json
import os
from unittest import mock

import cumulus_fhir_support as cfs
import pytest

from cumulus_library import cli, note_utils
from cumulus_library.builders import nlp_builder
from tests.conftest import duckdb_args

SALT_STR = "e359191164cd209708d93551f481edd048946a9d844c51dea1b64d3f83dfd1fa"
SALT_BYTES = binascii.unhexlify(SALT_STR)


def add_dxr(id_val: str, text: str | None, file) -> None:
    dxr = {"resourceType": "DiagnosticReport", "id": id_val}
    if text is not None:
        dxr["presentedForm"] = [
            {
                "contentType": "text/plain",
                "data": base64.standard_b64encode(text.encode()).decode(),
            }
        ]
    json.dump(dxr, file)
    file.write("\n")


@pytest.fixture
def note_source(tmp_path) -> note_utils.NoteSource:
    """Just make a sample note source with a row - contents not important"""
    with open(f"{tmp_path}/dxr.ndjson", "w", encoding="utf8") as f:
        add_dxr("empty", None, f)
    yield note_utils.NoteSource([tmp_path])


def test_unexpected_config_field(tmp_path, note_source):
    workflow_path = f"{tmp_path}/nlp.workflow"
    with open(workflow_path, "w", encoding="utf8") as f:
        f.write("""
config_type="nlp"
extra_field="yup"
""")
    with pytest.raises(SystemExit, match="contains unknown field `extra_field`"):
        nlp_builder.NlpBuilder(toml_config_path=workflow_path, notes=note_source)


def test_empty_note_dir(tmp_path):
    workflow_path = f"{tmp_path}/nlp.workflow"
    with open(workflow_path, "w", encoding="utf8") as f:
        f.write('config_type="nlp"\n[[task]]')
    with pytest.raises(SystemExit, match="there are no notes to work with"):
        nlp_builder.NlpBuilder(toml_config_path=workflow_path, notes=note_utils.NoteSource())


def test_table_filter_but_no_salt(tmp_path, note_source, mock_db_config):
    workflow_path = f"{tmp_path}/nlp.workflow"
    with open(workflow_path, "w", encoding="utf8") as f:
        f.write("""
config_type="nlp"
[[task]]
select_by_table="table"
""")
    builder = nlp_builder.NlpBuilder(toml_config_path=workflow_path, notes=note_source)
    err_msg = "Cannot calculate anonymized resource IDs without a PHI dir defined"
    with pytest.raises(RuntimeError, match=err_msg):
        builder.prepare_queries(config=mock_db_config)


def test_flattened_config(tmp_path, note_source):
    workflow_path = f"{tmp_path}/nlp.workflow"
    with open(workflow_path, "w", encoding="utf8") as f:
        f.write("""
config_type="nlp"
[shared]
system_prompt="hello"
[[task]]
name="override"
system_prompt="bye"
[[task]]
name="fallthrough"
""")
    builder = nlp_builder.NlpBuilder(toml_config_path=workflow_path, notes=note_source)
    assert builder._workflow_config.task[0].system_prompt == "bye"
    assert builder._workflow_config.task[1].system_prompt == "hello"


def test_filter(tmp_path, mock_db_config):
    codebook_path = f"{tmp_path}/codebook.json"
    with open(codebook_path, "w", encoding="utf8") as f:
        json.dump({"id_salt": SALT_STR}, f)

    workflow_path = f"{tmp_path}/nlp.workflow"
    with open(workflow_path, "w", encoding="utf8") as f:
        f.write("""
config_type="nlp"
[[task]]
name="filtered"
select_by_word=["fever"]
reject_by_word=["cold"]
select_by_table="prev_table"
[[task]]
name="all"
""")

    with open(f"{tmp_path}/dxr.ndjson", "w", encoding="utf8") as f:
        add_dxr("1", None, f)  # no text, will be skipped
        add_dxr("2", "hello world", f)  # ignored by filters
        add_dxr("3", "has fever", f)  # selected by filters
        add_dxr("4", "has fever and cold", f)  # rejected by filters
        add_dxr("5", "has fever", f)  # would be selected but is excluded by table

    expected_stats = """ Notes processed:
  Available:                5 
  Had text:                 4 
  Considered (filtered):    1 
  Got response (filtered):  0 
  Considered (all):         4 
  Got response (all):       0 """

    mock_db_config.db.cursor().execute(f"""
        CREATE TABLE prev_table AS SELECT * FROM (
            VALUES
            ('{cfs.anon_id("1", SALT_BYTES)}'),
            ('{cfs.anon_id("2", SALT_BYTES)}'),
            ('{cfs.anon_id("3", SALT_BYTES)}'),
            ('{cfs.anon_id("4", SALT_BYTES)}')
        )
        AS t (diagnosticreport_id)
    """)

    source = note_utils.NoteSource([tmp_path], phi_dir=tmp_path)

    builder = nlp_builder.NlpBuilder(toml_config_path=workflow_path, notes=source)

    console_output = io.StringIO()
    with contextlib.redirect_stdout(console_output):
        builder.prepare_queries(config=mock_db_config)
    assert expected_stats in console_output.getvalue()


@mock.patch.dict(os.environ, clear=True)
@mock.patch("cumulus_library.builders.nlp_builder.NlpBuilder")
def test_args_passed_down(mock_builder, tmp_path):
    os.makedirs(f"{tmp_path}/notes")
    with open(f"{tmp_path}/notes/dxr.ndjson", "w", encoding="utf8") as f:
        dxr = {"resourceType": "DiagnosticReport", "id": "1"}
        json.dump(dxr, f)

    os.makedirs(f"{tmp_path}/phi")
    with open(f"{tmp_path}/phi/codebook.json", "w", encoding="utf8") as f:
        json.dump({"id_salt": SALT_STR}, f)

    mock_builder.side_effect = RuntimeError("nope")

    build_args = duckdb_args(
        [
            "build",
            str(tmp_path),
            "--target=example_nlp",
            "--stage=nlp",
            f"--note-dir={tmp_path}",
            f"--etl-phi-dir={tmp_path}/phi",
        ],
        tmp_path,
    )

    with pytest.raises(RuntimeError, match="nope"):
        cli.main(cli_args=build_args)

    source = mock_builder.call_args[1]["notes"]
    assert source.salt == SALT_BYTES
    assert list(source.progress_iter("label")) == [dxr]
