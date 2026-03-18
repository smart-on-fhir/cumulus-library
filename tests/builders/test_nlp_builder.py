import base64
import contextlib
import io
import json

import pytest

from cumulus_library import note_utils
from cumulus_library.builders import nlp_builder


def add_dxr(text: str | None, file) -> None:
    dxr = {"resourceType": "DiagnosticReport"}
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
        add_dxr(None, f)
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


def test_filter(tmp_path):
    workflow_path = f"{tmp_path}/nlp.workflow"
    with open(workflow_path, "w", encoding="utf8") as f:
        f.write("""
config_type="nlp"
[[task]]
name="filtered"
select_by_word=["fever"]
reject_by_word=["cold"]
[[task]]
name="all"
""")

    with open(f"{tmp_path}/dxr.ndjson", "w", encoding="utf8") as f:
        add_dxr(None, f)  # no text, will be skipped
        add_dxr("hello world", f)  # ignored by filters
        add_dxr("has fever", f)  # selected by filters
        add_dxr("has fever and cold", f)  # rejected by filters

    source = note_utils.NoteSource([tmp_path])

    builder = nlp_builder.NlpBuilder(toml_config_path=workflow_path, notes=source)

    expected_stats = """ Notes processed:
  Available:                4 
  Had text:                 3 
  Considered (filtered):    1 
  Got response (filtered):  0 
  Considered (all):         3 
  Got response (all):       0 """

    console_output = io.StringIO()
    with contextlib.redirect_stdout(console_output):
        builder.prepare_queries()
    assert expected_stats in console_output.getvalue()
