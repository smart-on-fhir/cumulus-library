import pytest

from cumulus_library.builders import nlp_builder


def test_ignored_with_no_args():
    builder = nlp_builder.NlpBuilder()
    builder.prepare_queries()  # just confirm it doesn't blow up or do something bad


def test_unexpected_config_field(tmp_path):
    workflow_path = f"{tmp_path}/nlp.workflow"
    with open(workflow_path, "w", encoding="utf8") as f:
        f.write("""
config_type="nlp"
extra_field="yup"
""")
    with pytest.raises(SystemExit, match="contains an unexpected param:"):
        nlp_builder.NlpBuilder(toml_config_path=workflow_path)
