import binascii
import json
import os
from unittest import mock

import pytest

from cumulus_library import (
    cli_parser,
    note_utils,
)

# Some convenience salt values to use
SALT_STR = "e359191164cd209708d93551f481edd048946a9d844c51dea1b64d3f83dfd1fa"
SALT_BYTES = binascii.unhexlify(SALT_STR)


@mock.patch("rich.progress.Progress.advance")
def test_note_source_iter(mock_advance, tmp_path):
    dxr1 = json.dumps({"resourceType": "DiagnosticReport", "id": "dxr1"})
    dxr2 = json.dumps({"resourceType": "DiagnosticReport", "id": "dxr2"})
    docref1 = json.dumps({"resourceType": "DocumentReference", "id": "docref1"})
    docref2 = json.dumps({"resourceType": "DocumentReference", "id": "docref2"})

    os.makedirs(f"{tmp_path}/1")
    with open(f"{tmp_path}/1/dxr.ndjson", "w", encoding="utf8") as f:
        f.write(f"{dxr1}\n{dxr2}")
    # Write patient file, which should be ignored
    with open(f"{tmp_path}/1/pat.ndjson", "w", encoding="utf8") as f:
        json.dump({"resourceType": "Patient", "id": "pat1"}, f)

    os.makedirs(f"{tmp_path}/2/subdir")  # confirm we are checking recursively w/ a subdir
    with open(f"{tmp_path}/2/subdir/docref.ndjson", "w", encoding="utf8") as f:
        f.write(f"{docref1}\n{docref2}")

    source = note_utils.NoteSource([f"{tmp_path}/1", f"{tmp_path}/2"])

    ids = [x["id"] for x in source.progress_iter("testing")]
    assert ids == ["dxr1", "dxr2", "docref1", "docref2"]

    skips = [call[0][1] for call in mock_advance.call_args_list]
    assert skips == [0, len(dxr1) + 1, len(dxr2), 0, len(docref1) + 1, len(docref2)]

    # Confirm we can iterate twice
    ids = [x["id"] for x in source.progress_iter("testing")]
    assert ids == ["dxr1", "dxr2", "docref1", "docref2"]


def test_note_source_phi_dir(tmp_path):
    """Just confirm we parse the salt from a codebook file"""
    with open(f"{tmp_path}/codebook.json", "w") as f:
        json.dump({"id_salt": SALT_STR}, f)

    config = note_utils.NlpConfig({"etl_phi_dir": tmp_path})
    assert config.salt == SALT_BYTES


def test_get_table_refs():
    cursor = mock.MagicMock()
    cursor.execute.return_value.description = [["documentreference_id"]]
    cursor.execute.return_value.fetchall.return_value = [("a",), ("b",)]

    refs = note_utils.get_table_refs(cursor, "my_table")
    assert list(refs) == ["DocumentReference/a", "DocumentReference/b"]


def test_get_table_refs_bad_table():
    with pytest.raises(ValueError, match="Invalid SQL table name"):
        note_utils.get_table_refs(None, "table; drop USERS")


def test_nlp_args_reach_the_config():
    """Every NLP flag should actually land on NlpConfig."""
    parser, _defaults = cli_parser.get_parser()
    args = vars(
        parser.parse_args(
            [
                "build",
                "--target=my_study",
                "--nlp-model=gpt4o",
                "--nlp-provider=azure",
                "--azure-deployment=dep-a",
                "--azure-deployment=dep-b",
                "--nlp-concurrency=7",
                "--nlp-chunksize=13",
                "--batch-nlp",
                "--clean-nlp",
                "--no-nlp-stats",
                "--nlp-subtask=age",
                "--nlp-subtask=race",
                "--etl-phi-dir=/tmp/phi",
            ]
        )
    )

    config = note_utils.NlpConfig(args)
    default = note_utils.NlpConfig()

    from_cli = {
        "model": "gpt4o",
        "provider": "azure",
        "azure_deployments": ["dep-a", "dep-b"],
        "concurrency": 7,
        "chunksize": 13,
        "use_batching": True,
        "clean": True,
        "show_stats": False,
        "subtasks": ["age", "race"],
        "phi_dir": "/tmp/phi",
        "target": "my_study",
    }
    for field, value in from_cli.items():
        assert getattr(config, field) == value, f"--{field} did not reach NlpConfig"
        assert getattr(default, field) != value, (
            f"the test value for {field} matches its default, so this assertion proves nothing"
        )

    # The other half of the same trap: argparse puts every flag in the dict even when it isn't
    # passed, holding None. So a config reading it as args.get(key, fallback) finds the key,
    # gets None back, and the fallback never fires - leaving a None where a number belongs.
    bare = vars(parser.parse_args(["build", "--target=my_study", "--nlp-model=gpt4o"]))
    bare_config = note_utils.NlpConfig(bare)
    assert bare_config.chunksize == 100000
    assert bare_config.concurrency == 1
    assert bare_config.provider == "local"
    assert bare_config.azure_deployments == []
    assert bare_config.use_batching is False
    assert bare_config.clean is False


def test_nlp_config_defaults():
    """Every NlpConfig fallback should actually be the same as argparse's default."""
    parser, _ = cli_parser.get_parser()
    # Parse a bare "build" command, which is the only one that actually uses NLP. This gives us
    # the argparse defaults for every NLP flag, which we can compare to NlpConfig's
    parsed = vars(parser.parse_args(["build"]))

    # Args that are allowed to be None (or empty list)
    nullable = [
        "azure_deployments",
        "chunksize",
        "etl_phi_dir",
        "nlp_concurrency",
        "nlp_model",
        "nlp_subtasks",
        "target",
    ]
    # All NLP specific keys
    nlp_keys = [*nullable, "batch_nlp", "clean_nlp", "nlp_provider", "nlp_stats"]

    # Confirm that the nullable list is up to date with what argparse actually produces
    assert sorted(k for k in nlp_keys if parsed.get(k) is None) == sorted(nullable), (
        "the nullable-key list has drifted from what argparse actually produces"
    )

    # Expected defaults based on note_utils (except for show_stats, which we check separately)
    expected = {
        "azure_deployments": [],
        "chunksize": 100000,
        "clean": False,
        "concurrency": 1,
        "model": None,
        "phi_dir": None,
        "provider": "local",
        "salt": None,
        "subtasks": None,
        "target": None,
        "use_batching": False,
    }
    # Variations on nlp configs
    shapes = {
        "no keys at all": note_utils.NlpConfig(),
        "nullable keys present but None": note_utils.NlpConfig(dict.fromkeys(nullable)),
        "a real parse with no NLP flags": note_utils.NlpConfig(parsed),
    }

    # Test that each variation of NlpConfig has the expected defaults for each field.
    for shape, config in shapes.items():
        for field, value in expected.items():
            assert getattr(config, field) == value, f"{field} default is wrong given {shape}"

    # show_stats is the one field that differs by shape, and legitimately: --no-nlp-stats is a
    # store_false so argparse always supplies True, while a bare dict has no key to read and
    # falls through to None. Pinned here so that changing either side stays deliberate.
    assert shapes["no keys at all"].show_stats is None
    assert shapes["nullable keys present but None"].show_stats is None
    assert shapes["a real parse with no NLP flags"].show_stats is True
