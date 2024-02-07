import json
from pathlib import Path

from tests.conftest import ID_PATHS, MOCK_DATA_DIR, ndjson_data_generator


def test_ndjson_data_generator(tmp_path):
    iters = 20
    target = tmp_path
    # generating outside tmp storage for debugging
    # target = Path(MOCK_DATA_DIR + '/test_output')
    ndjson_data_generator(Path(MOCK_DATA_DIR), target, iters)
    for key in ID_PATHS:
        for filepath in [f for f in Path(target / key).iterdir()]:
            with open(filepath) as f:
                first_new = json.loads(next(f))
                *_, last_new = f
                last_new = json.loads(last_new)
            with open(f"{Path(MOCK_DATA_DIR)}/{key}/{filepath.name}") as f:
                first_line = next(f)
                first_ref = json.loads(first_line)
                # handling patient file of length 1:
                try:
                    *_, last_ref = f
                    last_ref = json.loads(last_ref)
                except ValueError:
                    last_ref = first_ref
            for source in [[first_new, first_ref, 0], [last_new, last_ref, iters - 1]]:
                for id_path in ID_PATHS[key]:
                    new_test = source[0]
                    ref_test = source[1]
                    for subkey in id_path:
                        if new_test is None:
                            break
                        if isinstance(new_test, list):
                            new_test = new_test[0].get(subkey)
                            ref_test = ref_test[0].get(subkey)
                        else:
                            new_test = new_test.get(subkey)
                            ref_test = ref_test.get(subkey)
                    if ref_test is not None:
                        assert new_test == ref_test + str(source[2])
                    else:
                        assert ref_test == new_test
