"""test util for accessing mock toml configs"""


def get_mock_toml(key: str):
    """convenience method for in-mem toml object"""
    return mock_manifests[key]


mock_manifests = {
    "valid": {
        "study_prefix": "valid",
        "sql_config": {"file_names": ["test1.sql", "test2.sql"]},
        "export_config": {"export_list": ["valid__table1", "valid__table2"]},
    },
    "valid_empty_arrays": {
        "study_prefix": "valid_empty",
        "sql_config": {"file_names": []},
        "export_config": {"export_list": []},
    },
    "valid_null_arrays": {
        "study_prefix": "valid_null_arrays",
        "sql_config": {"file_names": None},
        "export_config": {"export_list": None},
    },
    "valid_only_prefix": {
        "study_prefix": "valid_only_prefix",
    },
    "valid_extra_data": {
        "study_prefix": "valid_extra_data",
        "unexpected_key": "unexpected_value",
    },
    "invalid_bad_export_names": {
        "study_prefix": "valid",
        "sql_config": {"file_names": ["test1.sql", "test2.sql"]},
        "export_config": {"export_list": ["wrong__table1", "wrong__table2"]},
    },
    "invalid_none": "",
}
