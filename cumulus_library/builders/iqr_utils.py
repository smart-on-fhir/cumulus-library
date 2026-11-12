import pathlib
import sys

import msgspec


class IqrWorkflowTable(msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True):
    name: str
    display_name: str
    source_table: str
    target_col: str


class IqrWorkflow(msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True):
    config_type: str
    distributions: list[IqrWorkflowTable]


def load_toml_config(toml_config_path: pathlib.Path) -> dict:
    # This is the most generic form of the msgspec toml loader - consider
    # making this generic if there's another case for it. The counts builder is
    # similar, but would need some more generic logic to handle class loading,
    # and both would need some mapping between the classes and the type field
    # so we could load the right spec for validation.
    try:
        with open(toml_config_path, "rb") as file:
            file_bytes = file.read()
            workflow_config = msgspec.to_builtins(msgspec.toml.decode(file_bytes, type=IqrWorkflow))
    except msgspec.ValidationError as e:
        sys.exit(f"The workflow at {toml_config_path!s} contains an unexpected param: \n{e}")
    return workflow_config
