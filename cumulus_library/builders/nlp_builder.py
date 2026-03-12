"""Class for generating NLP results"""

import pathlib
import sys

import msgspec

from cumulus_library import BaseTableBuilder

# NLP is driven by a workflow config. See docs/workflows/nlp.md for more details
# on syntax and expectations.


class NlpShared(msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True):
    system_prompt: str | None = None
    user_prompt: str = "%CLINICAL-NOTE%"
    select_by_word: list[str] = msgspec.field(default_factory=list)
    select_by_regex: list[str] = msgspec.field(default_factory=list)
    select_by_athena_table: str = ""
    reject_by_word: list[str] = msgspec.field(default_factory=list)
    reject_by_regex: list[str] = msgspec.field(default_factory=list)


class NlpTask(NlpShared):
    version: int = 0
    response_schema: str = ""
    name: str = ""


class NlpWorkflow(msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True):
    config_type: str
    task: list[NlpTask]
    shared: NlpShared = msgspec.field(default_factory=NlpShared)


class NlpBuilder(BaseTableBuilder):
    def __init__(
        self,
        *args,
        toml_config_path: pathlib.Path | None = None,
        **kwargs,
    ):
        super().__init__()

        if toml_config_path:
            try:
                with open(toml_config_path, "rb") as file:
                    file_bytes = file.read()
                    self._workflow_config = msgspec.toml.decode(file_bytes, type=NlpWorkflow)

            except msgspec.ValidationError as e:
                sys.exit(
                    f"The NLP workflow at {toml_config_path!s} contains an unexpected param: \n{e}"
                )

        else:
            self._workflow_config = None

    def prepare_queries(
        self,
        *args,
        **kwargs,
    ):
        if not self._workflow_config:
            return
        # TODO: the actual NLP :D
