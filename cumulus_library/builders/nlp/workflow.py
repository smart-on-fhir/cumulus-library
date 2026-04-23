import msgspec


class NlpShared(msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True):
    system_prompt: str | None = None
    user_prompt: str | None = None
    select_by_word: list[str] | None = None
    select_by_regex: list[str] | None = None
    select_by_table: str | None = None  # TODO
    reject_by_word: list[str] | None = None
    reject_by_regex: list[str] | None = None


class NlpTask(NlpShared):
    version: int = 0
    response_schema: str = ""
    name: str = ""


class NlpWorkflow(msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True):
    config_type: str
    task: list[NlpTask]
    shared: NlpShared = msgspec.field(default_factory=NlpShared)
