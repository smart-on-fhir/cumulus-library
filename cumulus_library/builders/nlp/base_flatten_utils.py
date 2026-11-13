"""
The vocabulary for flattening NLP result tables, as a study's TOML config declares it.

An NLP task's output table (see ``NlpBuilder``) holds the LLM's answer in a nested ``result``
struct. Studies usually want that unpacked into something easier to query, in one of two shapes:

- A *wide* table: one row per note (or per UNNESTed mention), one column per field you pick
  out of ``result``. See ``NlpWideBuilder``.
- A *highlight* table: one row per supporting text span, shaped for upload to Label Studio.
  See ``NlpHighlightBuilder``.

Both are declared as data and rendered by a generic template, so the terms are shared:

* ``root`` means "the alias a path hangs off". It is either RESULT_ROOT or an UNNEST alias.
* ``path`` means "the drill-down under that root".
* ``unnest_paths`` (wide) and ``within`` (highlight) are similar but distinct:
    * A wide table declares collections once for the whole table; columns then pick a root by name.
    * A highlight table declares them per mention, because each mention becomes its own SELECT.

This module has two layers: the msgspec structs that validate the TOML as it is loaded, and the
``normalize_*`` functions that resolve every default so that the templates only render and
never derive anything.
"""

from typing import Any, Literal, TypedDict, get_args

import msgspec

from cumulus_library import errors
from cumulus_library.builders.nlp import pydantic_primitives

# The alias of the source table inside both templates, and the struct every path is relative
# to unless it hangs off an UNNESTed collection instead.
SOURCE_ALIAS = "nlp"
RESULT_ROOT = f"{SOURCE_ALIAS}.result"

# The leading columns of every wide table, in order. Fixed by the template and downstream of
# the NLP driver's output schema, so that every wide table shares the same leading shape.
METADATA_COLUMNS = [
    "note_ref",
    "encounter_ref",
    "subject_ref",
    "origin",
    "generated_on",
    "task_version",
    "system_fingerprint",
]

# The columns of every highlight table, in order. Fixed by the template.
HIGHLIGHT_COLUMNS = [
    "note_ref",
    "subject_ref",
    "generated_on",
    "task_version",
    "system_fingerprint",
    "origin",
    "label",
    "span",
    "sublabel_name",
    "sublabel_value",
]

# How an UNNESTed collection joins to its root. 'cross' drops a mention whose list is empty;
# 'left' keeps one row with NULLs, as for a medication's adverse events.
JoinKind = Literal["cross", "left"]

# How a highlight's value is required to be present:
#   not_null      -> value IS NOT NULL
#   not_mentioned -> value != 'NOT_MENTIONED'
#   nota          -> value != 'NONE_OF_THE_ABOVE'
FilterKind = Literal["not_null", "not_mentioned", "nota"]
FILTER_KINDS = get_args(FilterKind)

# What each FilterKind compares against, where it compares at all
FILTER_SENTINELS = {
    "not_mentioned": pydantic_primitives.NOT_MENTIONED,
    "nota": pydantic_primitives.NONE_OF_THE_ABOVE,
}

# Instead of adding these to every class, we define them here and splat them below.
_STRUCT_OPTIONS = {"forbid_unknown_fields": True, "omit_defaults": True}


###############################################################################
# TOML-facing structs
###############################################################################
class UnnestSpec(msgspec.Struct, **_STRUCT_OPTIONS):
    """One collection to UNNEST, for a wide table.

    Only ``path`` is required; a bare string in ``unnest_paths`` is shorthand for
    ``{path = s}``. The alias defaults to the last segment of the path, and the root to the
    previous unnest's alias (or RESULT_ROOT for the first).
    """

    path: str
    alias: str | None = None
    root: str | None = None
    join: JoinKind = "cross"


class ColumnSpec(msgspec.Struct, **_STRUCT_OPTIONS):
    """An output column for a wide table. Either ``path`` or ``expr`` must be set, not both.

    A ``path`` column reads a value from a struct path. ``transform`` is a single-argument SQL
    function wrapped around it (e.g. UPPER). A bare string in ``columns`` is shorthand for
    ``{path = s}``.

    An ``expr`` column is raw SQL, for values no single path can express. ``alias`` is required
    there because there is no path to derive one from. The expression is interpolated verbatim,
    so it must be valid in every backend the study builds against (Athena and DuckDB). The
    source table is always aliased ``nlp``.

    ``sql_type`` is the column type used when there are no source tables and an empty table has
    to be created instead (default varchar).
    """

    path: str | None = None
    expr: str | None = None
    alias: str | None = None
    root: str | None = None
    transform: str | None = None
    sql_type: str = "varchar"


class HighlightSpec(msgspec.Struct, **_STRUCT_OPTIONS):
    """One Label Studio highlight row source, for a highlight table.

    ls_variable_name -> Label Studio variable name, emitted as ``label``
    ls_subvalue_name -> emitted as ``sublabel_name``
    path             -> path under the mention root to the value, emitted as
                        ``sublabel_value``. The parent mention struct is everything before
                        its last segment.
    span_path        -> a str (one spans array; one highlight row per span) or a list of str
                        (k parallel spans arrays merged into one min:max span; one highlight
                        row per mention). Use the list form when one Label Studio entity is
                        described by sibling concepts whose spans are scattered through the
                        note (e.g. a medication's class, status, and response), because
                        highlights cannot express relations between separate mentions.
    require          -> how the value must be present; omit for no filter at all
    within           -> collections to UNNEST before reaching the mention root; when omitted,
                        the mention root is RESULT_ROOT
    """

    ls_variable_name: str
    ls_subvalue_name: str
    path: str
    span_path: str | list[str]
    require: FilterKind | None = None
    within: list[str] = msgspec.field(default_factory=list)


class _TableBase(msgspec.Struct, **_STRUCT_OPTIONS):
    # The NLP task whose result tables feed this one. Defaults to the table's key, which is
    # right unless one task feeds several tables of the same kind (genetic findings holds two
    # independent lists, and UNNESTing both into one table would cross them).
    source_task: str | None = None
    # Only rows from these task_version values are used; empty means no filter.
    task_versions: list[int] = msgspec.field(default_factory=list)
    # Restrict the source tables to these model slugs (as in `my_study__nlp_<task>_<model>`).
    # By default, every table found for the task is used.
    source_models: list[str] = msgspec.field(default_factory=list)


class WideTable(_TableBase):
    unnest_paths: list[str | UnnestSpec] = msgspec.field(default_factory=list)
    columns: list[str | ColumnSpec] = msgspec.field(default_factory=list)


class HighlightTable(_TableBase):
    mentions: list[HighlightSpec] = msgspec.field(default_factory=list)


class WideWorkflow(msgspec.Struct, **_STRUCT_OPTIONS):
    config_type: str
    tables: dict[str, WideTable]


class HighlightWorkflow(msgspec.Struct, **_STRUCT_OPTIONS):
    config_type: str
    tables: dict[str, HighlightTable]


###############################################################################
# After normalization: what the templates receive
###############################################################################
class NormalizedUnnest(TypedDict):
    """An UnnestSpec after defaulting and validation: every key present, ready to render."""

    path: str
    alias: str
    root: str
    join: JoinKind


class NormalizedColumn(TypedDict, total=False):
    """A ColumnSpec after defaulting and validation.

    ``alias`` and ``sql_type`` are always present. The rest is one of two shapes, matching the
    template's branches: ``expr``, or ``root`` + ``path`` with an optional ``transform``.
    """

    alias: str
    sql_type: str
    root: str
    path: str
    transform: str
    expr: str


class NormalizedHighlight(TypedDict):
    """A HighlightSpec after defaulting and validation.

    Everything the template would otherwise work out for itself is decided here instead:

        unnests      -> the resolved UNNEST chain, same shape the wide template consumes
        root         -> RESULT_ROOT, or the last unnest's alias
        mention_path -> ``path`` minus its last segment: the parent struct that must be
                        non-NULL for the row to mean anything
        merge_spans  -> True when span_path is a list, so the template branches on a bool
        require      -> None when no filter was declared, rather than a missing key
        sentinel     -> the literal the value is compared to for `not_mentioned` and `nota`
    """

    ls_variable_name: str
    ls_subvalue_name: str
    path: str
    span_path: str | list[str]
    require: FilterKind | None
    sentinel: str | None
    within: list[str]
    unnests: list[NormalizedUnnest]
    root: str
    mention_path: str
    merge_spans: bool


###############################################################################
# Normalization
###############################################################################
def _as_dict(entry: Any) -> dict[str, Any]:
    """Turns a bare string or a struct into a mutable dict, dropping unset (None) fields"""
    if isinstance(entry, str):
        return {"path": entry}
    return {k: v for k, v in msgspec.structs.asdict(entry).items() if v is not None}


def normalize_unnests(
    entries: list[str | UnnestSpec],
    *,
    owner: str,
    auto_alias: bool = False,
) -> list[NormalizedUnnest]:
    """Resolves a chain of collections to UNNEST.

    Each entry hangs off the previous one's alias, or off RESULT_ROOT if it is the first.

    - A bare entry string is shorthand for ``{path = s}``.
    - ``result`` is accepted as a synonym for RESULT_ROOT.
    - ``auto_alias`` names each entry ``mention0``, ``mention1``, ... instead of deriving the
      alias from the path. Highlights number their aliases that way.
    - ``owner`` is the table name, used for error attribution.
    """
    normalized: list[NormalizedUnnest] = []
    for entry in entries:
        entry = _as_dict(entry)
        if auto_alias:
            entry.setdefault("alias", f"mention{len(normalized)}")
        else:
            entry.setdefault("alias", entry["path"].rsplit(".", 1)[-1])
        entry.setdefault("root", normalized[-1]["alias"] if normalized else RESULT_ROOT)
        entry.setdefault("join", "cross")
        if entry["root"] == "result":
            entry["root"] = RESULT_ROOT
        normalized.append(entry)
    return normalized


def normalize_wide_table(
    unnest_paths: list[str | UnnestSpec],
    columns: list[str | ColumnSpec],
    *,
    owner: str,
) -> tuple[list[NormalizedUnnest], list[NormalizedColumn]]:
    """Resolves a wide table's unnests and columns, and validates the result.

    Raises rather than returning an empty list when ``columns`` is missing: a wide table IS its
    column config, so forgetting it is a mistake, not a legitimately empty table.
    """
    if not columns:
        raise errors.NlpTableConfigError(f"{owner}: a wide table must declare `columns`")

    unnests = normalize_unnests(unnest_paths, owner=owner)
    default_root = unnests[-1]["alias"] if unnests else RESULT_ROOT
    known_roots = {RESULT_ROOT, *(u["alias"] for u in unnests)}

    normalized: list[NormalizedColumn] = []
    for entry in columns:
        entry = _as_dict(entry)
        if "expr" in entry and "path" in entry:
            raise errors.NlpTableConfigError(
                f"{owner}: a column can set `path` or `expr`, not both: {entry!r}"
            )
        if "expr" in entry:
            if "alias" not in entry:
                raise errors.NlpTableConfigError(f"{owner}: expr columns need an explicit alias")
            # A root/transform on a raw expression would be silently ignored, so refuse them
            if "root" in entry or "transform" in entry:
                raise errors.NlpTableConfigError(
                    f"{owner}: column {entry['alias']!r} is an `expr`, which can't also set"
                    " `root` or `transform`"
                )
        elif "path" in entry:
            entry.setdefault("alias", entry["path"].rsplit(".", 1)[-1])
            entry.setdefault("root", default_root)
            if entry["root"] == "result":
                entry["root"] = RESULT_ROOT
            if entry["root"] not in known_roots:
                raise errors.NlpTableConfigError(
                    f"{owner}: column {entry['alias']!r} has root {entry['root']!r},"
                    " which is not nlp.result or an unnest alias"
                )
        else:
            raise errors.NlpTableConfigError(f"{owner}: a column needs a `path` or an `expr`")
        entry.setdefault("sql_type", "varchar")
        normalized.append(entry)

    aliases = METADATA_COLUMNS + [c["alias"] for c in normalized]
    if duplicates := sorted({a for a in aliases if aliases.count(a) > 1}):
        raise errors.NlpTableConfigError(f"{owner}: duplicate output columns {duplicates}")
    return unnests, normalized


def normalize_highlights(
    specs: list[HighlightSpec],
    *,
    owner: str,
) -> list[NormalizedHighlight]:
    """Resolves every highlight spec, deciding here what the template would decide inline.

    Raises rather than returning an empty list when there are no mentions: a highlight table
    IS its mention config, so forgetting it is a mistake, not a legitimately empty table.

    (``require`` is validated by msgspec as the TOML loads. The template matches it with
    if/elif and no else, so an unchecked misspelling would silently emit no filter at all and
    the table would quietly gain rows nobody asked for.)
    """
    if not specs:
        raise errors.NlpTableConfigError(f"{owner}: a highlight table must declare `mentions`")

    normalized: list[NormalizedHighlight] = []
    for spec in specs:
        if not spec.span_path:
            raise errors.NlpTableConfigError(
                f"{owner}: highlight {spec.ls_subvalue_name!r} needs a non-empty `span_path`"
            )

        unnests = normalize_unnests(list(spec.within), owner=owner, auto_alias=True)

        # The parent mention struct is the value's path minus its last segment. For a mention
        # that IS the value (surgery_mentions[*].surgery) there is no dot, so the parent is the
        # mention itself. The template uses this to filter out rows.
        mention_path = spec.path.rsplit(".", 1)[0]

        normalized.append(
            {
                "ls_variable_name": spec.ls_variable_name,
                "ls_subvalue_name": spec.ls_subvalue_name,
                "path": spec.path,
                "span_path": spec.span_path,
                "require": spec.require,
                "sentinel": FILTER_SENTINELS.get(spec.require),
                "within": list(spec.within),
                "unnests": unnests,
                "root": unnests[-1]["alias"] if unnests else RESULT_ROOT,
                "mention_path": mention_path,
                "merge_spans": not isinstance(spec.span_path, str),
            }
        )
    return normalized
