---
title: NLP Tables
parent: Workflows
grand_parent: Library
nav_order: 6
# audience: study authors using NLP
# type: reference
---

# NLP Tables

An [NLP workflow](nlp.md) leaves the LLM's answer in a nested `result` column. That's faithful to
what the model returned, but awkward to query. These two workflows turn it into something easier
to work with, and you configure them with a TOML file rather than writing SQL:

| `config_type`   | Table you get                    | One row per                  | Used for                  |
|-----------------|----------------------------------|------------------------------|---------------------------|
| `nlp_wide`      | `<study>__llm_<name>_wide`       | note (or mention in a note)  | Joining NLP into study SQL |
| `nlp_highlight` | `<study>__llm_<name>_highlights` | supporting text span         | Uploading to Label Studio  |

Both read the `<study>__nlp_<task>_<model>` tables that an NLP workflow wrote. Run the NLP stage
first, then list these configs in a later stage of your manifest.

## Where the source tables come from

For a table named `thing`, the builder uses every table named `<study>__nlp_thing_*` that has a
`result` column, so results from several models are combined (one `SELECT` per model, `UNION ALL`ed,
with the source table recorded in an `origin` column).

- If the task has a different name than the table you want, set `source_task`. This is also how you
  make several tables from one task (say, two independent lists in the same result).
- To use only some models, set `source_models` (e.g. `["gpt_oss_120b"]`).
- If a study has tasks `lab` and `lab_panel`, `lab` does not pick up `lab_panel`'s tables.
- If no source tables exist yet, an empty table with the right columns is still created, so that
  downstream SQL doesn't break.

Settings available on every table:

```toml
[tables.thing]
source_task = "other_task"    # default: the table's name
source_models = ["gpt_oss_120b"]   # default: every model found
task_versions = [7]           # only use rows from these task versions (default: all)
```

Use `task_versions` to avoid mixing results produced by different versions of a prompt/schema.

## Wide tables

```toml
config_type = "nlp_wide"

[tables.treatment]
task_versions = [7]
# Collections to UNNEST before selecting, in order. Each hangs off the previous one.
unnest_paths = [
    { path = "rx_annotations", alias = "med" },
    { path = "rx_adverse_drug_events", alias = "ade", root = "med", join = "left" },
]
columns = [
    { path = "rx_class.rx_class", root = "med" },
    { path = "rx_start_date.rx_start_date", root = "med" },
    { path = "ade", root = "ade" },
]
```

Every wide table starts with the same seven columns: `note_ref`, `encounter_ref`, `subject_ref`,
`origin`, `generated_on`, `task_version`, `system_fingerprint`. Your `columns` follow them.

**`unnest_paths`** — each entry is a path string (shorthand for `{ path = "..." }`) or a table with:

- `path`: the list to unnest.
- `alias`: the name later entries and columns use (default: the last segment of `path`).
- `root`: what the path hangs off: `result`, or an earlier alias (default: the previous entry).
- `join`: `cross` (default) drops a note whose list is empty; `left` keeps one row with `NULL`s.

**`columns`** — each entry is a path string (shorthand for `{ path = "..." }`) or a table with:

- `path`: where the value is, under `root`. The column is named after the last segment unless you
  give an `alias`.
- `root`: `result`, or an `unnest_paths` alias. Defaults to the last unnest, or `result` if there
  are none.
- `transform`: a single-argument SQL function to wrap the value in, like `UPPER`.
- `sql_type`: the column's type if the table has to be created empty (default `varchar`).

For a value that no single path can express, use raw SQL instead of `path`:

```toml
columns = [{ expr = "nlp.result.a.flag OR nlp.result.b.flag", alias = "any_flag", sql_type = "boolean" }]
```

An `expr` always needs an `alias`, can't be combined with `root` or `transform`, and must be valid
in every database you build against (Athena and DuckDB). The source table is always called `nlp`.

## Highlight tables

```toml
config_type = "nlp_highlight"

[tables.treatment]
task_versions = [7]

[[tables.treatment.mentions]]
ls_variable_name = "Medication"
ls_subvalue_name = "Medication Class"
path = "rx_class.rx_class"
span_path = ["rx_class.spans", "rx_status.spans"]
within = ["rx_annotations"]
require = "nota"
```

The table has the columns `note_ref`, `subject_ref`, `generated_on`, `task_version`,
`system_fingerprint`, `origin`, `label`, `span`, `sublabel_name`, `sublabel_value`, where `span` is
the `start:end` character offsets of the text.

Each `[[mentions]]` entry becomes one `SELECT`:

- `ls_variable_name` / `ls_subvalue_name`: the Label Studio variable and sub-value, emitted as
  `label` and `sublabel_name`.
- `path`: the value to emit as `sublabel_value`. Everything before its last segment is the parent
  "mention"; rows are dropped when the mention is missing.
- `span_path`: where the spans are. A single string gives one row per span. A list merges several
  span arrays into one span (from the earliest start to the latest end), giving one row per mention.
  Use the list form when one thing in Label Studio is described by several sibling concepts whose
  text is scattered through the note (like a medication's class, status and response).
- `within`: lists to unnest before reaching the mention. Omit it when the mention is directly
  under `result`.
- `require`: when to keep the row. `not_null` (value is present), `not_mentioned` (value is not
  `NOT_MENTIONED`), or `nota` (value is not `NONE_OF_THE_ABOVE`). Omit for no filter.

## Writing response schemas that work with these builders

The builders rely on a few conventions that `cumulus_library.builders.nlp.pydantic_primitives` provides.
**Subclass these rather than copying them** so that studies stay compatible:

```python
from pydantic import Field

from cumulus_library.builders.nlp import pydantic_primitives


class RxStartDateMention(pydantic_primitives.SpanAugmentedMention):
    rx_start_date: str | None = Field(None, description="ISO YYYY-MM-DD date of the first dose")
    rx_start_date_precision: pydantic_primitives.DatePrecision | None = Field(
        None, description="..."
    )
```

- `SpanAugmentedMention` adds `has_mention` and `spans`. The NLP driver converts any field named
  `spans` from the LLM's verbatim quotes into character offsets, and highlight tables read those
  offsets. Don't rename it.
- End choice enums with `NONE_OF_THE_ABOVE` (and use `NOT_MENTIONED` where absence needs its own
  value). The highlight `require` filters compare against those exact spellings.
- `DatePrecision` pairs with a full ISO date field, so consumers can tell `2021-01-01` from "2021".

Your response schema is what gets sent to the model, so **changing the primitives changes the schema
of every task that uses them**. The library treats edits to them as schema changes; when you upgrade
and a primitive has changed, bump the `version` of each NLP task that uses it.

You only need the primitives to *write* your schema. The tables here work on the JSON schema file
your NLP workflow already points at, so a study that doesn't use the primitives can still use these
builders.
