# SQL builders

This folder contains the builders that Cumulus Library uses to turn a study's
configuration into SQL. Every builder here extends `BaseTableBuilder`.

A study reaches a builder in one of two ways:

- **Python API**: the study imports a class in its own Python code and
  subclasses it or calls it. These classes are exported from the top-level
  `cumulus_library` package (or from a documented subpackage), and their
  signatures are part of the library's public API.
- **Workflows**: the study lists a TOML file in its manifest, and the
  `config_type` in that file tells the library which builder to run. For these,
  the TOML schema is the public interface. The builder classes are internal,
  and studies shouldn't import them.

Some builders are used only by the library itself. Those are listed at the end for 
maintainer convenience and documentation.

If you add a builder, add an entry for it here.

## Python API

These are importable directly from `cumulus_library`. See the
[API reference](../../docs/api.md) for details and examples.

### BaseTableBuilder (base_table_builder.py)

All builders inherit from this class. It handles the prepare/execute hooks that
Cumulus Library uses to run queries against your database. If you're writing a
custom builder, base it on this class and implement `prepare_queries()`.

### CountsBuilder (counts_builder.py)

Provides conveniences for generating aggregate tables that summarize data at the
FHIR resource level. These are the basic output units of Cumulus Library.

As long as your table has the appropriate resource refs (always the ref of the
resource in question, and sometimes also subject or encounter refs), you only
need to specify the columns you want and any join clauses, and it builds the
counts query for you. `CountAnnotation` (also exported from `cumulus_library`)
lets you join extra annotation columns onto a count.

`counts.py` is a symlink to `counts_builder.py`, kept for older import paths.

The same builder also runs the `counts` workflow (see below), for studies that
would rather configure counts in TOML than write Python.

### NLP response-schema primitives (nlp/)

`cumulus_library.builders.nlp` exports the building blocks that NLP studies use
to define the shape of an LLM response, such as `SpanAugmentedMention` and
`DatePrecision`. Import them from the package, not from its submodules. See the
[NLP tables documentation](../../docs/workflows/nlp-tables.md) for how these
connect to the `nlp_wide` and `nlp_highlight` workflows.

## Workflows

Each of these is selected by the `config_type` value in a study's TOML file.
The linked pages document each config format in full.

| `config_type` | Builder | Docs |
|---|---|---|
| `counts` | `CountsBuilder` (counts_builder.py) | [Counts](../../docs/workflows/counts.md) |
| `file_upload` | `FileUploadBuilder` (file_upload_builder.py) | [File Upload](../../docs/workflows/file-upload.md) |
| `iqr` | `IqrBuilder` (iqr_builder.py) | [Interquartile Range](../../docs/workflows/iqr.md) |
| `nlp` | `NlpBuilder` (nlp_builder.py) | [NLP](../../docs/workflows/nlp.md) |
| `nlp_wide` | `NlpWideBuilder` (nlp_wide_builder.py) | [NLP Tables](../../docs/workflows/nlp-tables.md) |
| `nlp_highlight` | `NlpHighlightBuilder` (nlp_highlight_builder.py) | [NLP Tables](../../docs/workflows/nlp-tables.md) |
| `psm` | `PsmBuilder` (psm_builder.py) | [Propensity Score Matching](../../docs/workflows/propensity-score-matching.md) |
| `valueset` | `ValuesetBuilder` (valueset_builder.py) | [Valuesets](../../docs/workflows/valueset.md) |

### Counts (`counts`) workflow

Configures `CountsBuilder` from TOML instead of Python. 

### File upload (`file_upload`) workflow

Loads local data files (from Nov 26: `.csv`, `.bsv`, `.tsv`, `.xlsx` or `.parquet`) into
database tables, converting them to parquet. Use it for code
lists, code system definitions, or results from an external analysis such as a
chart review. 

### Interquartile range (`iqr`) workflow

Computes quartiles and IQR-based outlier bounds for one or more numeric
columns, giving trimmed estimates for the dashboard to render as box plots.
The TOML config schema is defined in `iqr_utils.py`.

### NLP (`nlp`) workflow

Runs clinical notes through an LLM with a study-defined prompt and response
schema, and writes the parsed results to a table for the rest of the study to
query. The response schemas, model dispatch, caching and run tracking live in
the `nlp/` package.

### NLP table (`nlp_wide`, `nlp_highlight`) workflows

Flatten the nested `result` column that an `nlp` workflow leaves behind.
`nlp_wide` produces one row per note (or mention) with a column per field you
pick, and `nlp_highlight` produces one row per supporting text span, for
Label Studio. Both are configured entirely in TOML.

### Propensity score matching (`psm`) workflow

Builds a matched cohort around a variable of interest. The matching itself uses
`psmpy_lite.py`, an adapted copy of [psmpy](https://github.com/adriennekline/psmpy)
without its graphing code.

This is a **statistics workflow**. Because matching samples at random, its
output is persisted in timestamped tables that survive `build` and `clean`.
It is rerun only when you pass `--statistics`. See the
[workflows overview](../../docs/workflows.md) for details.

### Valueset (`valueset`) workflow

Builds a compound clinical valueset (by default, drugs containing a given set of
ingredients). It starts from VSAC valuesets, UMLS keyword searches, or
hand-curated code lists, then expands them by walking UMLS relationships. This
requires a UMLS API key. The implementation and its SQL templates live in
`valueset/`.

## Internal builders

These are used by the library itself. Studies shouldn't use them directly.

### ProtectedTableBuilder (protected_table_builder.py)

Creates the per-study system tables (`lib_transactions`, `lib_statistics`,
`lib_build_source` and `lib_ref_summary`) that persist across `build` and
`clean`. The CLI runs it automatically before a study builds.

### BaseFlattenBuilder (base_flatten_builder.py)

The shared base class for `NlpWideBuilder` and `NlpHighlightBuilder`. It loads
the `[tables.<name>]` TOML config and handles everything the two flattening
builders have in common.

## Supporting directories

- `statistics_templates/`: Jinja templates and Python template helpers for the
  counts, IQR and PSM builders. `CountAnnotation` is defined here.
- `reference_sql/`: an example of the SQL that PSM generates, for reference
  only. Nothing runs it.
