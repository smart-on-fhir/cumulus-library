# IBD parity fixtures for the NLP wide/highlight builders

`test_nlp_flatten_ibd_parity.py` checks that the `nlp_wide` and `nlp_highlight` builders, driven by
TOML, render the same SQL that the IBD study's own Python builders used to generate.

- `ibd__llm_treatment_wide.sql` and `ibd__llm_treatment_highlights.sql` are **verbatim copies** of
  the IBD study's checked-in output (`cumulus_library_ibd_cds/llm/athena/`, as of IBD commit
  `6e8703d`), rendered for Athena from a single `gpt_oss_120b` source table.
- `treatment_wide.toml` and `treatment_highlights.toml` are the same two builders
  (`IbdNlpTreatmentFlattenedBuilder`, `IbdNlpTreatmentHighlightsBuilder`) expressed as configs.

Treatment is the sample task because it exercises most of the config vocabulary: a two-level
`UNNEST` with a `LEFT` join, merged multi-array `span_path`s, and the `nota` and `not_null`
filters.

Don't reformat the `.sql` files: they are fixtures, not SQL we maintain (hence the
`.sqlfluffignore` entry). If the IBD builders change, re-copy the new output and update the
commit above.
