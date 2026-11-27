This directory contains synthetic note data that can be used with the example
NLP study to test notes. An example of running this:

```bash
rm /tmp/dev.duckdb
rm -rf /tmp/phi

cumulus-library build --target core \
  --db-type duckdb --database /tmp/dev.duckdb \
  --load-ndjson-dir /path/to/this/dir

cumulus-library build --target example_nlp \
  --db-type duckdb --database /tmp/dev.duckdb \
  --note-dir /path/to/this/dir \
  --nlp-model gpt-oss-120b \
  --nlp-provider [provider] \
  --etl-phi-dir /tmp/phi
  --profile [your profile]
```
