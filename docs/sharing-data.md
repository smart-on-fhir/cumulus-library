---
title: Data Sharing
parent: Library
nav_order: 5
# audience: IT security or clinical researcher with low to medium familiarity with project
# type: explanation
---

# Sharing data with third parties

While it is not required if you are using this framework internally to help you manage
your own data analysis, the Cumulus project is intended to produce anonymized data
which is suitable for sharing with other institutions via federated networks.

## Exporting data

We talk about the mechanics of this a little bit in the 
[First time setup guide](./first-time-setup.md), but a study can be configured with a
subset of tables set as being exportable to disk. By default, we export two file
formats: csv, and parquet. The former is intended for things like SME verification,
while the latter is intended for transport/programmatic consumption/type safety.

Data exported from Cumulus goes into the `cumulus_library/data_export` directory 
of the project, which is ignored by git by default. From here, you can load them 
into your analysis tool of choice. If you're using the pip package you may want to
use the `--export-dir` flag to specify a path for this data if you want to manually
review it, or set the `CUMULUS_LIBRARY_EXPORT_DIR` environment variable so that
all future runs will export to an easy to find directory.

## Uploading data to Cumulus Aggregator

As part of the Cumulus framework, we have a 
[middleware application](https://github.com/smart-on-fhir/cumulus-aggregator/) 
configured to receive and combine datasets from multiple organizations, which can
then be loaded into the [dashboard](https://github.com/smart-on-fhir/cumulus-app) 
for SME analysis. As of this writing these are not open source, but are intended
to be in the near term.

We recommend configuring the following environment variables for using this script:

- `CUMULUS_AGGREGATOR_USER`
- `CUMULUS_AGGREGATOR_ID`

The administrator of your aggregator can help you with generating the values for
these variables; reach out to them for more details.

With these configured, running `cumulus-library upload` will send any exported
data up to the defined aggregator instance. If you are doing something slightly
more complex than participating in one clinical study with the main Cumulus project,
using the `--help` flag will give you some additional configuration options that
may help with your use case.