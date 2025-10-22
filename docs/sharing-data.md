---
title: Data Sharing
parent: Library
nav_order: 7
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
subset of tables set as being exportable to disk. By default, we export two kinds of
data:
- An archive containing the exported tables (in parquet format) and a copy of the
manifest used by the study, which we will use when 
[uploading data](#uploading-data-to-cumulus-aggregator).
- CSVs, created from the parquet files, which can be used for human evaluation of the
datasets, or importing into other tools. 

When exporting/uploading data, you need to provide a path to a data directory for
local reading and writing. Optionally, you can specify a path value to use for this
in a `CUMULUS_LIBRARY_DATA_PATH` variable. The library will create subfolders in
this directory based on the names studies you are exporting. These will be emptied
before each export run.

If you are exporting data from Amazon Athena, it is assumed that you will have permissions
to get/put/list/delete files in a folder called export at the root level of the
bucket containing your Athena data, like in the following IAM policy:

```json
{
            "Sid": "ManageExportStagingLocation",
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:ListBucket",
            ],
            "Resource": "arn:aws:s3:::my-athena-bucket-123456789012-us-east-1/export/*"
        }
```

## Uploading data to Cumulus Aggregator

As part of the Cumulus framework, we have a middleware application called
[Cumulus Aggregator](https://docs.smarthealthit.org/cumulus/aggregator/) 
configured to receive and combine datasets from multiple organizations,
which can then be loaded into the 
[Cumulus Dashboard](https://github.com/smart-on-fhir/cumulus-app)
for SME analysis.

We recommend configuring the following environment variables for using this script:

- `CUMULUS_AGGREGATOR_USER`
- `CUMULUS_AGGREGATOR_ID`
- `CUMULUS_AGGREGATOR_NETWORK`

The administrator of your Aggregator instance can help you with generating the values for
these variables; reach out to them for more details.

With these configured, running `cumulus-library upload` will send any exported
data up to the defined Aggregator instance. If you are doing something slightly
more complex than participating in one clinical study with the main Cumulus project,
using the `--help` flag will give you some additional configuration options that
may help with your use case.