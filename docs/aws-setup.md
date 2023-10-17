---
title: AWS Setup
parent: Library
nav_order: 2
# audience: clinical researcher, low AWS familiarity or engineer w/ some AWS experience
# type: reference
---

# AWS setup

Cumulus library executes queries against an 
[Amazon Athena](https://aws.amazon.com/athena/) datastore. A
[sample database](https://github.com/smart-on-fhir/cumulus-library-sample-database)
for creating such a datastore is available for testing purposes if you don't
already have one.

The cloudformation template in the sample database's Cloudformation template should
have the appropriate permissions set for all the services. If you need to configure
an IAM policy manually, you will need to ensure the AWS profile you are using has
the following permissions:

- Glue access to starting/stopping crawlers
- Glue Get/create database permission for your glue catalog and the database
- Glue CRUD permissions for tables and partitions for the catalog, database, and all tables
- Athena CRUD query access and queing permissions
- S3 CRUD access to your ETL bucket (along with any secrets/kms keys)

A 
[sample IAM policy](https://github.com/smart-on-fhir/cumulus-library/blob/main/docs//sample-iam-policy.json) 
for this use case is available as
a starting point.

## Local AWS configuration

The [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-welcome.html)
has some built in hooks that allow applications to seamlessly connect to AWS services.
If you are going to be using AWS services for more than just Cumulus, we recommend
following the 
[CLI installation guide](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)
as well as the 
[configuration and credentials guide](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html)
so that anything on your system can successfully communicate with AWS.

If you are only using AWS for Cumulus, it may be simpler to configure environment
variables containing your credential information. The relevant ones are:
- `CUMULUS_LIBRARY_PROFILE` : The profile name ('default' is usually the right value,
unless your organization is using advanced credential management.)
- `CUMULUS_LIBRARY_REGION` : The AWS region your bucket is in (e.g., us-east-1)

There are several additional parameters you will need to configure
to specify where your database information lives:
- `CUMULUS_LIBRARY_DATABASE` : The name of the database Athena will use (`cumulus_library_sample_db` if using the sample DB)
- `CUMULUS_LIBRARY_WORKGROUP` : the Athena workgroup to execute queries in (`cumulus_library_sample_db` if using the sample DB)

Configuring environment variables on your system is out of scope of this document, but several guides are available elsewhere. [This guide](https://www.twilio.com/blog/2017/01/how-to-set-environment-variables.html), for example, covers Mac, Windows, and Linux. And, as a plus, it has a picture of an adorable puppy at the top of it.