# Configuring regression

At BCH, we went through the following steps to configure this regression test:

- Using the 
[ETL Cloudformation Template](https://github.com/smart-on-fhir/cumulus-etl/blob/main/docs/setup/cumulus-aws-template.yaml)
, we created a new Athena stack in AWS
- We used the ETL to load the 
[Synthea 1000 patient sample dataset](https://github.com/smart-on-fhir/sample-bulk-fhir-datasets/tree/1000-patients)
into this AWS environment
- We followed the 
[Configure AWS Credentials](https://github.com/aws-actions/configure-aws-credentials#overview)
instructions to set up AWS permissions.
  - We created a Cognito identity pool for federated tokens
  - We added the Github OIDC service as an identity provider, using that identity pool
  - We created an IAM role, using the identity provider, restricted to our github org/project
  - We restricted role access using the [IAM Template](./regression-iam.json), so that it can
  only see data in the synthetic bucket
  - We configured the 
  [CI regression job](https://github.com/smart-on-fhir/cumulus-library/blob/main/.github/workflows/ci.yaml)
  to call this endpoint and run the regression script

## What this regression covers

This compares a static known good export of counts data to the branch's export of
core counts data. This should be a good stress test of 'did the contents of the core
library change in a meaningful way'.

## What this does not cover

This does not effectively test differences in implementation/nesting between different
EHR vendors - this testing should be performed manually on data from each EHR vendor.
We recommend not using this data for regression as a PHI security measure, even though
that data should be fully anonymized.