# cribbed from https://towardsdatascience.com/mock-aws-athena-for-your-etl-tests-1f5447261705

import boto3
import botocore
import pandas as pd
import pyathena
from sqlalchemy import create_engine

REGION = "us-east-2"

# 0. Global set up
mock_s3_client = boto3.client(
    "s3", region_name=REGION, endpoint_url="http://localhost:5000"
)
# cursor = pyathena.connect(
#     region_name=REGION,
#     work_group='workgroup',
#     s3_staging_dir="http://localhost:10000",
#     profile_name='test'
# ).cursor()
cursor = create_engine("hive://localhost:10000", echo=True)
# 1. Create mothena bucket
# try:
#    mock_s3_client.create_bucket(
#        Bucket="mothena", CreateBucketConfiguration={"LocationConstraint": REGION}
#    )
# except botocore.exceptions.ClientError:
#    pass

# 2. Upload file
# file = open(
#        "test_data/count_synthea_patient.parquet",
#        "rb",
#    )
# mock_s3_client.put_object(
#        Bucket="mothena",
#        Key="count_synthea_patient/downloaded_at=2020-10-15/properties.parquet",
#        Body=file.read(),
#    )

# 3. Create table
cursor.execute("DROP TABLE IF EXISTS count_synthea_patient")
cursor.execute(
    """
    CREATE EXTERNAL TABLE count_synthea_patient(
      address string, 
      residential_units int, 
      sale_price int, 
      sale_date string)
    PARTITIONED BY ( 
      downloaded_at string)
    STORED AS PARQUET LOCATION 's3://mothena/count_synthea_patient'"""
)

# 4. Update metadata
cursor.execute("MSCK REPAIR TABLE count_synthea_patient")

# 5. Query results
df = pd.read_sql_query("SELECT * FROM count_synthea_patient", cursor)
print(df.head())  # or just df.head() if you're using jupyter
