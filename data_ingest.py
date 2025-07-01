import os
import pandas as pd
import boto3
import pymysql
from sqlalchemy import create_engine
from botocore.exceptions import ClientError

# ENV Variables
s3_bucket = os.getenv('S3_BUCKET')
csv_key = os.getenv('CSV_KEY')
rds_host = os.getenv('RDS_HOST')
rds_user = os.getenv('RDS_USER')
rds_pass = os.getenv('RDS_PASS')
rds_db = os.getenv('RDS_DB')
rds_table = os.getenv('RDS_TABLE')
glue_db = os.getenv('GLUE_DB')
glue_table = os.getenv('GLUE_TABLE')
glue_s3_location = os.getenv('GLUE_S3_LOCATION')

def read_csv_from_s3():
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=s3_bucket, Key=csv_key)
    return pd.read_csv(obj['Body'])

def upload_to_rds(df):
    try:
        engine = create_engine(f'mysql+pymysql://{rds_user}:{rds_pass}@{rds_host}/{rds_db}')
        df.to_sql(rds_table, con=engine, index=False, if_exists='replace')
        print("✅ Upload to RDS successful")
        return True
    except Exception as e:
        print(f"❌ Upload to RDS failed: {e}")
        return False

def fallback_to_glue():
    glue = boto3.client('glue')
    try:
        glue.create_database(DatabaseInput={'Name': glue_db})
    except glue.exceptions.AlreadyExistsException:
        pass

    try:
        glue.create_table(
            DatabaseName=glue_db,
            TableInput={
                'Name': glue_table,
                'StorageDescriptor': {
                    'Columns': [{'Name': 'col'+str(i), 'Type': 'string'} for i in range(1, 21)],
                    'Location': glue_s3_location,
                    'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                    'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                    'SerdeInfo': {
                        'SerializationLibrary': 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
                        'Parameters': {'field.delim': ','}
                    }
                },
                'TableType': 'EXTERNAL_TABLE'
            }
        )
        print("✅ Fallback: Glue table registered")
    except glue.exceptions.AlreadyExistsException:
        print("ℹ️ Glue table already exists")

def main():
    df = read_csv_from_s3()
    if not upload_to_rds(df):
        fallback_to_glue()

if __name__ == "__main__":
    main()
