import json
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import logging
import io
import os

s3 = boto3.client('s3')
logger = logging.getLogger()
logger.setLevel(logging.ERROR)

def handler(event, context):
    """
    Expected event:
    {
        "bucketName": "source-bucket",
        "bucketNameLower": "source-bucket",
        "objectKey": "path/to/file.csv",
        "contentType": "text/csv",
        "fileSize": 123456,
        "destBucket": "destination-bucket",
        "destPrefix": "curated/"
    }
    """
    try:
        # Read CSV from S3
        response = s3.get_object(Bucket=event["bucketName"], Key=event["objectKey"])
        content = response["Body"].read().decode("utf-8")

        # Convert CSV to Parquet
        df = pd.read_csv(io.StringIO(content))
        table = pa.Table.from_pandas(df)
        buffer = io.BytesIO()
        pq.write_table(table, buffer)

        # Build destination key
        base_key = event["objectKey"].rsplit(".", 1)[0]  # remove .csv
        dest_key = f"{event['destPrefix']}{base_key}.parquet"

        # Upload Parquet to destination bucket
        s3.put_object(
            Bucket=event["destBucket"],
            Key=dest_key,
            Body=buffer.getvalue(),
            ContentType="application/parquet"
        )

        return {
            'statusCode': 200,
            'body': json.dumps('Successful Step Functions execution! This goes to the next state.')
        }

    except Exception as e:
        error_log = {
            "errorMessage": str(e),
            "awsRequestId": context.aws_request_id,
            "objectKey": event.get("objectKey"),
            "bucketName": event.get("bucketName")
        }
        logger.error(json.dumps(error_log))
        raise e
