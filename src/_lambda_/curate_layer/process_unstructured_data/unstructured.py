import boto3
import json
import logging
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
        "objectKey": "path/to/image.jpg",
        "contentType": "image/jpeg",
        "fileSize": 123456,
        "destBucket": "destination-bucket",
        "destPrefix": "curated/"
    }
    """
    try:
        # Read the object from the source bucket
        response = s3.get_object(Bucket=event["bucketName"], Key=event["objectKey"])
        content = response["Body"].read()  # ✅ Read as bytes, not text

        # Write it to the destination bucket
        s3.put_object(
            Bucket=event["destBucket"],
            Key=f"{event['destPrefix']}{event['objectKey']}",
            Body=content,
            ContentType=event.get("contentType", "binary/octet-stream")
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
