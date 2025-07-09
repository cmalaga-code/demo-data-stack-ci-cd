import json
import urllib.parse
import boto3
import os
import logging

s3 = boto3.client('s3')
step_functions = boto3.client('stepfunctions')
logger = logging.getLogger()
logger.setLevel(logging.ERROR)

STEP_FUNCTIONS_STATE_MACHINE_ARN = os.environ.get("STATE_MACHINE_ARN")


def handler(event, context):
    bucket_name = None
    object_key = None
    try:
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        object_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
        prefix = '/'.join(object_key.split("/")[:-1])

        response = s3.head_object(Bucket=bucket_name, Key=object_key)
        content_type = response['ContentType']
        file_size = response['ContentLength']  # File size in bytes

        # Prepare input for state machine
        if "stage" in bucket_name.lower():
            step_functions_input = {
                "bucketName": bucket_name,
                "bucketNameLower": bucket_name.lower(),
                "objectKey": object_key,
                "contentType": content_type,
                "fileSize": file_size,
                "destBucket": os.environ.get("CURATED_BUCKET"),
                "destPrefix": prefix
            }
            step_functions.start_execution(
                stateMachineArn=STEP_FUNCTIONS_STATE_MACHINE_ARN,
                input=json.dumps(step_functions_input)
            )
            return {
                'statusCode': 200,
                'body': json.dumps('Successful step functions execution!')
            }
        elif "curated" in bucket_name.lower():
            step_functions_input = {
                "bucketName": bucket_name,
                "bucketNameLower": bucket_name.lower(),
                "objectKey": object_key,
                "contentType": content_type,
                "fileSize": file_size,
                "destBucket": os.environ.get("APPLICATION_BUCKET"),
                "destPrefix": prefix
            }
            step_functions.start_execution(
                stateMachineArn=STEP_FUNCTIONS_STATE_MACHINE_ARN,
                input=json.dumps(step_functions_input)
            )
            return {
                'statusCode': 200,
                'body': json.dumps('Successful step functions execution!')
            }
        elif "application" in bucket_name.lower():
            step_functions_input = {
                "bucketName": bucket_name,
                "bucketNameLower": bucket_name.lower(),
                "objectKey": object_key,
                "contentType": content_type,
                "fileSize": file_size,
                "destBucket": "N/A",
                "destPrefix": "N/A"
            }
            step_functions.start_execution(
                stateMachineArn=STEP_FUNCTIONS_STATE_MACHINE_ARN,
                input=json.dumps(step_functions_input)
            )
            return {
                'statusCode': 200,
                'body': json.dumps('Successful step functions execution!')
            }

    except Exception as e:
        error_log = {
            "errorMessage": str(e),
            "awsRequestId": getattr(context, "aws_request_id", "N/A"),
            "objectKey": object_key if object_key else "Not available",
            "bucketName": bucket_name if bucket_name else "Not available"
        }
        logger.error(json.dumps(error_log))
        raise e