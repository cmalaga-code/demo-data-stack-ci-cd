import os
import json
import time
import base64
import jwt
import requests

def generate_jwt():
    private_key = base64.b64decode(os.environ["SNOWFLAKE_PRIVATE_KEY"])
    account = os.environ["SNOWFLAKE_ACCOUNT"]
    user = os.environ["SNOWFLAKE_USER"]

    payload = {
        "iss": f"{account}.{user}",
        "sub": f"{account}.{user}",
        "iat": int(time.time()),
        "exp": int(time.time()) + 60
    }

    return jwt.encode(payload, private_key, algorithm="RS256")

def create_post_url(key: str, account: str, snowflake_pipe: dict) -> str:
    if "claims/model/fact/" in key.lower():
        return f"https://{account}.snowflakecomputing.com/v1/data/pipes/{snowflake_pipe['FACT_CLAIMS']}/insertFiles"
    return ""

def lambda_handler(event, context):
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
        snowflake_pipe = json.loads(os.environ["SNOWFLAKE_PIPE"])
        account = os.environ["SNOWFLAKE_ACCOUNT"]

        jwt_token = generate_jwt()
        url = create_post_url(event["objectKey"], account, snowflake_pipe)

        if not url:
            raise ValueError(f"No matching Snowpipe found for key: {event['objectKey']}")

        headers = {
            "Authorization": f"Bearer {jwt_token}",
            "Content-Type": "application/json"
        }

        body = {"files": [{"path": event["objectKey"]}]}
        response = requests.post(url, headers=headers, json=body)

        print(f"Snowpipe response: {response.status_code} - {response.text}")

        if response.status_code != 200:
            raise Exception(f"Snowpipe failed: {response.text}")
        
        return {
            "statusCode": 200,
            "body": f"Successfully triggered Snowpipe for {event['objectKey']}"
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "error": str(e)
        }
