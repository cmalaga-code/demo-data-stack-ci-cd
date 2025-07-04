import boto3
from botocore.exceptions import ClientError
from aws_cdk import (
    Stack, Tags,
    RemovalPolicy,
    aws_s3 as s3,
)
from constructs import Construct


class S3BucketStack(Stack):
    def __init__(self, scope: Construct, id: str, bucket_name: str, env_name, **kwargs):
        super().__init__(scope, id, **kwargs)

        is_dev = str(env_name).upper() == "DEV"

        if not self.bucket_exists(bucket_name):
            s3.Bucket(
                self, id,
                bucket_name=bucket_name,  # must be globally unique
                versioned=True,
                removal_policy=RemovalPolicy.DESTROY if is_dev else RemovalPolicy.RETAIN,  # for dev/testing
                auto_delete_objects=is_dev  # only works with DESTROY
            )
            # Add tags to the stack
            Tags.of(self).add("Environment", env_name)
            Tags.of(self).add("Project", "DataLake")
    
    def bucket_exists(self, bucket_name: str) -> bool:
        s3_client = boto3.client("s3")
        try:
            s3_client.head_bucket(Bucket=bucket_name)
            return True
        except ClientError as e:
            error_code = int(e.response["Error"]["Code"])
            if error_code == 404:
                return False
            raise

            