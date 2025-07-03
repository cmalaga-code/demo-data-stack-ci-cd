from aws_cdk import (
    Stack, Tags,
    aws_s3 as s3,
)
from constructs import Construct

class S3BucketStack(Stack):
    def __init__(self, scope: Construct, id: str, bucket_name: str, env_name, **kwargs):
        super().__init__(scope, id, **kwargs)

        is_dev = str(env_name).upper() == "DEV"

        s3.Bucket(
            self, id,
            bucket_name=bucket_name,  # must be globally unique
            versioned=True,
            removal_policy=s3.RemovalPolicy.DESTROY if is_dev else s3.RemovalPolicy.RETAIN,  # for dev/testing
            auto_delete_objects=is_dev  # only works with DESTROY
        )
        # Add tags to the stack
        Tags.of(self).add("Environment", env_name)
        Tags.of(self).add("Project", "DataLake")

