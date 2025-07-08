from aws_cdk import (
    Stack, Tags,
    RemovalPolicy,
    aws_s3 as s3,
    aws_iam as iam
)
from constructs import Construct



class S3BucketStack(Stack):
    def __init__(self, scope: Construct, id: str, bucket_name: str, env_name: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        is_dev = str(env_name).upper() == "DEV"

        bucket = s3.Bucket(
            self, id,
            bucket_name=bucket_name,  # must be globally unique
            versioned=True,
            removal_policy=RemovalPolicy.DESTROY if is_dev else RemovalPolicy.RETAIN,  # for dev/testing
            auto_delete_objects=is_dev  # only works with DESTROY
        )

        content_type_enforce_policy = iam.PolicyStatement(
            effect=iam.Effect.DENY,
            actions=["s3:PutObject"],
            resources=[bucket.arn_for_objects("*")],
            conditions={
                "Null": {
                    "s3:ContentType": "true"
                }
            },
            principals=[iam.AnyPrincipal()]
        )
        bucket.add_to_resource_policy(content_type_enforce_policy)
        # Add tags to the stack
        Tags.of(self).add("Environment", env_name)
        Tags.of(self).add("Project", "DataLake")
    
            