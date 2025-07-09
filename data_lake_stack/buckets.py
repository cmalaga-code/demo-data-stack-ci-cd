from aws_cdk import (
    Stack, Tags,
    RemovalPolicy,
    aws_s3 as s3,
    aws_s3_notifications as s3n
)
from constructs import Construct

class ImportedBucketStack(Stack):
    def __init__(self, scope: Construct, id: str, bucket_name: str, event_lambda_fn, event_prefix: str, event_suffix: str, **kwargs):
        super().__init__(scope, id, **kwargs)
        logical_id = f"imported-bucket-{bucket_name}"
        self.bucket = s3.Bucket.from_bucket_name(self, logical_id, bucket_name)
        self.imported = True

        if event_lambda_fn and event_prefix is not None and event_suffix is not None:
            self.bucket.add_event_notification(
                s3.EventType.OBJECT_CREATED_PUT,
                s3n.LambdaDestination(event_lambda_fn),
                s3.NotificationKeyFilter(prefix=event_prefix, suffix=event_suffix)
            )

        Tags.of(self).add("Project", "DataLake")


class S3BucketStack(Stack):
    def __init__(self, scope: Construct, id: str, bucket_name: str, env_name: str, event_lambda_fn, event_prefix: str, event_suffix: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        is_dev = str(env_name).upper() == "DEV"

        self.bucket = s3.Bucket(
            self, id,
            bucket_name=bucket_name,  # must be globally unique
            versioned=True,
            removal_policy=RemovalPolicy.DESTROY if is_dev else RemovalPolicy.RETAIN,  # for dev/testing
            auto_delete_objects=is_dev  # only works with DESTROY
        )
        if event_lambda_fn and event_prefix is not None and event_suffix is not None:
            self.bucket.add_event_notification(
                s3.EventType.OBJECT_CREATED_PUT,
                s3n.LambdaDestination(event_lambda_fn),
                s3.NotificationKeyFilter(prefix=event_prefix, suffix=event_suffix)
            )
        self.imported = False

        # Add tags to the stack
        Tags.of(self).add("Environment", env_name)
        Tags.of(self).add("Project", "DataLake")
    
            