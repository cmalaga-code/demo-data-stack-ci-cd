from aws_cdk import Stack, aws_s3 as s3, aws_iam as iam
from aws_cdk import aws_s3_notifications as s3n


class NotificationManagerStack(Stack):
    def __init__(self, scope, id, bucket_refs, lambda_fn, **kwargs):
        super().__init__(scope, id, **kwargs)

        prefixes = {
            "stage": ("claims/type=structured/", ".csv"),
            "curated": ("claims/type=structured/", ".parquet"),
            "application": ("claims/model/fact/", ".parquet")
        }

        for label, (prefix, suffix) in prefixes.items():
            bucket = bucket_refs[label]
            bucket.add_event_notification(
                s3.EventType.OBJECT_CREATED,
                s3n.LambdaDestination(lambda_fn),
                s3.NotificationKeyFilter(prefix=prefix, suffix=suffix)
            )

            lambda_fn.add_permission(
                f"allow-invoke-from-s3-{label}",
                principal=iam.ServicePrincipal("s3.amazonaws.com"),
                source_arn=bucket.bucket_arn
            )
