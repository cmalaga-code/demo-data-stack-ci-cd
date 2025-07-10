from aws_cdk import (
    Stack,
    Duration,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_s3 as s3,
    aws_s3_notifications as s3n
)
from constructs import Construct

class MetaLambdaStack(Stack):
    def __init__(self, scope: Construct, id: str, orchestration_stack, env_name: str, stage_bucket, curated_bucket, application_bucket, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)


        # Lambda role with full access to all buckets + Step Function
        lambda_role = iam.Role(
            self, "MetaLambdaExecutionRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")
            ]
        )

        lambda_role.add_to_policy(
            iam.PolicyStatement.from_json({
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "states:StartExecution"
                ],
                "Resource": [
                    f"{stage_bucket.bucket_arn}/*",
                    f"{curated_bucket.bucket_arn}/*",
                    f"{application_bucket.bucket_arn}/*",
                    orchestration_stack.state_machine.state_machine_arn
                ]
            })
        )

        # Create Docker Lambda
        self.meta_lambda = _lambda.DockerImageFunction(
            self, "MetaLambdaFunction",
            code=_lambda.DockerImageCode.from_image_asset("src/_lambda_/process_meta_data"),
            timeout=Duration.seconds(180),
            memory_size=2048,
            description="Triggered by S3 to extract metadata and start Step Function",
            environment={
                "STATE_MACHINE_ARN": orchestration_stack.state_machine.state_machine_arn
            },
            role=lambda_role
        )

        # Allow S3 to trigger the Lambda
        self.meta_lambda.add_permission(
            "AllowInvokeFromS3",
            principal=iam.ServicePrincipal("s3.amazonaws.com"),
            source_arn=stage_bucket.bucket_arn
        )

        # Wire up S3 event notification on stage bucket
        stage_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.LambdaDestination(self.meta_lambda),
            s3.NotificationKeyFilter(
                prefix="claims/type=structured/",
                suffix=".csv"
            )
        )