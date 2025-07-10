import os
import boto3
from botocore.exceptions import ClientError
from aws_cdk import (
    Stack,
    Duration,
    RemovalPolicy,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_s3 as s3,
    aws_s3_notifications as s3n
)
from constructs import Construct

class MetaLambdaStack(Stack):
    def __init__(
        self,
        scope: Construct,
        id: str,
        orchestration_stack,
        env_name: str,
        **kwargs
    ) -> None:
        super().__init__(scope, id, **kwargs)

        is_dev = str(env_name).upper() == "DEV"

        # Resolve buckets + check if they were imported
        stage_bucket, imported_stage = self.resolve_bucket("StageBucket", os.environ["STAGE_BUCKET"], is_dev)
        curated_bucket, _ = self.resolve_bucket("CuratedBucket", os.environ["CURATED_BUCKET"], is_dev)
        application_bucket, _ = self.resolve_bucket("ApplicationBucket", os.environ["APPLICATION_BUCKET"], is_dev)

        #  Lambda role with access to buckets + Step Function
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

        #  Docker Lambda
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

        # Conditional Lambda trigger permission
        if not imported_stage:
            self.meta_lambda.add_permission(
                "AllowInvokeFromS3",
                principal=iam.ServicePrincipal("s3.amazonaws.com"),
                source_arn=stage_bucket.bucket_arn
            )

            #  Conditional event notification wiring
            stage_bucket.add_event_notification(
                s3.EventType.OBJECT_CREATED,
                s3n.LambdaDestination(self.meta_lambda),
                s3.NotificationKeyFilter(
                    prefix="claims/type=structured/",
                    suffix=".csv"
                )
            )

    def resolve_bucket(self, id: str, name: str, is_dev: bool) -> tuple[s3.IBucket, bool]:
        if self.bucket_exists(name):
            return s3.Bucket.from_bucket_name(self, f"Imported{id}", name), True
        else:
            bucket = s3.Bucket(
                self, id,
                bucket_name=name,
                versioned=True,
                removal_policy=RemovalPolicy.DESTROY if is_dev else RemovalPolicy.RETAIN,
                auto_delete_objects=is_dev
            )
            return bucket, False

    def bucket_exists(self, bucket_name: str) -> bool:
        try:
            boto3.client("s3").head_bucket(Bucket=bucket_name)
            return True
        except ClientError as e:
            return e.response["Error"]["Code"] != "404"
