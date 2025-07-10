# import os
# from aws_cdk import (
#     Stack, Tags,
#     RemovalPolicy,
#     aws_s3 as s3,
#     aws_iam as iam,
#     aws_lambda as _lambda,
#     Duration,
#     aws_s3_notifications as s3n
# )
# from constructs import Construct

# class MetaLambdaStack(Stack):
#     def __init__(self, scope: Construct, id: str, orchestration_stack, **kwargs) -> None:
#         super().__init__(scope, id, **kwargs)

#         lambda_role = iam.Role(
#             self, "meta-lambda-exection-role",
#             assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
#             managed_policies=[
#                 iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")
#             ]
#         )

#         lambda_role.add_to_policy(
#             iam.PolicyStatement.from_json({
#                 "Effect": "Allow",
#                 "Action": [
#                     "s3:GetObject",
#                     "states:StartExecution"
#                 ],
#                 "Resource": [
#                     f"arn:aws:s3:::{os.environ['STAGE_BUCKET']}/*",
#                     f"arn:aws:s3:::{os.environ['CURATED_BUCKET']}/*",
#                     f"arn:aws:s3:::{os.environ['APPLICATION_BUCKET']}/*",
#                     orchestration_stack.state_machine.state_machine_arn
#                 ]
#             })
#         )


#         self.meta_lambda = _lambda.DockerImageFunction(
#             self, "meta-lambda-function",
#             code=_lambda.DockerImageCode.from_image_asset("src/_lambda_/process_meta_data"),
#             timeout=Duration.seconds(180),
#             memory_size=2048, # MB -- 2GB
#             description="Triggered by S3 to extract metadata and start Step Function",
#             environment={
#                 "STATE_MACHINE_ARN": orchestration_stack.state_machine.state_machine_arn
#             },
#             role=lambda_role
#         )


# class ImportedBucketStack(Stack):
#     def __init__(self, scope: Construct, id: str, bucket_name: str, **kwargs):
#         super().__init__(scope, id, **kwargs)
#         logical_id = f"imported-bucket-{bucket_name}"
#         self.bucket = s3.Bucket.from_bucket_name(self, logical_id, bucket_name)
#         self.imported = True

#         Tags.of(self).add("Project", "DataLake")


# class S3BucketStack(Stack):
#     def __init__(self, scope: Construct, id: str, bucket_name: str, env_name: str, **kwargs):
#         super().__init__(scope, id, **kwargs)

#         is_dev = str(env_name).upper() == "DEV"

#         self.bucket = s3.Bucket(
#             self, id,
#             bucket_name=bucket_name,  # must be globally unique
#             versioned=True,
#             removal_policy=RemovalPolicy.DESTROY if is_dev else RemovalPolicy.RETAIN,  # for dev/testing
#             auto_delete_objects=is_dev  # only works with DESTROY
#         )
       
#         self.imported = False

#         # Add tags to the stack
#         Tags.of(self).add("Environment", env_name)
#         Tags.of(self).add("Project", "DataLake")
    

import os
from aws_cdk import (
    Stack,
    RemovalPolicy,
    Duration,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_s3 as s3,
    aws_s3_notifications as s3n
)
from constructs import Construct

class MetaLambdaStack(Stack):
    def __init__(self, scope: Construct, id: str, orchestration_stack, env_name: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # Helper to choose removal policy based on environment
        is_dev = str(env_name).upper() == "DEV"

        #  Create all three buckets
        stage_bucket = s3.Bucket(
            self, "StageBucket",
            bucket_name=os.environ["STAGE_BUCKET"],
            versioned=True,
            removal_policy=RemovalPolicy.DESTROY if is_dev else RemovalPolicy.RETAIN,
            auto_delete_objects=is_dev
        )

        curated_bucket = s3.Bucket(
            self, "CuratedBucket",
            bucket_name=os.environ["CURATED_BUCKET"],
            versioned=True,
            removal_policy=RemovalPolicy.DESTROY if is_dev else RemovalPolicy.RETAIN,
            auto_delete_objects=is_dev
        )

        application_bucket = s3.Bucket(
            self, "ApplicationBucket",
            bucket_name=os.environ["APPLICATION_BUCKET"],
            versioned=True,
            removal_policy=RemovalPolicy.DESTROY if is_dev else RemovalPolicy.RETAIN,
            auto_delete_objects=is_dev
        )

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

        # 🔗 Allow S3 to trigger the Lambda
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
