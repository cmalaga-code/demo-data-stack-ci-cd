import os
import json
from aws_cdk import (
    Stack,
    aws_iam as iam,
    aws_lambda as _lambda,
    Duration
)
from constructs import Construct


class StructuredCurateDataLambdaStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        lambda_role = iam.Role(
            self, "structured-curate-lambda-exection-role",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")
            ]
        )

        lambda_role.add_to_policy(
        iam.PolicyStatement.from_json({
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject"
                ],
                "Resource": [
                    f"arn:aws:s3:::{os.environ['STAGE_BUCKET']}/*"
                ]
            })
        )


        lambda_role.add_to_policy(
            iam.PolicyStatement.from_json({
                "Effect": "Allow",
                "Action": [
                    "s3:PutObject"
                ],
                "Resource": [
                    f"arn:aws:s3:::{os.environ['CURATED_BUCKET']}/*"
                ]
            })
        )


        self.fn = _lambda.DockerImageFunction(
            self, "structured-curate-lambda",
            code=_lambda.DockerImageCode.from_image_asset("src/_lambda_/curate_layer/process_structured_data"),
            timeout=Duration.seconds(180),
            memory_size=4096, # MB -- 4GB
            description="State task for state machine .. process structured data that is not big data",
            role=lambda_role
        )

class StructuredApplicationDataLambdaStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        lambda_role = iam.Role(
            self, "structured-application-lambda-exection-role",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")
            ]
        )

        lambda_role.add_to_policy(
            iam.PolicyStatement.from_json({
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject"
                ],
                "Resource": [
                    f"arn:aws:s3:::{os.environ['CURATED_BUCKET']}/*"
                ]
            })
        )

        lambda_role.add_to_policy(
            iam.PolicyStatement.from_json({
                "Effect": "Allow",
                "Action": [
                    "s3:PutObject"
                ],
                "Resource": [
                    f"arn:aws:s3:::{os.environ['APPLICATION_BUCKET']}/*"
                ]
            })
        )


        self.fn = _lambda.DockerImageFunction(
            self, "structured-application-lambda",
            code=_lambda.DockerImageCode.from_image_asset("src/_lambda_/application_layer/process_structured_data"),
            timeout=Duration.seconds(180),
            memory_size=4096, # MB -- 4GB
            description="State task for state machine .. process structured data that is not big data",
            role=lambda_role
        )

class SemiStructuredCurateDataLambdaStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        lambda_role = iam.Role(
            self, "semi-structured-curate-lambda-exection-role",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")
            ]
        )

        lambda_role.add_to_policy(
            iam.PolicyStatement.from_json({
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject"
                ],
                "Resource": [
                    f"arn:aws:s3:::{os.environ['STAGE_BUCKET']}/*"
                ]
            })
        )

        lambda_role.add_to_policy(
            iam.PolicyStatement.from_json({
                "Effect": "Allow",
                "Action": [
                    "s3:PutObject"
                ],
                "Resource": [
                    f"arn:aws:s3:::{os.environ['CURATED_BUCKET']}/*"
                ]
            })
        )


        self.fn = _lambda.DockerImageFunction(
            self, "semi-structured-curate-lambda",
            code=_lambda.DockerImageCode.from_image_asset("src/_lambda_/curate_layer/process_semi_structured_data"),
            timeout=Duration.seconds(180),
            memory_size=4096, # MB -- 4GB
            description="State task for state machine .. process structured data that is not big data",
            role=lambda_role
        )


class UnStructuredCurateDataLambdaStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        lambda_role = iam.Role(
            self, "unstructured-curate-lambda-exection-role",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")
            ]
        )

        lambda_role.add_to_policy(
            iam.PolicyStatement.from_json({
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject"
                ],
                "Resource": [
                    f"arn:aws:s3:::{os.environ['STAGE_BUCKET']}/*"
                ]
            })
        )

        lambda_role.add_to_policy(
            iam.PolicyStatement.from_json({
                "Effect": "Allow",
                "Action": [
                    "s3:PutObject"
                ],
                "Resource": [
                    f"arn:aws:s3:::{os.environ['CURATED_BUCKET']}/*"
                ]
            })
        )



        self.fn = _lambda.DockerImageFunction(
            self, "unstructured-curate-lambda",
            code=_lambda.DockerImageCode.from_image_asset("src/_lambda_/curate_layer/process_unstructured_data"),
            timeout=Duration.seconds(180),
            memory_size=4096, # MB -- 4GB
            description="State task for state machine .. process unstructured data that is not big data",
            role=lambda_role
        )

class UnStructuredApplicationDataLambdaStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        lambda_role = iam.Role(
            self, "unstructured-application-lambda-exection-role",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")
            ]
        )

        lambda_role.add_to_policy(
            iam.PolicyStatement.from_json({
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject"
                ],
                "Resource": [
                    f"arn:aws:s3:::{os.environ['CURATED_BUCKET']}/*"
                ]
            })
        )


        lambda_role.add_to_policy(
            iam.PolicyStatement.from_json({
                "Effect": "Allow",
                "Action": [
                    "s3:PutObject"
                ],
                "Resource": [
                    f"arn:aws:s3:::{os.environ['APPLICATION_BUCKET']}/*"
                ]
            })
        )


        self.fn = _lambda.DockerImageFunction(
            self, "unstructured-application-lambda",
            code=_lambda.DockerImageCode.from_image_asset("src/_lambda_/application_layer/process_unstructured_data"),
            timeout=Duration.seconds(180),
            memory_size=4096, # MB -- 4GB
            description="State task for state machine .. process unstructured data that is not big data",
            role=lambda_role
        )

class SnowflakeModelLambdaStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        lambda_role = iam.Role(
            self, "snowflake-lambda-exection-role",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")
            ]
        )

        lambda_role.add_to_policy(
            iam.PolicyStatement.from_json({
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject"
                ],
                "Resource": [
                    f"arn:aws:s3:::{os.environ['APPLICATION_BUCKET']}/*"
                ]
            })
        )


        self.fn = _lambda.DockerImageFunction(
            self, "snowflake-lambda-application-lambda",
            code=_lambda.DockerImageCode.from_image_asset("src/_lambda_/ingest_data_model"),
            timeout=Duration.seconds(180),
            memory_size=4096, # MB -- 4GB
            description="State task for state machine .. ingest data to model",
            environment={
                "SNOWFLAKE_ACCOUNT": os.environ["SNOWFLAKE_ACCOUNT"],
                "SNOWFLAKE_USER": os.environ["SNOWFLAKE_USER"],
                "SNOWFLAKE_ROLE": os.environ["SNOWFLAKE_ROLE"],
                "SNOWFLAKE_PIPE": json.dumps({
                    "FACT_CLAIMS": os.environ["SNOWFLAKE_PIPE_FACT_CLAIMS"]
                }),
                "SNOWFLAKE_PRIVATE_KEY": os.environ["SNOWFLAKE_PRIVATE_KEY"],  # base64-encoded
            },
            role=lambda_role
        )

    
