import os
import boto3
from aws_cdk import App, aws_s3 as s3, RemovalPolicy, Stack
from botocore.exceptions import ClientError

from orchestration_stack.step_function_construct import OrchestrationStack
from data_lake_stack.buckets import MetaLambdaStack
from compute_stack.lambda_stack.lambda_construct import (
    StructuredCurateDataLambdaStack,
    StructuredApplicationDataLambdaStack,
    SemiStructuredCurateDataLambdaStack,
    UnStructuredCurateDataLambdaStack,
    UnStructuredApplicationDataLambdaStack,
    SnowflakeModelLambdaStack
)
from compute_stack.glue_stack.glue_construct import (
    StructuredCurateDataGlueStack,
    StructuredApplicationDataGlueStack,
    SemiStructuredCurateDataGlueStack,
    UnStructuredCurateDataGlueStack,
    UnStructuredApplicationDataGlueStack
)

def bucket_exists(bucket_name: str) -> bool:
    s3_client = boto3.client("s3")
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        return True
    except ClientError as e:
        return e.response["Error"]["Code"] != "404"

def check_env_vars(required_vars):
    missing = [var for var in required_vars if not os.environ.get(var)]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

# App entry point
app = App()

#  Ensure required env vars are set
check_env_vars(["STAGE_BUCKET", "CURATED_BUCKET", "APPLICATION_BUCKET", "ENV"])
deployment_env = os.environ["ENV"]
is_dev = str(deployment_env).upper() == "DEV"

# Lambda stacks
structured_curated_lambda_stack = StructuredCurateDataLambdaStack(app, "structured-curated-lambda-stack")
structured_application_lambda_stack = StructuredApplicationDataLambdaStack(app, "structured-application-lambda-stack")
semi_structured_curated_lambda_stack = SemiStructuredCurateDataLambdaStack(app, "semi-structured-curated-lambda-stack")
unstructured_curated_lambda_stack = UnStructuredCurateDataLambdaStack(app, "unstructured-curated-lambda-stack")
unstructured_application_lambda_stack = UnStructuredApplicationDataLambdaStack(app, "unstructured-application-lambda-stack")
snowflake_model_claims_lambda_stack = SnowflakeModelLambdaStack(app, "snowflake-model-claims-lambda-stack")

# Glue stacks
structured_curated_glue_stack = StructuredCurateDataGlueStack(app, "structured-curated-glue-stack")
structured_application_glue_stack = StructuredApplicationDataGlueStack(app, "structured-application-glue-stack")
semi_structured_curated_glue_stack = SemiStructuredCurateDataGlueStack(app, "semi-structured-curated-glue-stack")
unstructured_curated_glue_stack = UnStructuredCurateDataGlueStack(app, "unstructured-curated-glue-stack")
unstructured_application_glue_stack = UnStructuredApplicationDataGlueStack(app, "unstructured-application-glue-stack")

# Orchestration stack
orchestration_stack = OrchestrationStack(
    app, "data-stack-orchestration",
    structured_curated_lambda_stack.fn,
    structured_application_lambda_stack.fn,
    semi_structured_curated_lambda_stack.fn,
    unstructured_curated_lambda_stack.fn,
    unstructured_application_lambda_stack.fn,
    structured_curated_glue_stack.glue_job.name,
    structured_application_glue_stack.glue_job.name,
    semi_structured_curated_glue_stack.glue_job.name,
    unstructured_curated_glue_stack.glue_job.name,
    unstructured_application_glue_stack.glue_job.name,
    snowflake_model_claims_lambda_stack.fn
)

#  Create all three buckets
bucket_stack = Stack(app, "BucketProvisioningStack")

if bucket_exists(os.environ["STAGE_BUCKET"]):
    stage_bucket = s3.Bucket.from_bucket_name(app, "ImportedStageBucket", os.environ["STAGE_BUCKET"])
else:
    stage_bucket = s3.Bucket(
        bucket_stack, "StageBucket",
        bucket_name=os.environ["STAGE_BUCKET"],
        versioned=True,
        removal_policy=RemovalPolicy.DESTROY if is_dev else RemovalPolicy.RETAIN,
        auto_delete_objects=is_dev
    )
if bucket_exists(os.environ["CURATED_BUCKET"]):
    curated_bucket = s3.Bucket.from_bucket_name(app, "ImportedCuratedBucket", os.environ["CURATED_BUCKET"])
else:
    curated_bucket = s3.Bucket(
        bucket_stack, "CuratedBucket",
        bucket_name=os.environ["CURATED_BUCKET"],
        versioned=True,
        removal_policy=RemovalPolicy.DESTROY if is_dev else RemovalPolicy.RETAIN,
        auto_delete_objects=is_dev
    )

if bucket_exists(os.environ["APPLICATION_BUCKET"]):
   application_bucket = s3.Bucket.from_bucket_name(app, "ImportedApplicationBucket", os.environ["APPLICATION_BUCKET"])
else:
    application_bucket = s3.Bucket(
        bucket_stack, "ApplicationBucket",
        bucket_name=os.environ["APPLICATION_BUCKET"],
        versioned=True,
        removal_policy=RemovalPolicy.DESTROY if is_dev else RemovalPolicy.RETAIN,
        auto_delete_objects=is_dev
    )


meta_lambda_stack = MetaLambdaStack(
    app, "meta-lambda-stack", orchestration_stack, env_name=deployment_env, 
    stage_bucket=stage_bucket, curated_bucket=curated_bucket, application_bucket=application_bucket
) 

# Synthesize to generate CloudFormation templates
app.synth()
