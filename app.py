import os
import boto3
from aws_cdk import App
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

def check_env_vars(required_vars):
    missing = [var for var in required_vars if not os.environ.get(var)]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

def bucket_exists(bucket_name: str) -> bool:
    s3_client = boto3.client("s3")
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        return True
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "404":
            return False
        raise

# App entry point
app = App()

#  Ensure required env vars are set
check_env_vars(["STAGE_BUCKET", "CURATED_BUCKET", "APPLICATION_BUCKET", "ENV"])
deployment_env = os.environ["ENV"]

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

# Finalized Lambda + buckets + event notification
meta_lambda_stack = MetaLambdaStack(app, "meta-lambda-stack", orchestration_stack, env_name=deployment_env) 

# Synthesize to generate CloudFormation templates
app.synth()
