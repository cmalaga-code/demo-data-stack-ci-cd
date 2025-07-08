import os
import boto3
from aws_cdk import App, aws_s3 as s3, aws_s3_notifications as s3n
from botocore.exceptions import ClientError

from data_lake_stack.buckets import S3BucketStack
from orchestration_stack.step_function_construct import OrchestrationStack
from compute_stack.lambda_stack.lambda_construct import (
    StructuredCurateDataLambdaStack,
    StructuredApplicationDataLambdaStack,
    SemiStructuredCurateDataLambdaStack,
    UnStructuredCurateDataLambdaStack,
    UnStructuredApplicationDataLambdaStack,
    MetaLambdaStack
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
        error_code = e.response["Error"]["Code"]
        if error_code == "404":
            return False
        raise

if __name__ == "__main__":
    app = App()

    # check if env variables are present

    required_vars = ["STAGE_BUCKET", "CURATED_BUCKET", "APPLICATION_BUCKET", "ENV"]
    missing = [var for var in required_vars if not os.environ.get(var)]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")


    # data lake stack

    if bucket_exists(os.environ.get("STAGE_BUCKET")):
        stage_bucket_stack = s3.Bucket.from_bucket_name(app, "stage-bucket", os.environ.get("STAGE_BUCKET"))
    else:
        stage_bucket_stack = S3BucketStack(app, "stage-bucket", bucket_name=os.environ.get("STAGE_BUCKET"), env_name=os.environ.get("ENV"))

    if bucket_exists(os.environ.get("CURATED_BUCKET")):
        curated_bucket_stack = s3.Bucket.from_bucket_name(app, "curated-bucket", os.environ.get("CURATED_BUCKET"))
    else:
        curated_bucket_stack = S3BucketStack(app, "curated-bucket", bucket_name=os.environ.get("CURATED_BUCKET"), env_name=os.environ.get("ENV"))

    if bucket_exists(os.environ.get("APPLICATION_BUCKET")):
        curated_bucket_stack = s3.Bucket.from_bucket_name(app, "application-bucket", os.environ.get("APPLICATION_BUCKET"))
    else:
        application_bucket_stack = S3BucketStack(app, "application-bucket", bucket_name=os.environ.get("APPLICATION_BUCKET"), env_name=os.environ.get("ENV"))

    # lambda and glue stack

    structured_curated_lambda_stack = StructuredCurateDataLambdaStack(app, "structured-curated-lambda-stack")
    structured_application_lambda_stack = StructuredApplicationDataLambdaStack(app, "structured-application-lambda-stack")
    semi_structured_curated_lambda_stack = SemiStructuredCurateDataLambdaStack(app, "semi-structured-curated-lambda-stack")
    unstructured_curated_lambda_stack = UnStructuredCurateDataLambdaStack(app, "unstructured-curated-lambda-stack")
    unstructured_application_lambda_stack = UnStructuredApplicationDataLambdaStack(app, "unstructured-application-lambda-stack")

    
    structured_curated_glue_stack = StructuredCurateDataGlueStack(app, "structured-curated-glue-stack")
    structured_application_glue_stack = StructuredApplicationDataGlueStack(app, "structured-application-glue-stack")
    semi_structured_curated_glue_stack = SemiStructuredCurateDataGlueStack(app, "semi-structured-curated-glue-stack")
    unstructured_curated_glue_stack = UnStructuredCurateDataGlueStack(app, "unstructured-curated-glue-stack")
    unstructured_application_glue_stack = UnStructuredApplicationDataGlueStack(app, "unstructured-application-glue-stack")

    # event orchestration

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
        unstructured_application_glue_stack.glue_job.name
    )

    meta_lambda_stack = MetaLambdaStack(app, "meta-lambda-stack", orchestration_stack)
    
    # event notification stage -> curated
    stage_bucket_stack.bucket.add_event_notification(
        s3.EventType.OBJECT_CREATED_PUT,
        s3n.LambdaDestination(meta_lambda_stack.meta_lambda),
        s3.NotificationKeyFilter(prefix="claims/type=structured/", suffix=".csv")
    )

    stage_bucket_stack.bucket.add_event_notification(
        s3.EventType.OBJECT_CREATED_PUT,
        s3n.LambdaDestination(meta_lambda_stack.meta_lambda),
        s3.NotificationKeyFilter(prefix="patient/type=structured/", suffix=".csv")
    )

    stage_bucket_stack.bucket.add_event_notification(
        s3.EventType.OBJECT_CREATED_PUT,
        s3n.LambdaDestination(meta_lambda_stack.meta_lambda),
        s3.NotificationKeyFilter(prefix="provider/type=structured/", suffix=".csv")
    )

    stage_bucket_stack.bucket.add_event_notification(
        s3.EventType.OBJECT_CREATED_PUT,
        s3n.LambdaDestination(meta_lambda_stack.meta_lambda),
        s3.NotificationKeyFilter(prefix="lab/type=semi-structured/", suffix=".json")
    )

    stage_bucket_stack.bucket.add_event_notification(
        s3.EventType.OBJECT_CREATED_PUT,
        s3n.LambdaDestination(meta_lambda_stack.meta_lambda),
        s3.NotificationKeyFilter(prefix="lab/type=unstructured/", suffix=".jpeg")
    )

    # event notification curated (clean) -> application (model data)

    curated_bucket_stack.bucket.add_event_notification(
        s3.EventType.OBJECT_CREATED_PUT,
        s3n.LambdaDestination(meta_lambda_stack.meta_lambda),
        s3.NotificationKeyFilter(prefix="claims/type=structured/", suffix=".parquet")
    )

    curated_bucket_stack.bucket.add_event_notification(
        s3.EventType.OBJECT_CREATED_PUT,
        s3n.LambdaDestination(meta_lambda_stack.meta_lambda),
        s3.NotificationKeyFilter(prefix="patient/type=structured/", suffix=".parquet")
    )

    curated_bucket_stack.bucket.add_event_notification(
        s3.EventType.OBJECT_CREATED_PUT,
        s3n.LambdaDestination(meta_lambda_stack.meta_lambda),
        s3.NotificationKeyFilter(prefix="provider/type=structured/", suffix=".parquet")
    )

    curated_bucket_stack.bucket.add_event_notification(
        s3.EventType.OBJECT_CREATED_PUT,
        s3n.LambdaDestination(meta_lambda_stack.meta_lambda),
        s3.NotificationKeyFilter(prefix="lab/type=structured/", suffix=".parquet")
    )

    curated_bucket_stack.bucket.add_event_notification(
        s3.EventType.OBJECT_CREATED_PUT,
        s3n.LambdaDestination(meta_lambda_stack.meta_lambda),
        s3.NotificationKeyFilter(prefix="lab/type=unstructured/", suffix=".jpeg")
    )


    # Synthesize app (executes code and generates CloudFormation Template in JSON format)
    app.synth() # executing code and the apis create a file with the proper CloudFormation template cdk.out

