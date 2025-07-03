from aws_cdk import (
    Stack,
    aws_lambda as _lambda,
    aws_s3 as s3,
)
from constructs import Construct

class TransformationStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        # Lambda function for transforming data
        transform_lambda = _lambda.Function(
            self, "TransformLambda",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="handler.main",
            code=_lambda.Code.from_asset("lambda/transformation")
        )

        # Example: grant access to a shared bucket (could be passed in from another stack)
        processed_bucket = s3.Bucket.from_bucket_name(
            self, "ProcessedBucket", "my-processed-data-bucket"
        )

        processed_bucket.grant_read_write(transform_lambda)
