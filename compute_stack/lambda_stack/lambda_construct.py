from aws_cdk import (
    Stack,
    aws_lambda as _lambda,
    Duration
)
from constructs import Construct

class MetaLambdaStack(Stack):
    def __init__(self, scope: Construct, id: str, orchestration_stack, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        self.meta_lambda = _lambda.DockerImageFunction(
            self, "meta-lambda-function",
            code=_lambda.DockerImageCode.from_image_asset("src/_lambda_/process_meta_data"),
            timeout=Duration.seconds(80),
            memory_size=2048, # MB -- 2GB
            description="Triggered by S3 to extract metadata and start Step Function",
            environment={
                "STATE_MACHINE_ARN": orchestration_stack.state_machine.state_machine_arn
            }
        )

class StructuredCurateDataLambdaStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        self.fn = _lambda.DockerImageFunction(
            self, "structured-curate-lambda",
            code=_lambda.DockerImageCode.from_image_asset("src/_lambda_/curate_layer/process_structured_data"),
            timeout=Duration.seconds(80),
            memory_size=4096, # MB -- 4GB
            description="State task for state machine .. process structured data that is not big data"
        )

class StructuredApplicationDataLambdaStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        self.fn = _lambda.DockerImageFunction(
            self, "structured-application-lambda",
            code=_lambda.DockerImageCode.from_image_asset("src/_lambda_/application_layer/process_structured_data"),
            timeout=Duration.seconds(80),
            memory_size=4096, # MB -- 4GB
            description="State task for state machine .. process structured data that is not big data"
        )

class SemiStructuredCurateDataLambdaStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        self.fn = _lambda.DockerImageFunction(
            self, "semi-structured-curate-lambda",
            code=_lambda.DockerImageCode.from_image_asset("src/_lambda_/curate_layer/process_semi_structured_data"),
            timeout=Duration.seconds(80),
            memory_size=4096, # MB -- 4GB
            description="State task for state machine .. process structured data that is not big data"
        )


class UnStructuredCurateDataLambdaStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        self.fn = _lambda.DockerImageFunction(
            self, "unstructured-curate-lambda",
            code=_lambda.DockerImageCode.from_image_asset("src/_lambda_/curate_layer/process_unstructured_data"),
            timeout=Duration.seconds(80),
            memory_size=4096, # MB -- 4GB
            description="State task for state machine .. process unstructured data that is not big data"
        )

class UnStructuredApplicationDataLambdaStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        self.fn = _lambda.DockerImageFunction(
            self, "unstructured-application-lambda",
            code=_lambda.DockerImageCode.from_image_asset("src/_lambda_/application_layer/process_unstructured_data"),
            timeout=Duration.seconds(80),
            memory_size=4096, # MB -- 4GB
            description="State task for state machine .. process unstructured data that is not big data",
        )

    
