from aws_cdk import (
    Stack,
    aws_lambda as _lambda,
    BundlingOptions,
    Duration
)
from constructs import Construct

class MetaLambdaStack(Stack):
    def __init__(self, scope: Construct, id: str, orchestration_stack, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        self.meta_lambda = _lambda.Function(
            self, id,
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="metadata.handler",
            timeout=Duration.seconds(80),
            memory_size=2048, # MB -- 2GB
            description="Triggered by S3 to extract metadata and start Step Function",
            code=_lambda.Code.from_asset(
                "src/_lambda_/process_meta_data",
                exclude=["*.pyc", "__pycache__", "tests", ".venv", ".DS_Store"],

                bundling=BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_12.bundling_image,
                    command=[
                        "bash", "-c",
                        "pip install -r requirements.txt -t /asset-output && cp -au . /asset-output"
                    ]
                )
            ),
            environment={
                "STATE_MACHINE_ARN": orchestration_stack.state_machine.state_machine_arn
            }
        )

class StructuredCurateDataLambdaStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        self.fn = _lambda.Function(
            self, id,
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="structured.handler",
            timeout=Duration.seconds(80),
            memory_size=4096, # MB -- 4GB
            description="State task for state machine .. process structured data that is not big data",
            code=_lambda.Code.from_asset(
                "src/_lambda_/curate_layer/process_structured_data",
                exclude=["*.pyc", "__pycache__", "tests", ".venv", ".DS_Store"],
                bundling=BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_12.bundling_image,
                    command=[
                        "bash", "-c",
                        "pip install -r requirements.txt -t /asset-output && cp -au . /asset-output"
                    ]
                )
            )
        )

class StructuredApplicationDataLambdaStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        self.fn = _lambda.Function(
            self, id,
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="structured.handler",
            timeout=Duration.seconds(80),
            memory_size=4096, # MB -- 4GB
            description="State task for state machine .. process structured data that is not big data",
            code=_lambda.Code.from_asset(
                "src/_lambda_/application_layer/process_structured_data",
                exclude=["*.pyc", "__pycache__", "tests", ".venv", ".DS_Store"],
                bundling=BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_12.bundling_image,
                    command=[
                        "bash", "-c",
                        "pip install -r requirements.txt -t /asset-output && cp -au . /asset-output"
                    ]
                )
            )
        )

class SemiStructuredCurateDataLambdaStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        self.fn = _lambda.Function(
            self, id,
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="semi_structured.handler",
            timeout=Duration.seconds(80),
            memory_size=4096, # MB -- 4GB
            description="State task for state machine .. process structured data that is not big data",
            code=_lambda.Code.from_asset(
                "src/_lambda_/curate_layer/process_semi_structured_data",
                exclude=["*.pyc", "__pycache__", "tests", ".venv", ".DS_Store"],
                bundling=BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_12.bundling_image,
                    command=[
                        "bash", "-c",
                        "pip install -r requirements.txt -t /asset-output && cp -au . /asset-output"
                    ]
                )
            )
        )


class UnStructuredCurateDataLambdaStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        self.fn = _lambda.Function(
            self, id,
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="unstructured.handler",
            timeout=Duration.seconds(80),
            memory_size=4096, # MB -- 4GB
            description="State task for state machine .. process unstructured data that is not big data",
            code=_lambda.Code.from_asset(
                "src/_lambda_/curate_layer/process_unstructured_data",
                exclude=["*.pyc", "__pycache__", "tests", ".venv", ".DS_Store"],
                bundling=BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_12.bundling_image,
                    command=[
                        "bash", "-c",
                        "pip install -r requirements.txt -t /asset-output && cp -au . /asset-output"
                    ]
                )
            )
        )

class UnStructuredApplicationDataLambdaStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        self.fn = _lambda.Function(
            self, id,
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="unstructured.handler",
            timeout=Duration.seconds(80),
            memory_size=4096, # MB -- 4GB
            description="State task for state machine .. process unstructured data that is not big data",
            code=_lambda.Code.from_asset(
                "src/_lambda_/application_layer/process_unstructured_data",
                exclude=["*.pyc", "__pycache__", "tests", ".venv", ".DS_Store"],
                bundling=BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_12.bundling_image,
                    command=[
                        "bash", "-c",
                        "pip install -r requirements.txt -t /asset-output && cp -au . /asset-output"
                    ]
                )
            )
        )

    
