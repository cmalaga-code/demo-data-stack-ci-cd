import os
from aws_cdk import (
    Stack,
    aws_iam as iam,
    aws_s3_assets as s3_assets,
    aws_glue as glue
)
from constructs import Construct

class StructuredCurateDataGlueStack(Stack):
    def __init__(self, scope: Construct, id: str, glue_role, **kwargs):
        super().__init__(scope, id, **kwargs)

        # Upload the PySpark script to an S3 asset
        script_asset = s3_assets.Asset(self, "StructuredCurateDataGlueAsset",
            path="src/glue/curate_layer/process_structured_data/structured.py"
        )

        # IAM Role for Glue
        glue_role = iam.Role(
            self, "structured_curated_glue_role",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole")
            ]
        )

        glue_role.add_to_policy(iam.PolicyStatement(
            actions=[
                "s3:GetObject",
                "s3:PutObject",
                "s3:ListBucket"
            ],
            resources=[
                f"arn:aws:s3:::{os.environ.get('STAGE_BUCKET')}/*",
                f"arn:aws:s3:::{os.environ.get('CURATED_BUCKET')}/*"
            ]
        ))


        # Define the Glue job
        self.glue_job = glue.CfnJob(self, id,
            name="pyspark_structured_curate_data_glue_job",
            role=glue_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=script_asset.s3_object_url
            ),
            glue_version="4.0",  # or "3.0"
            max_retries=0,
            number_of_workers=2,
            worker_type="G.1X",  # or "G.2X"
            description="Pyspark Glue Job"
        )

class StructuredApplicationDataGlueStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        # Upload the PySpark script to an S3 asset
        script_asset = s3_assets.Asset(self, "StructuredApplicationDataGlueAsset",
            path="src/glue/application_layer/process_structured_data/structured.py"
        )

        # IAM Role for Glue
        glue_role = iam.Role(
            self, "structured_application_glue_role",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole")
            ]
        )

        glue_role.add_to_policy(iam.PolicyStatement(
            actions=[
                "s3:GetObject",
                "s3:PutObject",
                "s3:ListBucket"
            ],
            resources=[
                f"arn:aws:s3:::{os.environ.get('CURATED_BUCKET')}/*",
                f"arn:aws:s3:::{os.environ.get('APPLICATION_BUCKET')}/*"
            ]
        ))


        # Define the Glue job
        self.glue_job = glue.CfnJob(self, id,
            name="pyspark_structured_application_data_glue_job",
            role=glue_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=script_asset.s3_object_url
            ),
            glue_version="4.0",  # or "3.0"
            max_retries=0,
            number_of_workers=2,
            worker_type="G.1X",  # or "G.2X"
            description="A sample Glue job using PySpark"
        )

class SemiStructuredCurateDataGlueStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        # Upload the PySpark script to an S3 asset
        script_asset = s3_assets.Asset(self, "SemiStructuredCurateDataGlueAsset",
            path="src/glue/curate_layer/process_semi_structured_data/semi_structured.py"
        )

        # IAM Role for Glue
        glue_role = iam.Role(
            self, "semi_structured_curated_glue_role",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole")
            ]
        )

        glue_role.add_to_policy(iam.PolicyStatement(
            actions=[
                "s3:GetObject",
                "s3:PutObject",
                "s3:ListBucket"
            ],
            resources=[
                f"arn:aws:s3:::{os.environ.get('STAGE_BUCKET')}/*",
                f"arn:aws:s3:::{os.environ.get('CURATED_BUCKET')}/*"
            ]
        ))

        # Define the Glue job
        self.glue_job = glue.CfnJob(self, id,
            name="pyspark_semi_structured_curate_data_glue_job",
            role=glue_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=script_asset.s3_object_url
            ),
            glue_version="4.0",  # or "3.0"
            max_retries=0,
            number_of_workers=2,
            worker_type="G.1X",  # or "G.2X"
            description="A sample Glue job using PySpark"
        )


class UnStructuredCurateDataGlueStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        # Upload the PySpark script to an S3 asset
        script_asset = s3_assets.Asset(self, "UnStructuredCurateDataGlueAsset",
            path="src/glue/curate_layer/process_unstructured_data/unstructured.py"
        )

        # IAM Role for Glue
        glue_role = iam.Role(
            self, "unstructured_curated_glue_role",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole")
            ]
        )

        glue_role.add_to_policy(iam.PolicyStatement(
            actions=[
                "s3:GetObject",
                "s3:PutObject",
                "s3:ListBucket"
            ],
            resources=[
                f"arn:aws:s3:::{os.environ.get('STAGE_BUCKET')}/*",
                f"arn:aws:s3:::{os.environ.get('CURATED_BUCKET')}/*"
            ]
        ))

        # Define the Glue job
        self.glue_job = glue.CfnJob(self, id,
            name="pyspark_unstructured_curate_data_glue_job",
            role=glue_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=script_asset.s3_object_url
            ),
            glue_version="4.0",  # or "3.0"
            max_retries=0,
            number_of_workers=2,
            worker_type="G.1X",  # or "G.2X"
            description="A sample Glue job using PySpark"
        )

class UnStructuredApplicationDataGlueStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        # Upload the PySpark script to an S3 asset
        script_asset = s3_assets.Asset(self, "UnStructuredApplicationDataGlueAsset",
            path="src/glue/application_layer/process_unstructured_data/unstructured.py"
        )

        # IAM Role for Glue
        glue_role = iam.Role(
            self, "unstructured_application_glue_role",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole")
            ]
        )

        glue_role.add_to_policy(iam.PolicyStatement(
            actions=[
                "s3:GetObject",
                "s3:PutObject",
                "s3:ListBucket"
            ],
            resources=[
                f"arn:aws:s3:::{os.environ.get('CURATED_BUCKET')}/*",
                f"arn:aws:s3:::{os.environ.get('APPLICATION_BUCKET')}/*"
            ]
        ))

        # Define the Glue job
        self.glue_job = glue.CfnJob(self, id,
            name="pyspark_unstructured_application_data_glue_job",
            role=glue_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=script_asset.s3_object_url
            ),
            glue_version="4.0",  # or "3.0"
            max_retries=0,
            number_of_workers=2,
            worker_type="G.1X",  # or "G.2X"
            description="A sample Glue job using PySpark"
        )