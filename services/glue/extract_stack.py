from aws_cdk import (
    Stack,
    aws_glue as glue,
    aws_iam as iam,
)
from constructs import Construct

class GlueStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        glue_role = iam.Role(
            self, "GlueJobRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole")
            ]
        )

        glue_job = glue.CfnJob(
            self, "MyGlueJob",
            name="MyGlueTransformJob",
            role=glue_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location="s3://my-script-bucket/glue_jobs/transform_script.py"
            ),
            glue_version="3.0",
            max_capacity=2.0
        )
