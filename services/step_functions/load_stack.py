from aws_cdk import (
    Stack,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
)
from constructs import Construct

class StepFunctionsStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        glue_task = tasks.GlueStartJobRun(
            self, "StartGlueJob",
            glue_job_name="MyGlueTransformJob",
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            arguments=sfn.TaskInput.from_object({
                "--run_id": sfn.JsonPath.string_at("$.run_id")
            })
        )

        definition = glue_task

        sfn.StateMachine(
            self, "GlueStateMachine",
            definition=definition,
            timeout=Duration.minutes(30)
        )
