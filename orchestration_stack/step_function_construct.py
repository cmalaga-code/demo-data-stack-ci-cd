from aws_cdk import Stack
from aws_cdk import (
    aws_stepfunctions as sfn,
    aws_lambda as _lambda_,
    aws_stepfunctions_tasks as tasks,
    aws_logs as logs
)
from aws_cdk.aws_stepfunctions import Choice, Condition
from constructs import Construct 

class OrchestrationStack(Stack):
    def __init__(
            self, scope: Construct, id: str, 
            structured_curated_lambda_fn: _lambda_.IFunction, 
            structured_application_lambda_fn: _lambda_.IFunction,
            semi_structured_curated_lambda_fn: _lambda_.IFunction,
            unstructured_curated_lambda_fn: _lambda_.IFunction,
            unstructured_application_lambda_fn: _lambda_.IFunction,
            structured_curated_glue_name: str,
            structured_application_glue_name: str,
            semi_structured_curated_glue_name: str,
            unstructured_curated_glue_stack_name: str,
            unstructured_application_glue_stack_name: str,
            **kwargs
        ) -> None:
        super().__init__(scope, id, **kwargs)

         # file size threshold 2GB
        SIZE_THRESHOLD = 2 * 1024 * 1024 * 1024

        # sucess defined
        success = sfn.Succeed(self, "Done")

        # lambda tasks defined
        structured_curated_lambda_fn_task = tasks.LambdaInvoke(
            self, "job-structured-curated-lambda-task",
            lambda_function=structured_curated_lambda_fn,
            output_path="$.Payload"
        ).add_catch(
            sfn.Fail(self, "job-structured-curated-lambda-task-failed", error="job-structured-curated-lambda-task-error", cause="failed")
        )

        structured_application_lambda_fn_task = tasks.LambdaInvoke(
            self, "job-structured-application-lambda-task",
            lambda_function=structured_application_lambda_fn,
            output_path="$.Payload"
        ).add_catch(
            sfn.Fail(self, "job-structured-application-lambda-task-failed", error="job-structured-application-lambda-task-error", cause="failed")
        )

        semi_structured_curated_lambda_fn_task = tasks.LambdaInvoke(
            self, "job-semi-structured-curated-lambda-task",
            lambda_function=semi_structured_curated_lambda_fn,
            output_path="$.Payload"
        ).add_catch(
            sfn.Fail(self, "lambda_curated_semi_structured_task_failed", error="lambda_curated_semi_structured_task_error", cause="failed")
        )

        unstructured_curated_lambda_fn_task = tasks.LambdaInvoke(
            self, "job-unstructured-curated-lambda-task",
            lambda_function=unstructured_curated_lambda_fn,
            output_path="$.Payload"
        ).add_catch(
            sfn.Fail(self, "job-semi-structured-curated-lambda-task-failed", error="job-semi-structured-curated-lambda-task-error", cause="failed")
        )

        unstructured_application_lambda_fn_task = tasks.LambdaInvoke(
            self, "job-unstructured-application-lambda-task",
            lambda_function=unstructured_application_lambda_fn,
            output_path="$.Payload"
        ).add_catch(
            sfn.Fail(self, "job-unstructured-application-lambda-task-failed", error="job-unstructured-application-lambda-task-error", cause="failed")
        )
        # define glue task
        structured_curated_glue_task = tasks.GlueStartJobRun(
            self, "job-structured-curated-glue-task",
            glue_job_name=structured_curated_glue_name,
            arguments=sfn.TaskInput.from_object({
                "--JOB_NAME" : structured_curated_glue_name,
                "--SOURCE_BUCKET": sfn.JsonPath.string_at("$.bucketName"),
                "--SOURCE_KEY": sfn.JsonPath.string_at("$.objectKey"),
                "--DEST_BUCKET": sfn.JsonPath.string_at("$.destBucket"),
                "--DEST_PREFIX": sfn.JsonPath.string_at("$.destPrefix")
            }),
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,  # .sync waits for job to complete
            result_path="$.glue_result"
        ).add_catch(
            sfn.Fail(self, "structured-curated-glue-task-failed", error="structured-curated-glue-task-error", cause="failed")
        )

        structured_application_glue_task = tasks.GlueStartJobRun(
            self, "job-structured-application-glue-task",
            glue_job_name=structured_application_glue_name,
            arguments=sfn.TaskInput.from_object({
                "--JOB_NAME" : structured_application_glue_name,
                "--SOURCE_BUCKET": sfn.JsonPath.string_at("$.bucketName"),
                "--SOURCE_KEY": sfn.JsonPath.string_at("$.objectKey"),
                "--DEST_BUCKET": sfn.JsonPath.string_at("$.destBucket"),
                "--DEST_PREFIX": sfn.JsonPath.string_at("$.destPrefix")
            }),
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,  # .sync waits for job to complete
            result_path="$.glue_result"
        ).add_catch(
            sfn.Fail(self, "job-structured-application-glue-task-failed", error="job-structured-application-glue-task-error", cause="failed")
        )

        semi_structured_curated_glue_task = tasks.GlueStartJobRun(
            self, "job-semi-structured-curated-glue-task",
            glue_job_name=semi_structured_curated_glue_name,
            arguments=sfn.TaskInput.from_object({
                "--JOB_NAME" : semi_structured_curated_glue_name,
                "--SOURCE_BUCKET": sfn.JsonPath.string_at("$.bucketName"),
                "--SOURCE_KEY": sfn.JsonPath.string_at("$.objectKey"),
                "--DEST_BUCKET": sfn.JsonPath.string_at("$.destBucket"),
                "--DEST_PREFIX": sfn.JsonPath.string_at("$.destPrefix")
            }),
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,  # .sync waits for job to complete
            result_path="$.glue_result"
        ).add_catch(
            sfn.Fail(self, "job-semi-structured-curated-glue-task-failed", error="job-semi-structured-curated-glue-task-error", cause="failed")
        )

        unstructured_curated_glue_task = tasks.GlueStartJobRun(
            self, "job-unstructured-curated-glue-task",
            glue_job_name=unstructured_curated_glue_stack_name,
            arguments=sfn.TaskInput.from_object({
                "--JOB_NAME" : unstructured_curated_glue_stack_name,
                "--SOURCE_BUCKET": sfn.JsonPath.string_at("$.bucketName"),
                "--SOURCE_KEY": sfn.JsonPath.string_at("$.objectKey"),
                "--DEST_BUCKET": sfn.JsonPath.string_at("$.destBucket"),
                "--DEST_PREFIX": sfn.JsonPath.string_at("$.destPrefix")
            }),
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,  # .sync waits for job to complete
            result_path="$.glue_result"
        ).add_catch(
            sfn.Fail(self, "job-unstructured-curated-glue-task-failed", error="job-unstructured-curated-glue-task-error", cause="failed")
        )

        unstructured_application_glue_task = tasks.GlueStartJobRun(
            self, "job-unstructured-application-glue-task",
            glue_job_name=unstructured_application_glue_stack_name,
            arguments=sfn.TaskInput.from_object({
                "--JOB_NAME" : unstructured_application_glue_stack_name,
                "--SOURCE_BUCKET": sfn.JsonPath.string_at("$.bucketName"),
                "--SOURCE_KEY": sfn.JsonPath.string_at("$.objectKey"),
                "--DEST_BUCKET": sfn.JsonPath.string_at("$.destBucket"),
                "--DEST_PREFIX": sfn.JsonPath.string_at("$.destPrefix")
            }),
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,  # .sync waits for job to complete
            result_path="$.glue_result"
        ).add_catch(
            sfn.Fail(self, "job-unstructured-application-glue-task-failed", error="job-unstructured-application-glue-task-error", cause="failed")
        )
        
        # define choice per size
        size_bucket_structure_choice = Choice(self, "Check File Size, Bucket and Structure")

        size_bucket_structure_choice.when(
            Condition.and_(
                Condition.number_less_than("$.fileSize", SIZE_THRESHOLD),
                Condition.string_matches("$.bucketNameLower", "*stage*"),
                Condition.string_matches("$.objectKey", "*type=structured*")
            ),
            structured_curated_lambda_fn_task.next(success)
        ).when(
            Condition.and_(
                Condition.number_greater_than("$.fileSize", SIZE_THRESHOLD),
                Condition.string_matches("$.bucketNameLower", "*stage*"),
                Condition.string_matches("$.objectKey", "*type=structured*")
            ),
            structured_curated_glue_task.next(success)
        ).when(
            Condition.and_(
                Condition.number_less_than("$.fileSize", SIZE_THRESHOLD),
                Condition.string_matches("$.bucketNameLower", "*curated*"),
                Condition.string_matches("$.objectKey", "*type=structured*")
            ),
            structured_application_lambda_fn_task.next(success)
        ).when(
            Condition.and_(
                Condition.number_greater_than("$.fileSize", SIZE_THRESHOLD),
                Condition.string_matches("$.bucketNameLower", "*curated*"),
                Condition.string_matches("$.objectKey", "*type=structured*")
            ),
            structured_application_glue_task.next(success)
        ).when(
            Condition.and_(
                Condition.number_less_than("$.fileSize", SIZE_THRESHOLD),
                Condition.string_matches("$.bucketNameLower", "*stage*"),
                Condition.string_matches("$.objectKey", "*type=semi-structured*")
            ),
            semi_structured_curated_lambda_fn_task.next(success)
        ).when(
            Condition.and_(
                Condition.number_greater_than("$.fileSize", SIZE_THRESHOLD),
                Condition.string_matches("$.bucketNameLower", "*stage*"),
                Condition.string_matches("$.objectKey", "*type=semi-structured*")
            ),
            semi_structured_curated_glue_task.next(success)
        ).when(
            Condition.and_(
                Condition.number_less_than("$.fileSize", SIZE_THRESHOLD),
                Condition.string_matches("$.bucketNameLower", "*stage*"),
                Condition.string_matches("$.objectKey", "*type=unstructured*")
            ),
            unstructured_curated_lambda_fn_task.next(success)
        ).when(
            Condition.and_(
                Condition.number_greater_than("$.fileSize", SIZE_THRESHOLD),
                Condition.string_matches("$.bucketNameLower", "*stage*"),
                Condition.string_matches("$.objectKey", "*type=unstructured*")
            ),
            unstructured_curated_glue_task.next(success)
        ).when(
            Condition.and_(
                Condition.number_less_than("$.fileSize", SIZE_THRESHOLD),
                Condition.string_matches("$.bucketNameLower", "*curated*"),
                Condition.string_matches("$.objectKey", "*type=unstructured*")
            ),
            unstructured_application_lambda_fn_task.next(success)
        ).when(
            Condition.and_(
                Condition.number_greater_than("$.fileSize", SIZE_THRESHOLD),
                Condition.string_matches("$.bucketNameLower", "*curated*"),
                Condition.string_matches("$.objectKey", "*type=unstructured*")
            ),
            unstructured_application_glue_task.next(success)
        ).otherwise(success)


        self.state_machine = sfn.StateMachine(
            self, id,
            definition=sfn.Chain.start(size_bucket_structure_choice),
            logs=sfn.LogOptions(
                destination=logs.LogGroup(self, "StateMachineLogs"),
                level=sfn.LogLevel.ALL
            ),
            tracing_enabled=True #enable x-ray tracing
        )
