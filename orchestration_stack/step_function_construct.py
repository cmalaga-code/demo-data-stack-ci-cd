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
            snowflake_model_claims_lambda_fn: _lambda_.IFunction,
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
        )

        structured_application_lambda_fn_task = tasks.LambdaInvoke(
            self, "job-structured-application-lambda-task",
            lambda_function=structured_application_lambda_fn,
            output_path="$.Payload"
        )

        semi_structured_curated_lambda_fn_task = tasks.LambdaInvoke(
            self, "job-semi-structured-curated-lambda-task",
            lambda_function=semi_structured_curated_lambda_fn,
            output_path="$.Payload"
        )

        unstructured_curated_lambda_fn_task = tasks.LambdaInvoke(
            self, "job-unstructured-curated-lambda-task",
            lambda_function=unstructured_curated_lambda_fn,
            output_path="$.Payload"
        )

        unstructured_application_lambda_fn_task = tasks.LambdaInvoke(
            self, "job-unstructured-application-lambda-task",
            lambda_function=unstructured_application_lambda_fn,
            output_path="$.Payload"
        )
        snowflake_model_claims_fact_fn_task = tasks.LambdaInvoke(
            self, "job-snowflake-model-claims-fact-fn-task",
            lambda_function=snowflake_model_claims_lambda_fn,
            output_path="$.Payload"
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
        )
        # define choice per size
        file_size_choice = Choice(self, "Check File Size")

        bucket_choice = Choice(self, "Check Bucket")
        bucket_big_data_choice = Choice(self, "Check Bucket Big Data")

        data_format_stage = Choice(self, "Data Format Stage")
        data_format_stage_big_data = Choice(self, "Data Format Stage Big Data")

        data_format_curated = Choice(self, "Data Format Curated")
        data_format_curated_big_data = Choice(self, "Data Format Curated Big Data")

    
        check_task_success = Choice(self, "Check Task Success")

        success = sfn.Succeed(self, "Success")
        failure = sfn.Fail(self, "Failure", error="JobFailed", cause="Downstream task failed")

        file_size_choice.when(
            Condition.number_less_than("$.fileSize", SIZE_THRESHOLD),
            bucket_choice
        ).otherwise(
            bucket_big_data_choice
        )

        bucket_choice.when(
            Condition.string_matches("$.bucketNameLower", "*stage*"),
            data_format_stage
        ).when(
            Condition.string_matches("$.bucketNameLower", "*curated*"),
            data_format_curated
        ).when(
            Condition.string_matches("$.bucketNameLower", "*application*"),
            snowflake_model_claims_fact_fn_task.next(success)
        ).otherwise(failure)

        bucket_big_data_choice.when(
            Condition.string_matches("$.bucketNameLower", "*stage*"),
            data_format_stage_big_data
        ).when(
            Condition.string_matches("$.bucketNameLower", "*curated*"),
            data_format_curated_big_data
        ).when(
            Condition.string_matches("$.bucketNameLower", "*application*"),
            snowflake_model_claims_fact_fn_task.next(success)
        ).otherwise(failure)

        data_format_stage.when(
            Condition.string_matches("$.objectKey", "*type=structured/*"),
            structured_curated_lambda_fn_task.next(check_task_success)
        ).when(
            Condition.string_matches("$.objectKey", "*type=semi-structured/*"),
            semi_structured_curated_lambda_fn_task.next(check_task_success)
        ).when(
            Condition.string_matches("$.objectKey", "*type=unstructured/*"),
            unstructured_curated_lambda_fn_task.next(check_task_success)
        ).otherwise(failure)

        data_format_stage_big_data.when(
            Condition.string_matches("$.objectKey", "*type=structured/*"),
            structured_curated_glue_task.next(check_task_success)
        ).when(
            Condition.string_matches("$.objectKey", "*type=semi-structured/*"),
            semi_structured_curated_glue_task.next(check_task_success)
        ).when(
            Condition.string_matches("$.objectKey", "*type=unstructured/*"),
            unstructured_curated_glue_task.next(check_task_success)
        ).otherwise(failure)

        data_format_curated.when(
            Condition.string_matches("$.objectKey", "*type=structured/*"),
            structured_application_lambda_fn_task.next(check_task_success)
        ).when(
            Condition.string_matches("$.objectKey", "*type=unstructured/*"),
            unstructured_application_lambda_fn_task.next(check_task_success)
        ).otherwise(failure)

        data_format_curated_big_data.when(
            Condition.string_matches("$.objectKey", "*type=structured/*"),
            structured_application_glue_task.next(check_task_success)
        ).when(
            Condition.string_matches("$.objectKey", "*type=unstructured/*"),
            unstructured_application_glue_task.next(check_task_success)
        ).otherwise(failure)

    
        check_task_success.when(
            sfn.Condition.is_present("$.Error"),
            failure
        ).otherwise(success)


        # data_format_choice.when(
        #     Condition.string_matches("$.objectKey", "*type=structured/*"),
        #     structured_curated_lambda_fn_task.next(success)
        # )

        # size_bucket_structure_choice = Choice(self, "Check File Size, Bucket and Structure")

        # size_bucket_structure_choice.when(
        #     Condition.and_(
        #         Condition.number_less_than("$.fileSize", SIZE_THRESHOLD),
        #         Condition.string_matches("$.bucketNameLower", "*stage*"),
        #         Condition.string_matches("$.objectKey", "*type=structured/*")
        #     ),
        #     structured_curated_lambda_fn_task.next(success)
        # ).when(
        #     Condition.and_(
        #         Condition.number_greater_than("$.fileSize", SIZE_THRESHOLD),
        #         Condition.string_matches("$.bucketNameLower", "*stage*"),
        #         Condition.string_matches("$.objectKey", "*type=structured/*")
        #     ),
        #     structured_curated_glue_task.next(success)
        # ).when(
        #     Condition.and_(
        #         Condition.number_less_than("$.fileSize", SIZE_THRESHOLD),
        #         Condition.string_matches("$.bucketNameLower", "*curated*"),
        #         Condition.string_matches("$.objectKey", "*type=structured/*")
        #     ),
        #     structured_application_lambda_fn_task.next(success)
        # ).when(
        #     Condition.and_(
        #         Condition.number_greater_than("$.fileSize", SIZE_THRESHOLD),
        #         Condition.string_matches("$.bucketNameLower", "*curated*"),
        #         Condition.string_matches("$.objectKey", "*type=structured/*")
        #     ),
        #     structured_application_glue_task.next(success)
        # ).when(
        #     Condition.and_(
        #         Condition.number_less_than("$.fileSize", SIZE_THRESHOLD),
        #         Condition.string_matches("$.bucketNameLower", "*stage*"),
        #         Condition.string_matches("$.objectKey", "*type=semi-structured/*")
        #     ),
        #     semi_structured_curated_lambda_fn_task.next(success)
        # ).when(
        #     Condition.and_(
        #         Condition.number_greater_than("$.fileSize", SIZE_THRESHOLD),
        #         Condition.string_matches("$.bucketNameLower", "*stage*"),
        #         Condition.string_matches("$.objectKey", "*type=semi-structured/*")
        #     ),
        #     semi_structured_curated_glue_task.next(success)
        # ).when(
        #     Condition.and_(
        #         Condition.number_less_than("$.fileSize", SIZE_THRESHOLD),
        #         Condition.string_matches("$.bucketNameLower", "*stage*"),
        #         Condition.string_matches("$.objectKey", "*type=unstructured/*")
        #     ),
        #     unstructured_curated_lambda_fn_task.next(success)
        # ).when(
        #     Condition.and_(
        #         Condition.number_greater_than("$.fileSize", SIZE_THRESHOLD),
        #         Condition.string_matches("$.bucketNameLower", "*stage*"),
        #         Condition.string_matches("$.objectKey", "*type=unstructured/*")
        #     ),
        #     unstructured_curated_glue_task.next(success)
        # ).when(
        #     Condition.and_(
        #         Condition.number_less_than("$.fileSize", SIZE_THRESHOLD),
        #         Condition.string_matches("$.bucketNameLower", "*curated*"),
        #         Condition.string_matches("$.objectKey", "*type=unstructured/*")
        #     ),
        #     unstructured_application_lambda_fn_task.next(success)
        # ).when(
        #     Condition.and_(
        #         Condition.number_greater_than("$.fileSize", SIZE_THRESHOLD),
        #         Condition.string_matches("$.bucketNameLower", "*curated*"),
        #         Condition.string_matches("$.objectKey", "*type=unstructured/*")
        #     ),
        #     unstructured_application_glue_task.next(success)
        # ).when(
        #     Condition.and_(
        #         Condition.number_greater_than("$.fileSize", SIZE_THRESHOLD),
        #         Condition.string_matches("$.bucketNameLower", "*application*"),
        #         Condition.string_matches("$.objectKey", "*model/fact*")
        #     ),
        #     snowflake_model_claims_fact_fn_task.next(success)
        # ).otherwise(success)


        self.state_machine = sfn.StateMachine(
            self, id,
            state_machine_name="data-platform-orchestration-state-machine",
            definition_body=sfn.DefinitionBody.from_chainable(file_size_choice),
            logs=sfn.LogOptions(
                destination=logs.LogGroup(self, "StateMachineLogs", retention=logs.RetentionDays.ONE_WEEK),
                level=sfn.LogLevel.ALL
            ),
            tracing_enabled=True #enable x-ray tracing
        )
