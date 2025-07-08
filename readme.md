## Deploy Status 🚀🚀🚀

<!-- ![CDK Deploy](https://github.com/<your-org-or-username>/<your-repo>/actions/workflows/<workflow-file-name>.yml/badge.svg) -->
![CDK Deploy](https://github.com/cmalaga-code/demo-data-stack-ci-cd/actions/workflows/cdk_workflow.yml/badge.svg)

![CDK Deploy](https://github.com/cmalaga-code/demo-data-stack-ci-cd/actions/workflows/snowflake_workflow.yml/badge.svg)

## .github/workflows 

- Contains CI/CD automated pipeline
- Yaml file(s) used to define list of steps docker container will execute
- Used to deploy to Dev, Stage, and Production
- Allows for automated quick deployments

## app.py

- CDK (Cloud Developer Kit) entry point
- Instantiates stacks from each service and utilizes the constructs to create infrastructure
- Each service has its own stack

```bash
cd synth
cdk deploy IngestionStack
cdk deploy TransformationStack
```

## cdk.json

- Core configuration
- Tells cdk cli how to run app and can store context values and feature flags (basically configurations)

## CDK Stacks

### ./compute_stack/

- Contains the compute constructs
- Lambda functions & Glue Jobs
- L1, L2, L3

### ./data_lake_stack/

- Contains data lake constructs
- S3, S3 Event Notification, Lambda (Meta Data -- import from compute_stack)

### ./data_warehouse_stack/

- Deploy data model
- Deploy data mart
- Deploy role permissions

### ./github_oidc_stack/

- Contains github actions construct
- Authenticate utilizing temporary credentials by assuming role

### ./orchestration_stack/

- Contains orchestration construct

