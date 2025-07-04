## Deploy Status

<!-- ![CDK Deploy](https://github.com/<your-org-or-username>/<your-repo>/actions/workflows/<workflow-file-name>.yml/badge.svg) -->
![CDK Deploy](https://github.com/cmalaga-code/demo-data-stack-ci-cd/actions/workflows/cdk_workflow.yml/badge.svg)

![CDK Deploy](https://github.com/cmalaga-code/demo-data-stack-ci-cd/actions/workflows/snowflake_workflow.yml/badge.svg)


## app.py

- CDK entry point
- Instantiates stacks from each service
- Each service has its own stack class

```bash
cdk deploy IngestionStack
cdk deploy TransformationStack
```


## cdk.json

- Core configuration
- Tells cdk cli how to run app and can store context values and feature flags

