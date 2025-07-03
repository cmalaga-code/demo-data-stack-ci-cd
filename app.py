import os
from aws_cdk import App

from data_lake_stack.buckets import S3BucketStack
from github_oidc_stack.oidc import GitHubOIDCStack

app = App()

# check if env variables are present

required_vars = ["STAGE_BUCKET", "CURATED_BUCKET", "APPLICATION_BUCKET", "ENV"]
missing = [var for var in required_vars if not os.environ.get(var)]
if missing:
    raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

# OIDC Authentication For Temp Credentials (Temp Obtain access to Role)

GitHubOIDCStack(app, "GitHubOIDCStack")

# Create data lake stack

bucket_names = {
    "StageBucketStack": os.environ.get("STAGE_BUCKET"),
    "CuratedBucketStack": os.environ.get("CURATED_BUCKET"),
    "ApplicationBucketStack": os.environ.get("APPLICATION_BUCKET")
}

for stack_id, bucket_name in bucket_names.items():
    S3BucketStack(app, stack_id, bucket_name=bucket_name, env_name=os.environ.get("ENV"))

# Synthesize app (executes code and generates CloudFormation Template in JSON format)
app.synth() # executing code and the apis create a file with the proper CloudFormation template cdk.out

