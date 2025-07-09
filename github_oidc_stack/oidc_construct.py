from aws_cdk import aws_iam as iam, Stack, Duration
from aws_cdk import App
from constructs import Construct

class GitHubOIDCStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        # 1. Define the OIDC provider for GitHub
        provider = iam.OpenIdConnectProvider(
            self, id,
            url="https://token.actions.githubusercontent.com",
            client_ids=["sts.amazonaws.com"]
        )
        # 2. Define the GitHub repo that can assume the role
        repo = self.node.try_get_context("repoName")  # Replace with your actual GitHub org/repo

        github_principal = iam.OpenIdConnectPrincipal(provider).with_conditions({
            "StringLike": {
                "token.actions.githubusercontent.com:sub": f"repo:{repo}:*"
            }
        })

        # 3. Create the IAM role that GitHub Actions can assume
        role = iam.Role(
            self, "github-action-role",
            role_name="github-actions-deploy-role",
            assumed_by=github_principal,
            description="Role assumed by GitHub Actions via OIDC",
            max_session_duration=Duration.hours(1),
            # managed_policies=[
            #     iam.ManagedPolicy.from_aws_managed_policy_name("AdministratorAccess")
            # ]
        )

        # demo
        role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    # Lambda
                    "lambda:*",
                    # S3
                    "s3:*",

                    # Glue
                    "glue:*",

                    # Step Functions
                    "states:*",

                    # IAM (for creating roles for Lambda, Step Functions, etc.)
                    "iam:PassRole",
                    "iam:GetRole",
                    "iam:CreateRole",
                    "iam:AttachRolePolicy",
                    "iam:PutRolePolicy",
                    "iam:DeleteRole",
                    "iam:DetachRolePolicy",
                    "iam:DeleteRolePolicy"
                ],
                resources=["*"]
            )
        )


if __name__== "__main__":
    app = App()
    # OIDC Authentication For Temp Credentials (Temp Obtain access to Role) -- this is for the docker container and only execute once
    GitHubOIDCStack(app, "gitHub-oidc-stackv2")
    app.synth()