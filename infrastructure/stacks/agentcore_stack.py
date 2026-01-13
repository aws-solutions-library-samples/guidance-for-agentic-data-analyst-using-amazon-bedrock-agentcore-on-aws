import hashlib

from aws_cdk import (
    Stack,
    CfnOutput,
    RemovalPolicy,
    Duration,
    CustomResource,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_ecr as ecr,
    aws_codebuild as codebuild,
    aws_s3_assets as s3_assets,
    aws_bedrockagentcore as bedrockagentcore,
    aws_ssm as ssm,
    aws_kms as kms,
)
from constructs import Construct
from cdk_nag import NagSuppressions

class AgentCoreStack(Stack):
    """Stack for AgentCore runtime infrastructure"""

    def __init__(self,
                 scope: Construct,
                 construct_id: str,
                 data_stack,
                 **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        agent_name = "data_analyst_agent_service_cdk"
        image_tag = "latest"

        ecr_repository = ecr.Repository(
            self,
            "ECRRepository",
            repository_name=f"{self.stack_name.lower()}-data-analyst-agent",
            image_tag_mutability=ecr.TagMutability.MUTABLE,
            removal_policy=RemovalPolicy.DESTROY,
            empty_on_delete=True,
            image_scan_on_push=True,
        )
        source_asset = s3_assets.Asset(self, "SourceAsset", path="../agent")
        
        codebuild_role = iam.Role(
            self,
            "CodeBuildRole",
            role_name=f"{self.stack_name}-codebuild-role-{self.region}",
            assumed_by=iam.ServicePrincipal("codebuild.amazonaws.com"),
            inline_policies={
                f"CodeBuildPolicy-{self.region}": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            sid="CloudWatchLogs",
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "logs:CreateLogGroup",
                                "logs:CreateLogStream",
                                "logs:PutLogEvents",
                            ],
                            resources=[
                                f"arn:aws:logs:{self.region}:{self.account}:log-group:/aws/codebuild/*"
                            ],
                        ),
                        iam.PolicyStatement(
                            sid="ECRRepoAccess",
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "ecr:BatchCheckLayerAvailability",
                                "ecr:GetDownloadUrlForLayer",
                                "ecr:BatchGetImage",
                                "ecr:PutImage",
                                "ecr:InitiateLayerUpload",
                                "ecr:UploadLayerPart",
                                "ecr:CompleteLayerUpload",
                            ],
                            resources=[ecr_repository.repository_arn],
                        ),
                        iam.PolicyStatement(
                            sid="ECRAuthorization",
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "ecr:GetAuthorizationToken"
                            ],
                            resources=["*"],
                        ),
                        iam.PolicyStatement(
                            sid="S3SourceAccess",
                            effect=iam.Effect.ALLOW,
                            actions=["s3:GetObject"],
                            resources=[
                                f"{source_asset.bucket.bucket_arn}/*"
                            ],
                        ),
                    ]
                )
            },
        )

        NagSuppressions.add_resource_suppressions(
            codebuild_role,
            [{"id": "AwsSolutions-IAM5", "reason": "CodeBuild requires wildcards for CloudWatch logs, ECR auth token, and S3 source assets"}],
            apply_to_children=True
        )

        codebuild_encryption_key = kms.Key(
            self, "CodeBuildEncryptionKey",
            enable_key_rotation=True,
            description="KMS key for CodeBuild project encryption"
        )

        build_project = codebuild.Project(
            self,
            "AgentImageBuildProject",
            project_name=f"{self.stack_name}-data-analyst-agent-build",
            description=f"Build data-analyst agent Docker image for {self.stack_name}",
            role=codebuild_role,
            timeout=Duration.minutes(60),
            encryption_key=codebuild_encryption_key,
            environment=codebuild.BuildEnvironment(
                build_image=codebuild.LinuxArmBuildImage.AMAZON_LINUX_2_STANDARD_3_0,
                compute_type=codebuild.ComputeType.LARGE,
                privileged=True,
            ),
            source=codebuild.Source.s3(
                bucket=source_asset.bucket, path=source_asset.s3_object_key
            ),
            build_spec=codebuild.BuildSpec.from_object(
                {
                    "version": "0.2",
                    "phases": {
                        "pre_build": {
                            "commands": [
                                "echo Logging in to Amazon ECR...",
                                "aws ecr get-login-password --region $AWS_DEFAULT_REGION | docker login --username AWS --password-stdin $AWS_ACCOUNT_ID.dkr.ecr.$AWS_DEFAULT_REGION.amazonaws.com",
                            ]
                        },
                        "build": {
                            "commands": [
                                "echo Build started on `date`",
                                "echo Building the Docker image for data-analyst agent ARM64...",
                                "docker build -t $IMAGE_REPO_NAME:$IMAGE_TAG .",
                                "docker tag $IMAGE_REPO_NAME:$IMAGE_TAG $AWS_ACCOUNT_ID.dkr.ecr.$AWS_DEFAULT_REGION.amazonaws.com/$IMAGE_REPO_NAME:$IMAGE_TAG",
                            ]
                        },
                        "post_build": {
                            "commands": [
                                "echo Build completed on `date`",
                                "echo Pushing the Docker image...",
                                "docker push $AWS_ACCOUNT_ID.dkr.ecr.$AWS_DEFAULT_REGION.amazonaws.com/$IMAGE_REPO_NAME:$IMAGE_TAG",
                                "echo ARM64 Docker image pushed successfully",
                            ]
                        },
                    },
                }
            ),
            environment_variables={
                "AWS_DEFAULT_REGION": codebuild.BuildEnvironmentVariable(
                    value=self.region
                ),
                "AWS_ACCOUNT_ID": codebuild.BuildEnvironmentVariable(value=self.account),
                "IMAGE_REPO_NAME": codebuild.BuildEnvironmentVariable(
                    value=ecr_repository.repository_name
                ),
                "IMAGE_TAG": codebuild.BuildEnvironmentVariable(
                    value=image_tag
                ),
                "STACK_NAME": codebuild.BuildEnvironmentVariable(
                    value=self.stack_name
                ),
            },
        )

        NagSuppressions.add_resource_suppressions_by_path(
            self,
            f"/{self.stack_name}/CodeBuildRole/DefaultPolicy/Resource",
            [{"id": "AwsSolutions-IAM5", "reason": "CDK grants S3 read permissions for CodeBuild source bucket access"}]
        )

        build_trigger_role = iam.Role(
            self,
            "BuildTriggerRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
        )
        
        build_trigger_role.add_to_policy(iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
            resources=[f"arn:aws:logs:{self.region}:{self.account}:log-group:/aws/lambda/*"],
        ))
        
        build_trigger_role.add_to_policy(iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=["codebuild:StartBuild", "codebuild:BatchGetBuilds"],
            resources=[build_project.project_arn],
        ))

        build_trigger_function = _lambda.Function(
            self,
            "BuildTriggerFunction",
            runtime=_lambda.Runtime.PYTHON_3_14,
            handler="index.handler",
            timeout=Duration.minutes(15),
            code=_lambda.Code.from_asset("lambda/func_build_trigger"),
            role=build_trigger_role,
        )

        NagSuppressions.add_resource_suppressions(
            build_trigger_role,
            [{"id": "AwsSolutions-IAM5", "reason": "Lambda requires CloudWatch logs wildcard for log group creation"}],
            apply_to_children=True
        )
        
        # Custom Resource to Trigger Build
        source_hash = hashlib.md5(source_asset.asset_hash.encode()).hexdigest()[:8]
        trigger_build = CustomResource(
            self,
            "TriggerImageBuild",
            service_token=build_trigger_function.function_arn,
            properties={"ProjectName": build_project.project_name, "SourceHash": source_hash},
        )

        # ===== AgentCore Execution Role =====
        agent_role = iam.Role(
            self, "AgentRole",
            assumed_by=iam.CompositePrincipal(
                iam.ServicePrincipal("bedrock.amazonaws.com"),
                iam.ServicePrincipal("bedrock-agentcore.amazonaws.com")
            ),
            role_name=f"data-analyst-agent-role-{self.stack_name}-{self.region}"
        )

        agent_role_policy = iam.Policy(
            self, "AgentRolePolicy",
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "athena:StartQueryExecution",
                        "athena:GetQueryExecution", 
                        "athena:GetQueryResults",
                        "athena:StopQueryExecution"
                    ],
                    resources=["*"]
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "glue:GetTable",
                        "glue:GetDatabase",
                        "glue:GetTables",
                        "glue:GetPartitions",
                        "glue:GetPartition"
                    ],
                    resources=[
                        f"arn:aws:glue:{self.region}:{self.account}:catalog",
                        f"arn:aws:glue:{self.region}:{self.account}:database/{data_stack.glue_database.ref}",
                        f"arn:aws:glue:{self.region}:{self.account}:table/{data_stack.glue_database.ref}/*"
                    ]
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "ssm:GetParameter", 
                        "ssm:GetParameters"
                    ],
                    resources=[
                        f"arn:aws:ssm:{self.region}:{self.account}:parameter/data-analyst/*"
                    ]
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "s3vectors:QueryVectors", 
                        "s3vectors:GetVectors"
                    ],
                    resources=[
                        f"arn:aws:s3vectors:{self.region}:{self.account}:bucket/{data_stack.dataset_embeddings.vector_bucket_name}/index/*"
                    ]
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "s3:GetObject",
                        "s3:PutObject",
                        "s3:GetObject",
                        "s3:ListBucket",
                        "s3:GetBucketLocation"
                    ],
                    resources=[
                        f"arn:aws:s3:::{data_stack.athena_data_bucket.bucket_name}",
                        f"arn:aws:s3:::{data_stack.athena_data_bucket.bucket_name}/*",
                        f"arn:aws:s3:::{data_stack.dataset_embeddings.vector_bucket_name}",
                        f"arn:aws:s3:::{data_stack.dataset_embeddings.vector_bucket_name}/*"
                    ]
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "s3:GetObject",
                        "s3:PutObject",
                        "s3:ListBucket",
                        "s3:GetBucketLocation"
                    ],
                    resources=[
                        f"arn:aws:s3:::{data_stack.athena_query_results_bucket.bucket_name}",
                        f"arn:aws:s3:::{data_stack.athena_query_results_bucket.bucket_name}/*"
                    ]
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "ecr:GetAuthorizationToken",
                        "ecr:BatchCheckLayerAvailability",
                        "ecr:GetDownloadUrlForLayer",
                        "ecr:BatchGetImage"
                    ],
                    resources=["*"]
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "logs:DescribeLogStreams",
                        "logs:CreateLogGroup",
                        "logs:DescribeLogGroups",
                        "logs:CreateLogStream", 
                        "logs:PutLogEvents"
                    ],
                    resources=[f"arn:aws:logs:{self.region}:{self.account}:log-group:/aws/bedrock-agentcore/runtimes/*", f"arn:aws:logs:{self.region}:{self.account}:log-group:/aws/bedrock-agentcore/runtimes/*:log-stream:*"]
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "xray:PutTraceSegments",
                        "xray:PutTelemetryRecords", 
                        "xray:GetSamplingRules",
                        "xray:GetSamplingTargets"
                    ],
                    resources=["*"]
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=["cloudwatch:PutMetricData"],
                    resources=["*"],
                    conditions={
                        "StringEquals": {
                            "cloudwatch:namespace": "bedrock-agentcore"
                        }
                    }
                ),
                iam.PolicyStatement(
                    sid="BedrockModelInvocation",
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "bedrock:InvokeModel",
                        "bedrock:InvokeModelWithResponseStream"
                    ],
                    resources=[
                        "arn:aws:bedrock:*::foundation-model/*",
                        f"arn:aws:bedrock:{self.region}:{self.account}:*"
                    ]
                ),
                iam.PolicyStatement(
                    sid="BedrockAgentCoreRuntime",
                    effect=iam.Effect.ALLOW,
                    actions=["bedrock-agentcore:InvokeAgentRuntime"],
                    resources=[f"arn:aws:bedrock-agentcore:{self.region}:{self.account}:runtime/*"]
                ),
                iam.PolicyStatement(
                    sid="BedrockAgentCoreMemoryCreateMemory",
                    effect=iam.Effect.ALLOW,
                    actions=["bedrock-agentcore:CreateMemory"],
                    resources=["*"]
                ),
                iam.PolicyStatement(
                    sid="BedrockAgentCoreMemory",
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "bedrock-agentcore:CreateEvent",
                        "bedrock-agentcore:GetEvent",
                        "bedrock-agentcore:GetMemory",
                        "bedrock-agentcore:GetMemoryRecord",
                        "bedrock-agentcore:ListActors",
                        "bedrock-agentcore:ListEvents",
                        "bedrock-agentcore:ListMemoryRecords",
                        "bedrock-agentcore:ListSessions",
                        "bedrock-agentcore:DeleteEvent",
                        "bedrock-agentcore:DeleteMemoryRecord",
                        "bedrock-agentcore:RetrieveMemoryRecords"
                    ],
                    resources=[f"arn:aws:bedrock-agentcore:{self.region}:{self.account}:memory/*"]
                ),
                iam.PolicyStatement(
                    sid="BedrockAgentCoreIdentityGetResourceApiKey",
                    effect=iam.Effect.ALLOW,
                    actions=["bedrock-agentcore:GetResourceApiKey"],
                    resources=[
                        f"arn:aws:bedrock-agentcore:{self.region}:{self.account}:token-vault/default",
                        f"arn:aws:bedrock-agentcore:{self.region}:{self.account}:token-vault/default/apikeycredentialprovider/*",
                        f"arn:aws:bedrock-agentcore:{self.region}:{self.account}:workload-identity-directory/default",
                        f"arn:aws:bedrock-agentcore:{self.region}:{self.account}:workload-identity-directory/default/workload-identity/*"
                    ]
                ),
                iam.PolicyStatement(
                    sid="BedrockAgentCoreIdentityGetResourceOauth2Token",
                    effect=iam.Effect.ALLOW,
                    actions=["bedrock-agentcore:GetResourceOauth2Token"],
                    resources=[
                        f"arn:aws:bedrock-agentcore:{self.region}:{self.account}:token-vault/default",
                        f"arn:aws:bedrock-agentcore:{self.region}:{self.account}:token-vault/default/oauth2credentialprovider/*",
                        f"arn:aws:bedrock-agentcore:{self.region}:{self.account}:workload-identity-directory/default",
                        f"arn:aws:bedrock-agentcore:{self.region}:{self.account}:workload-identity-directory/default/workload-identity/*"
                    ]
                ),
                iam.PolicyStatement(
                    sid="BedrockAgentCoreIdentityGetWorkloadAccessToken",
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "bedrock-agentcore:GetWorkloadAccessToken",
                        "bedrock-agentcore:GetWorkloadAccessTokenForJWT",
                        "bedrock-agentcore:GetWorkloadAccessTokenForUserId"
                    ],
                    resources=[
                        f"arn:aws:bedrock-agentcore:{self.region}:{self.account}:workload-identity-directory/default",
                        f"arn:aws:bedrock-agentcore:{self.region}:{self.account}:workload-identity-directory/default/workload-identity/*"
                    ]
                )
            ],
            roles=[agent_role]
        )

        NagSuppressions.add_resource_suppressions(
            agent_role_policy,
            [{"id": "AwsSolutions-IAM5", "reason": "Agent requires wildcards for Athena queries, Glue tables, S3 buckets, Bedrock models, and AgentCore resources"}]
        )

        self.agent_runtime = bedrockagentcore.CfnRuntime(
            self,
            "AgentRuntime",
            agent_runtime_name=agent_name,
            agent_runtime_artifact=bedrockagentcore.CfnRuntime.AgentRuntimeArtifactProperty(
                container_configuration=bedrockagentcore.CfnRuntime.ContainerConfigurationProperty(
                    container_uri=f"{ecr_repository.repository_uri}:{image_tag}"
                )
            ),
            network_configuration=bedrockagentcore.CfnRuntime.NetworkConfigurationProperty(
                network_mode="PUBLIC",
            ),
            protocol_configuration="HTTP",
            role_arn=agent_role.role_arn,
            description=f"Data-Analyst agent runtime for {self.stack_name}",
            environment_variables={
                "AWS_DEFAULT_REGION": self.region,
                "DATABASES_METADATA_S3": data_stack.athena_data_bucket.bucket_name,
                "ATHENA_OUTPUT_LOCATION": data_stack.athena_query_results_bucket.bucket_name,
                "VERSION_HASH": source_hash
            },
        )

        # AgentCore Runtime depends on successful image build and trace configuration
        self.agent_runtime.node.add_dependency(trigger_build)
        
        # ===== Stack Outputs =====
        ssm.StringParameter(
            self, "AgentRuntimeARNParam",
            parameter_name="/data-analyst/agent-runtime-arn",
            string_value=self.agent_runtime.attr_agent_runtime_arn
        )
        CfnOutput(
            self,
            "AgentRuntimeArn",
            description="ARN of the created agent runtime",
            value=self.agent_runtime.attr_agent_runtime_arn,
            export_name=f"{self.stack_name}-AgentRuntimeArn",
        )
