from aws_cdk import (
    Stack,
    aws_secretsmanager as secretsmanager,
    SecretValue,
    aws_cognito as cognito,
    aws_ec2 as ec2,
    aws_ecs as ecs,
    aws_elasticloadbalancingv2 as elbv2,
    aws_iam as iam,
    aws_cloudfront_origins as origins,
    aws_cloudfront as cloudfront,
    CfnOutput,
    aws_logs as logs,
    aws_s3 as s3,
)
from constructs import Construct
from cdk_nag import NagSuppressions

# Put your own custom value here to prevent ALB to accept requests from
# other clients than CloudFront. You can choose any random string.
CUSTOM_HEADER_NAME = "X-Custom-Header"
CUSTOM_HEADER_VALUE = "AWS_DATA_ANALYST_AGENT"

# ID of Secrets Manager containing cognito parameters
# When you delete a secret, you cannot create another one immediately
# with the same name. Change this value if you destroy your stack and need
# to recreate it with the same STACK_NAME.
SECRETS_MANAGER_ID = "AWS_DATA_ANALYST_COGNITO_SECRET_1"


class WebAppStack(Stack):
    def __init__(self,
                 scope: Construct,
                 construct_id: str,
                 agent_stack,
                 **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        
        prefix = "DataAnalystWebApp"

        # Create Cognito user pool
        user_pool = cognito.UserPool(self, 
            f"{prefix}UserPool",
            password_policy=cognito.PasswordPolicy(
                min_length=8,
                require_digits=True,
                require_lowercase=True,
                require_uppercase=True,
                require_symbols=True
            )
        )

        NagSuppressions.add_resource_suppressions(
            user_pool,
            [{"id": "AwsSolutions-COG3", "reason": "Advanced Security requires Cognito Plus feature plan. We do not use Advanced Security for this sample code."}]
        )

        # Create Cognito client
        user_pool_client = cognito.UserPoolClient(self,
            f"{prefix}UserPoolClient",
            user_pool=user_pool,
            generate_secret=True
        )

        # Store Cognito parameters in a Secrets Manager secret
        secret = secretsmanager.Secret(self,
            f"{prefix}ParamCognitoSecret",
            secret_object_value={
                "pool_id": SecretValue.unsafe_plain_text(user_pool.user_pool_id),
                "app_client_id": SecretValue.unsafe_plain_text(user_pool_client.user_pool_client_id),
                "app_client_secret": user_pool_client.user_pool_client_secret
            },
            secret_name=SECRETS_MANAGER_ID
        )

        NagSuppressions.add_resource_suppressions(
            secret,
            [{"id": "AwsSolutions-SMG4", "reason": "Cognito client secret is managed by Cognito and cannot be rotated independently"}]
        )

        # VPC for ALB and ECS cluster
        vpc = ec2.Vpc(
            self,
            f"{prefix}AppVpc",
            ip_addresses=ec2.IpAddresses.cidr("10.0.0.0/16"),
            max_azs=2,
            vpc_name=f"{prefix}-stl-vpc",
            nat_gateways=1,
        )

        vpc.add_flow_log(
            f"{prefix}FlowLog",
            destination=ec2.FlowLogDestination.to_cloud_watch_logs(
                logs.LogGroup(self, f"{prefix}VpcFlowLogGroup")
            )
        )

        ecs_security_group = ec2.SecurityGroup(
            self,
            f"{prefix}SecurityGroupECS",
            vpc=vpc,
            security_group_name=f"{prefix}-stl-ecs-sg",
        )

        alb_security_group = ec2.SecurityGroup(
            self,
            f"{prefix}SecurityGroupALB",
            vpc=vpc,
            security_group_name=f"{prefix}-stl-alb-sg",
        )

        NagSuppressions.add_resource_suppressions(
            alb_security_group,
            [{"id": "AwsSolutions-EC23", "reason": "ALB is internet-facing and receives traffic from CloudFront which requires 0.0.0.0/0 ingress"}]
        )

        ecs_security_group.add_ingress_rule(
            peer=alb_security_group,
            connection=ec2.Port.tcp(8501),
            description="ALB traffic",
        )

        # ECS cluster and service definition
        cluster = ecs.Cluster(
            self,
            f"{prefix}Cluster",
            enable_fargate_capacity_providers=True,
            container_insights=True,
            vpc=vpc)

        # Access Logs Bucket for ALB
        alb_logs_bucket = s3.Bucket(
            self, f"{prefix}AlbLogsBucket",
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True
        )
        NagSuppressions.add_resource_suppressions(
            alb_logs_bucket,
            [{"id": "AwsSolutions-S1", "reason": "This is the ALB access logs bucket itself"}]
        )
        
        # ALB to connect to ECS
        alb = elbv2.ApplicationLoadBalancer(
            self,
            f"{prefix}Alb",
            vpc=vpc,
            internet_facing=True,
            load_balancer_name=f"{prefix}-stl",
            security_group=alb_security_group,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC),
        )

        alb.log_access_logs(alb_logs_bucket)
        
        fargate_task_definition = ecs.FargateTaskDefinition(
            self,
            f"{prefix}WebappTaskDef",
            memory_limit_mib=512,
            cpu=256,
        )
        
        # Build Dockerfile from local folder and push to ECR
        image = ecs.ContainerImage.from_asset('../webapp')

        fargate_task_definition.add_container(
            f"{prefix}WebContainer",
            # Use an image from DockerHub
            image=image,
            port_mappings=[
                ecs.PortMapping(
                    container_port=8501,
                    protocol=ecs.Protocol.TCP)],
            logging=ecs.LogDrivers.aws_logs(stream_prefix="WebContainerLogs"),
            environment={
                "SECRETS_MANAGER_ID": secret.secret_name,
                "DEPLOYMENT_REGION": self.region,
                "AGENT_ARN": agent_stack.agent_runtime.attr_agent_runtime_arn,
            }
        )

        NagSuppressions.add_resource_suppressions(
            fargate_task_definition,
            [{"id": "AwsSolutions-ECS2", "reason": "Environment variables contain non-sensitive configuration (secret name reference, region, ARN). Sensitive values are retrieved from Secrets Manager at runtime."}]
        )

        NagSuppressions.add_resource_suppressions(
            fargate_task_definition,
            [
                {"id": "AwsSolutions-ECS2", "reason": "Environment variables contain non-sensitive configuration (secret name reference, region, ARN). Sensitive values are retrieved from Secrets Manager at runtime."},
                {"id": "AwsSolutions-IAM5", "reason": "ECR image pull requires ecr:GetAuthorizationToken on * resource"}
            ],
            apply_to_children=True
        )

        service = ecs.FargateService(
            self,
            f"{prefix}ECSService",
            cluster=cluster,
            task_definition=fargate_task_definition,
            service_name=f"{prefix}-stl-front",
            security_groups=[ecs_security_group],
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS),
        )

        # Grant access to Bedrock
        bedrock_policy = iam.Policy(self, f"{prefix}BedrockPolicy",
            statements=[
                iam.PolicyStatement(
                    actions=["bedrock:InvokeModel", "bedrock-agentcore:InvokeAgentRuntime"],
                    resources=["*"]
                )
            ]
        )

        NagSuppressions.add_resource_suppressions(
            bedrock_policy,
            [{"id": "AwsSolutions-IAM5", "reason": "Bedrock and AgentCore invocation requires * resource as model/runtime ARNs are dynamic"}]
        )
        
        task_role = fargate_task_definition.task_role
        task_role.attach_inline_policy(bedrock_policy)

        # Grant access to read the secret in Secrets Manager
        secret.grant_read(task_role)
        
        # Add ALB as CloudFront Origin
        origin = origins.LoadBalancerV2Origin(
            alb,
            custom_headers={CUSTOM_HEADER_NAME: CUSTOM_HEADER_VALUE},
            origin_shield_enabled=False,
            protocol_policy=cloudfront.OriginProtocolPolicy.HTTP_ONLY,
            origin_ssl_protocols=[cloudfront.OriginSslPolicy.TLS_V1_2],
        )

        # S3 bucket for CloudFront access logs
        cloudfront_logs_bucket = s3.Bucket(
            self, f"{prefix}CloudFrontLogsBucket",
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            object_ownership=s3.ObjectOwnership.OBJECT_WRITER
        )
        NagSuppressions.add_resource_suppressions(
            cloudfront_logs_bucket,
            [{"id": "AwsSolutions-S1", "reason": "This is the CloudFront access logs bucket itself"}]
        )

        cloudfront_distribution = cloudfront.Distribution(
            self,
            f"{prefix}CfDist",
            default_behavior=cloudfront.BehaviorOptions(
                origin=origin,
                viewer_protocol_policy=cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
                allowed_methods=cloudfront.AllowedMethods.ALLOW_ALL,
                cache_policy=cloudfront.CachePolicy.CACHING_DISABLED,
                origin_request_policy=cloudfront.OriginRequestPolicy.ALL_VIEWER,
            ),
            minimum_protocol_version=cloudfront.SecurityPolicyProtocol.TLS_V1_2_2021,
            log_bucket=cloudfront_logs_bucket,
            log_file_prefix="cloudfront-logs/"
        )

        NagSuppressions.add_resource_suppressions(
            cloudfront_distribution,
            [
                {"id": "AwsSolutions-CFR4", "reason": "Using default CloudFront domain without custom SSL certificate. TLS 1.2 is set via minimum_protocol_version but viewer certificate requires custom domain for full control."},
                {"id": "AwsSolutions-CFR5", "reason": "Origin uses HTTP_ONLY as ALB does not have HTTPS listener. Traffic is secured via custom header validation and CloudFront enforces HTTPS for viewers."}
            ]
        )
        
        # ALB Listener
        http_listener = alb.add_listener(
            f"{prefix}HttpListener",
            port=80,
            open=True,
        )

        http_listener.add_targets(
            f"{prefix}TargetGroup",
            target_group_name=f"{prefix}-tg",
            port=8501,
            priority=1,
            conditions=[
                elbv2.ListenerCondition.http_header(
                    CUSTOM_HEADER_NAME,
                    [CUSTOM_HEADER_VALUE])],
            protocol=elbv2.ApplicationProtocol.HTTP,
            targets=[service],
        )
        # add a default action to the listener that will deny all requests that
        # do not have the custom header
        http_listener.add_action(
            "default-action",
            action=elbv2.ListenerAction.fixed_response(
                status_code=403,
                content_type="text/plain",
                message_body="Access denied",
            ),
        )

        NagSuppressions.add_resource_suppressions_by_path(
            self,
            f"/{self.stack_name}/AWS679f53fac002430cb0da5b7982bd2287/ServiceRole/Resource",
            [{"id": "AwsSolutions-IAM4", "reason": "CDK internal Lambda for Cognito client secret retrieval"}]
        )
        NagSuppressions.add_resource_suppressions_by_path(
            self,
            f"/{self.stack_name}/AWS679f53fac002430cb0da5b7982bd2287/Resource",
            [{"id": "AwsSolutions-L1", "reason": "CDK internal Lambda runtime managed by CDK"}]
        )

        # Output CloudFront URL
        CfnOutput(self, "CloudFrontDistributionURL",
                  value=cloudfront_distribution.domain_name)
