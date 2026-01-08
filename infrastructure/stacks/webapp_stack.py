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
)
from constructs import Construct


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
        user_pool = cognito.UserPool(self, f"{prefix}UserPool")

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

        # VPC for ALB and ECS cluster
        vpc = ec2.Vpc(
            self,
            f"{prefix}AppVpc",
            ip_addresses=ec2.IpAddresses.cidr("10.0.0.0/16"),
            max_azs=2,
            vpc_name=f"{prefix}-stl-vpc",
            nat_gateways=1,
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
            vpc=vpc)
        
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
                "AGENT_ARN": agent_stack.agent_runtime_arn,
            }
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

        # Output CloudFront URL
        CfnOutput(self, "CloudFrontDistributionURL",
                  value=cloudfront_distribution.domain_name)
