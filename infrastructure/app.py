#!/usr/bin/env python3
import os

import aws_cdk as cdk
import cdk_nag

from stacks.data_stack import DataStack
from stacks.agentcore_stack import AgentCoreStack
from stacks.webapp_stack import WebAppStack
from stacks.waf_stack import WafStack

env = cdk.Environment(
    account=os.environ.get("AWS_ACCOUNT") or os.environ.get("CDK_DEFAULT_ACCOUNT"),
    region=os.environ.get("AWS_REGION") or os.environ.get("CDK_DEFAULT_REGION")
)

# us-east-1 environment for WAF deployment
us_east_1_env = cdk.Environment(
    account=os.environ.get("AWS_ACCOUNT") or os.environ.get("CDK_DEFAULT_ACCOUNT"),
    region="us-east-1"
)

app = cdk.App()

waf_stack = WafStack(app, "WafStack", env=us_east_1_env, cross_region_references=True, description="SO9670 - Web Application Firewall Stack")
data_stack = DataStack(app, "DataStack", env=env, description="SO9670 - Data Stack")
agentcore_stack = AgentCoreStack(app, "AgentCoreStack", data_stack=data_stack, env=env, description="SO9670 - AgentCore Stack")
webapp_stack = WebAppStack(app, "WebAppStack", agent_stack=agentcore_stack, waf_stack=waf_stack, env=env, cross_region_references=True, description="SO9670 - WebApp Stack")

cdk.Aspects.of(app).add(cdk_nag.AwsSolutionsChecks())

app.synth()
