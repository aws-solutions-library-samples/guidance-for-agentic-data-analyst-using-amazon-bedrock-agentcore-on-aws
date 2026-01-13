#!/usr/bin/env python3
import os

import aws_cdk as cdk
import cdk_nag

from stacks.data_stack import DataStack
from stacks.agentcore_stack import AgentCoreStack
from stacks.webapp_stack import WebAppStack


env = cdk.Environment(
    account=os.environ.get("AWS_ACCOUNT") or os.environ.get("CDK_DEFAULT_ACCOUNT"),
    region=os.environ.get("AWS_REGION") or os.environ.get("CDK_DEFAULT_REGION")
)

app = cdk.App()

data_stack = DataStack(app, "DataStack", env=env)
agentcore_stack = AgentCoreStack(app, "AgentCoreStack", data_stack=data_stack, env=env)
webapp_stack = WebAppStack(app, "WebAppStack", agent_stack=agentcore_stack, env=env)

cdk.Aspects.of(app).add(cdk_nag.AwsSolutionsChecks())

app.synth()
