#!/usr/bin/env python3
import os

import aws_cdk as cdk

from stacks.data_stack import DataStack
from stacks.agentcore_stack import AgentCoreStack


env = cdk.Environment(
    account=os.environ.get("AWS_ACCOUNT"),
    region=os.environ.get("AWS_REGION")
)

app = cdk.App()

data_stack = DataStack(app, "DataStack", env=env)
agentcore_stack = AgentCoreStack(app, "AgentCoreStack", data_stack=data_stack, env=env)

app.synth()
