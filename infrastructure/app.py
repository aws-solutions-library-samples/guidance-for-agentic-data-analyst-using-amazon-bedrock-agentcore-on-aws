#!/usr/bin/env python3
import os

import aws_cdk as cdk

from data_stack import DataStack


env = cdk.Environment(
    account=os.environ.get("AWS_ACCOUNT"),
    region=os.environ.get("AWS_REGION")
)

app = cdk.App()

data_stack = DataStack(app, "DataStack", env=env)

app.synth()
