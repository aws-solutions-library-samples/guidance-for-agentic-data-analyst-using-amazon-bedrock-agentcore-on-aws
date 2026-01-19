import os
import json
from sys import platform

import boto3
import requests


class AgentCore:
    def __init__(self):
        self.agent_arn = os.getenv("AGENT_ARN")
        self.agent_core_client = boto3.client('bedrock-agentcore')

    def invoke(self, message, session_history=None):
        response = self.agent_core_client.invoke_agent_runtime(
            agentRuntimeArn=self.agent_arn, 
            payload=json.dumps({"message": message, "session_history": session_history}).encode()
        )
        for line in response["response"].iter_lines(chunk_size=10):
            if not line:
                continue
            line = line.decode("utf-8")
            if line.startswith("data: "):
                line = line[6:]
            yield json.loads(line)



class LocalhostAgent:
    """
    To spin-up the Agent server locally run:
    python -m aws_data_analyst.data_analyst_agent_service
    """
    def invoke(self, message, session_history=None):
        response = requests.post(
            "http://localhost:8080/invocations",
            headers={"Content-Type": "application/json"},
            json={"message": message, "session_history": session_history},
            stream=True,
            timeout=600)
        response.raise_for_status()

        for line in response.iter_lines(decode_unicode=True):
            if not line:
                continue
            elif line.startswith("data: "):
                    line = line[6:]
            yield json.loads(line)


def get_agent():
    if platform == "darwin":
        return LocalhostAgent()
    else:
        return AgentCore()
