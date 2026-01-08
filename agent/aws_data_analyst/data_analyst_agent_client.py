import json

import boto3

from aws_data_analyst.infrastructure import AGENT_ARN


class AgentCoreClient:
    def __init__(self):
        self.agent_arn = AGENT_ARN
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


if __name__ == "__main__":
    agent = AgentCoreClient()
    for msg in agent.invoke("What is the population of Cambridge?"):
        print(f"MSG: {msg}")
