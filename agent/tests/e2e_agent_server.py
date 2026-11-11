"""Run the real agent service on http://127.0.0.1:8080, fully offline, for UI end-to-end tests.

AWS is emulated by `offline_aws` (moto + botocore hooks) and the Bedrock LLM is
replaced by `ScriptedModel` replaying `analysis_scenario` on every request, so
the web UI receives exactly the SSE stream the production service would emit.

Usage (from the `agent` directory):
    python tests/e2e_agent_server.py
"""
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import offline_aws  # noqa: E402  (must be imported before aws_data_analyst)

from aws_data_analyst import data_analyst_agent  # noqa: E402
from aws_data_analyst.data_analyst_agent_service import app  # noqa: E402


def scripted_bedrock_model(**_):
    work_dir = tempfile.mkdtemp(prefix="e2e-agent-")
    turns, _code = offline_aws.analysis_scenario(work_dir)
    # The scenario runs exactly one Athena query: queue one result per request. Never clear
    # the (shared) queue, otherwise concurrent requests would steal each other's results.
    offline_aws.queue_athena_result(offline_aws.SCENARIO_ATHENA_COLUMNS, offline_aws.SCENARIO_ATHENA_ROWS)
    return offline_aws.ScriptedModel(turns)


data_analyst_agent.BedrockModel = scripted_bedrock_model


if __name__ == "__main__":
    app.run(port=int(os.environ.get("E2E_AGENT_PORT", "8080")), host="127.0.0.1")
