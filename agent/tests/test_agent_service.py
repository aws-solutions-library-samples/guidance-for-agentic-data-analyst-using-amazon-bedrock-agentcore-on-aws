"""Smoke tests for the agent service, run fully offline (see conftest.py).

They exercise the real dependency stack that dependency/security updates touch:
bedrock-agentcore (HTTP app + SSE streaming), strands-agents (event loop, tool
execution), strands-code-agent + smolagents (sandboxed Python REPL), pandas,
plotly, matplotlib, s3fs and boto3/botocore (SSM, S3, Athena, S3 Vectors, Bedrock).

The SSE events asserted here are the contract consumed by the web UI parser in
`user-interface/src/services/api.ts`. The same scenario is served to the UI
end-to-end tests by `e2e_agent_server.py` (see `user-interface/e2e/full-stack.spec.ts`).
"""
import json

import pytest
from starlette.testclient import TestClient

from offline_aws import (
    DATASET_KEY, DATASET_METADATA, EMBEDDING_DIMENSION, SCENARIO_ANSWER, SCENARIO_ATHENA_COLUMNS,
    SCENARIO_ATHENA_ROWS, SCENARIO_INTERMEDIATE_TEXT, VECTOR_BUCKET, VECTOR_INDEX, ScriptedModel,
    analysis_scenario,
)


@pytest.fixture(scope="module")
def service():
    from aws_data_analyst import data_analyst_agent_service
    return data_analyst_agent_service


@pytest.fixture
def client(service):
    with TestClient(service.app) as test_client:
        yield test_client


def read_sse_events(response):
    events = []
    for line in response.iter_lines():
        if line.startswith("data: "):
            events.append(json.loads(line[len("data: "):]))
    return events


def test_ping(client):
    response = client.get("/ping")
    assert response.status_code == 200


def test_solution_user_agent_is_registered(service):
    import boto3
    from aws_data_analyst.solution_user_agent import SOLUTION_UA

    session = boto3.session.Session()
    assert SOLUTION_UA in (session._session.user_agent_extra or "")


def test_invocation_streams_full_analysis(client, monkeypatch, tmp_path, athena_results, aws_calls):
    from aws_data_analyst import data_analyst_agent

    turns, analysis_code = analysis_scenario(tmp_path)
    model = ScriptedModel(turns)
    model_kwargs = {}

    def fake_bedrock_model(**kwargs):
        model_kwargs.update(kwargs)
        return model

    monkeypatch.setattr(data_analyst_agent, "BedrockModel", fake_bedrock_model)
    athena_results(SCENARIO_ATHENA_COLUMNS, SCENARIO_ATHENA_ROWS)

    with client.stream("POST", "/invocations", json={
        "message": "What was the highest unemployment rate?",
        "history": [["user", "Hello"], ["assistant", "Hi, how can I help?"]],
    }) as response:
        assert response.status_code == 200
        assert response.headers["content-type"].startswith("text/event-stream")
        events = read_sse_events(response)

    errors = [e for e in events if "error" in e]
    assert not errors, f"Agent stream reported errors: {errors}"

    by_type = {}
    for event in events:
        by_type.setdefault((event.get("msg_type"), event.get("name")), []).append(event)

    # 1. Initial dataset retrieval enriched with S3 metadata.
    assert events[0]["msg_type"] == "datasets"
    entry = events[0]["datasets"]["entries"][0]
    assert entry["key"] == DATASET_KEY
    assert entry["title"] == DATASET_METADATA["title"]
    assert entry["url"] == DATASET_METADATA["url"]

    # 2. Agentic dataset search tool.
    assert by_type[("toolUse", "search_datasets")][0]["input"] == {"query": "unemployment rate"}
    search_result = by_type[("toolResult", "search_datasets")][0]
    assert search_result["status"] == "success"
    assert "# ID [ons.labour-market-test]" in search_result["content"][0]["text"]

    # 3. Sandboxed Python REPL querying Athena through the toolkit.
    assert by_type[("toolUse", "python_repl")][0]["input"]["code"] == analysis_code
    repl_output = by_type[("toolResult", "python_repl")][0]["content"][0]["text"]
    assert "ROWS=3 MAX=5.1" in repl_output, repl_output
    assert "STDERR" not in repl_output, repl_output

    # 4. Visualizations forwarded to the UI (results of these tools are not streamed).
    chart_event = by_type[("toolUse", "visualize_interactive_chart")][0]
    chart_spec = json.loads(chart_event["chart_spec"])
    assert chart_spec["data"][0]["type"] == "scatter"
    image_event = by_type[("toolUse", "visualize_image")][0]
    assert image_event["image"].startswith("iVBOR")  # base64-encoded PNG signature
    assert ("toolResult", "visualize_image") not in by_type
    assert ("toolResult", "visualize_interactive_chart") not in by_type

    # 5. Final result with parsed answer and metrics.
    assert any(e.get("msg_type") == "text" and e["text"] == SCENARIO_INTERMEDIATE_TEXT for e in events)
    result = events[-1]
    assert result["msg_type"] == "result"
    assert result["result"]["answer"] == SCENARIO_ANSWER
    assert result["result"]["supported_by_data"] is True
    agent_metrics = result["result"]["metrics"]["agent"]
    assert agent_metrics["total_cycles"] == 4
    assert agent_metrics["on_demand_cost"] > 0
    used = result["result"]["metrics"]["query"]["datasets"]
    assert [(d["key"], d["title"], d["count"]) for d in used] == [(DATASET_KEY, DATASET_METADATA["title"], 1)]

    # The conversation history and default model are passed to the LLM.
    assert model_kwargs["model_id"] == data_analyst_agent.DEFAULT_MODEL_ID
    first_messages = model.requests[0]["messages"]
    assert first_messages[0] == {"role": "user", "content": [{"text": "Hello"}]}
    assert first_messages[1] == {"role": "assistant", "content": [{"text": "Hi, how can I help?"}]}
    tool_names = {spec["name"] for spec in model.requests[0]["tool_specs"]}
    assert {"python_repl", "search_datasets", "visualize_image", "visualize_interactive_chart"} <= tool_names

    # Vector search + embeddings were serialized by botocore with the expected parameters.
    query_calls = [c["body"] for name, c in aws_calls if name == "s3vectors.QueryVectors"]
    assert query_calls, aws_calls
    for body in query_calls:
        assert body["topK"] == data_analyst_agent.DATASET_RETRIEVAL_TOPK
        assert body["vectorBucketName"] == VECTOR_BUCKET and body["indexName"] == VECTOR_INDEX
        assert len(body["queryVector"]["float32"]) == EMBEDDING_DIMENSION
    embed_calls = [c for name, c in aws_calls if name == "bedrock-runtime.InvokeModel"]
    assert embed_calls, aws_calls
    for call in embed_calls:
        assert "amazon.nova-2-multimodal-embeddings-v1" in call["url_path"]
        assert call["body"]["singleEmbeddingParams"]["embeddingPurpose"] == "GENERIC_RETRIEVAL"


def test_invocation_reports_model_failure(client, monkeypatch):
    """A failing LLM call must surface as an SSE error event the UI can display."""
    from aws_data_analyst import data_analyst_agent

    class FailingModel(ScriptedModel):
        async def stream(self, *args, **kwargs):
            raise RuntimeError("model unavailable")
            yield  # pragma: no cover

    monkeypatch.setattr(data_analyst_agent, "BedrockModel", lambda **_: FailingModel([]))

    with client.stream("POST", "/invocations", json={"message": "anything"}) as response:
        events = read_sse_events(response)

    assert events[0]["msg_type"] == "datasets"
    assert any("error" in e for e in events), events
