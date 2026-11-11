"""Offline harness for running the Data Analyst agent service without AWS.

Used by the pytest suite (via conftest.py) and by `e2e_agent_server.py`, which
serves the agent to the web UI end-to-end tests.

The agent normally talks to SSM, S3, Athena, S3 Vectors and Bedrock. In CI none
of those exist, so importing this module:

* points every AWS SDK at fake credentials (no request can ever reach real AWS),
* starts `moto` and seeds it with the SSM parameters / S3 metadata the agent
  reads at import time (Athena runs on moto as well, with queued results),
* answers the two calls moto cannot emulate (S3 Vectors `QueryVectors` and the
  Bedrock embedding `InvokeModel`) through botocore `before-call` hooks. Those
  hooks run *after* botocore has validated and serialized the request against
  the installed service model, so a boto3/botocore bump that breaks the call
  shape still fails.

It also provides `ScriptedModel`, a deterministic stand-in for the Bedrock LLM
that drives the real Strands / strands-code-agent loop with pre-defined tool calls.

This module must be imported before `aws_data_analyst`, because several of its
modules create AWS clients and read SSM parameters at import time.
"""
import io
import json
import os

# --- 1. Isolate from any real AWS configuration (must happen before boto3 import)
for _var in ("AWS_PROFILE", "AWS_SESSION_TOKEN", "AWS_SECURITY_TOKEN"):
    os.environ.pop(_var, None)
os.environ.update({
    "AWS_ACCESS_KEY_ID": "testing",
    "AWS_SECRET_ACCESS_KEY": "testing",
    "AWS_DEFAULT_REGION": "us-east-1",
    "AWS_REGION": "us-east-1",
    "AWS_CONFIG_FILE": os.devnull,
    "AWS_SHARED_CREDENTIALS_FILE": os.devnull,
    "AWS_EC2_METADATA_DISABLED": "true",
    # Keep OpenTelemetry quiet/offline if the distro is auto-loaded.
    "OTEL_SDK_DISABLED": "true",
})

import boto3  # noqa: E402
import botocore.handlers  # noqa: E402
from botocore.awsrequest import AWSResponse  # noqa: E402
from botocore.response import StreamingBody  # noqa: E402
from moto import mock_aws  # noqa: E402
from moto.athena.models import QueryResults, athena_backends  # noqa: E402
from moto.core import DEFAULT_ACCOUNT_ID  # noqa: E402

from strands.models import Model  # noqa: E402


REGION = "us-east-1"
DATA_BUCKET = "datasets-test"
ATHENA_RESULTS_BUCKET = "athena-results-test"
VECTOR_BUCKET = "vectors-test"
VECTOR_INDEX = "datasets-test"
EMBEDDING_DIMENSION = 1024

DATASET_KEY = "ons.labour-market-test"
DATASET_METADATA = {
    "namespace": "ons",
    "id": "labour-market-test",
    "title": "Labour market test dataset",
    "url": "https://example.com/datasets/labour-market-test",
    "indexing-description": "ONS Dataset - Labour market (unemployment rate)",
    "usage-description": (
        "# ID [ons.labour-market-test]\n"
        "ONS Dataset - Labour market (unemployment rate)\n"
        "Dimensions:\n\t- time: Year"
    ),
}

SSM_PARAMETERS = {
    "data-bucket": DATA_BUCKET,
    "athena-query-results-bucket": ATHENA_RESULTS_BUCKET,
    "agent-runtime-arn": f"arn:aws:bedrock-agentcore:{REGION}:{DEFAULT_ACCOUNT_ID}:runtime/test",
    "vectordb_embedder": "nova",
    "vectordb_dimension": str(EMBEDDING_DIMENSION),
    "vectordb_bucket": VECTOR_BUCKET,
    "vectordb_index": VECTOR_INDEX,
}


# --- 2. Hooks for the AWS calls moto cannot emulate ---------------------------
# Every request reaching these hooks has been validated and serialized by
# botocore; it is recorded so tests can assert on what the agent sent.
AWS_CALLS = []


def _ok():
    # Same (http_response, parsed) contract used by botocore.stub.Stubber.
    return AWSResponse(None, 200, {}, None)


def _fake_query_vectors(params, **_):
    # `params` is the serialized HTTP request (rest-json): the body is the JSON payload.
    AWS_CALLS.append(("s3vectors.QueryVectors", {"url_path": params["url_path"], "body": json.loads(params["body"])}))
    response = {"vectors": [{"key": DATASET_KEY, "distance": 0.1, "metadata": {}}], "distanceMetric": "cosine"}
    return _ok(), response


def _fake_invoke_model(params, **_):
    # The model ID is serialized into the URL path, the request blob is the body.
    AWS_CALLS.append(("bedrock-runtime.InvokeModel", {"url_path": params["url_path"], "body": json.loads(params["body"])}))
    body = json.dumps({"embeddings": [{"embeddingType": "TEXT", "embedding": [0.01] * EMBEDDING_DIMENSION}]}).encode()
    response = {"body": StreamingBody(io.BytesIO(body), len(body)), "contentType": "application/json"}
    return _ok(), response


# BUILTIN_HANDLERS are copied into every botocore session created afterwards,
# including sessions created internally by s3fs / strands / bedrock-agentcore.
botocore.handlers.BUILTIN_HANDLERS.extend([
    ("before-call.s3vectors.QueryVectors", _fake_query_vectors),
    ("before-call.bedrock-runtime.InvokeModel", _fake_invoke_model),
])


# --- 3. Start moto and seed the resources read at import time -----------------
MOCK = mock_aws()
MOCK.start()


def _seed_aws():
    ssm = boto3.client("ssm", region_name=REGION)
    for name, value in SSM_PARAMETERS.items():
        ssm.put_parameter(Name=f"/data-analyst/{name}", Value=value, Type="String")

    s3 = boto3.client("s3", region_name=REGION)
    s3.create_bucket(Bucket=DATA_BUCKET)
    s3.create_bucket(Bucket=ATHENA_RESULTS_BUCKET)
    s3.put_object(
        Bucket=DATA_BUCKET,
        Key=f"metadata/ons/{DATASET_METADATA['id']}/dataset.json",
        Body=json.dumps(DATASET_METADATA).encode(),
    )


_seed_aws()


def queue_athena_result(columns, rows):
    """Queue the rows returned by the next Athena query (first row is the header, as in real Athena)."""
    to_row = lambda values: {"Data": [{"VarCharValue": str(v)} for v in values]}  # noqa: E731
    athena_backends[DEFAULT_ACCOUNT_ID][REGION].query_results_queue.append(QueryResults(
        rows=[to_row(columns)] + [to_row(r) for r in rows],
        column_info=[{"Name": c, "Type": "varchar"} for c in columns],
    ))


def clear_athena_results():
    athena_backends[DEFAULT_ACCOUNT_ID][REGION].query_results_queue.clear()


# --- 4. Deterministic LLM ------------------------------------------------------
def tool_use(name, tool_input, tool_use_id=None):
    return {"toolUse": {"name": name, "input": tool_input, "toolUseId": tool_use_id or f"tooluse_{name}"}}


def text(content):
    return {"text": content}


class ScriptedModel(Model):
    """A Strands `Model` that replays one scripted assistant turn per `stream()` call.

    Each turn is a list of content blocks built with `tool_use(...)` / `text(...)`.
    The real Strands event loop executes the requested tools and calls `stream()`
    again with the tool results, which are recorded in `self.requests`.
    """

    def __init__(self, turns):
        self.turns = list(turns)
        self.requests = []
        self.config = {"model_id": "scripted"}

    def update_config(self, **model_config):
        self.config.update(model_config)

    def get_config(self):
        return self.config

    async def structured_output(self, output_model, prompt, system_prompt=None, **kwargs):
        raise NotImplementedError
        yield  # pragma: no cover - makes this an async generator

    async def stream(self, messages, tool_specs=None, system_prompt=None, **kwargs):
        self.requests.append({"messages": messages, "tool_specs": tool_specs, "system_prompt": system_prompt})
        if not self.turns:
            raise AssertionError("ScriptedModel ran out of scripted turns")
        blocks = self.turns.pop(0)

        yield {"messageStart": {"role": "assistant"}}
        stop_reason = "end_turn"
        for block in blocks:
            if "toolUse" in block:
                stop_reason = "tool_use"
                tu = block["toolUse"]
                yield {"contentBlockStart": {"start": {"toolUse": {"name": tu["name"], "toolUseId": tu["toolUseId"]}}}}
                yield {"contentBlockDelta": {"delta": {"toolUse": {"input": json.dumps(tu["input"])}}}}
            else:
                yield {"contentBlockStart": {"start": {}}}
                yield {"contentBlockDelta": {"delta": {"text": block["text"]}}}
            yield {"contentBlockStop": {}}
        yield {"messageStop": {"stopReason": stop_reason}}
        yield {"metadata": {"usage": {"inputTokens": 100, "outputTokens": 20, "totalTokens": 120}, "metrics": {"latencyMs": 1}}}


# --- 5. Shared end-to-end analysis scenario -----------------------------------
SCENARIO_INTERMEDIATE_TEXT = "Let me look for more datasets."
SCENARIO_ANSWER = "The unemployment rate peaked at 5.1% in 2021."
SCENARIO_ATHENA_COLUMNS = ["time", "observation"]
SCENARIO_ATHENA_ROWS = [["2020", "4.5"], ["2021", "5.1"], ["2022", "3.9"]]


def analysis_scenario(work_dir):
    """Return (turns, analysis_code) for a realistic run touching every agent capability:
    agentic dataset search, sandboxed pandas analysis over Athena, a Plotly chart,
    a matplotlib/seaborn image and a final JSON answer.

    Call `queue_athena_result(SCENARIO_ATHENA_COLUMNS, SCENARIO_ATHENA_ROWS)` before running it.
    """
    chart_path = os.path.join(str(work_dir), "chart.json")
    image_path = os.path.join(str(work_dir), "chart.png")
    analysis_code = f"""
df = query_handler.query_dataset('{DATASET_KEY}', {{'geography': "United Kingdom"}})
print(f"ROWS={{len(df)}} MAX={{df['observation'].max()}}")
fig = px.line(df, x='time', y='observation', title='Unemployment rate')
fig.write_json('{chart_path}')
ax = sns.lineplot(data=df, x='time', y='observation')
plt.savefig('{image_path}')
"""
    final_answer = json.dumps({"answer": SCENARIO_ANSWER, "supported_by_data": True})
    turns = [
        [text(SCENARIO_INTERMEDIATE_TEXT), tool_use("search_datasets", {"query": "unemployment rate"})],
        [tool_use("python_repl", {"code": analysis_code})],
        [tool_use("visualize_interactive_chart", {"chart_path": chart_path, "chart_type": "plotly"}, "tooluse_chart"),
         tool_use("visualize_image", {"image_path": image_path}, "tooluse_image")],
        [text(final_answer)],
    ]
    return turns, analysis_code
