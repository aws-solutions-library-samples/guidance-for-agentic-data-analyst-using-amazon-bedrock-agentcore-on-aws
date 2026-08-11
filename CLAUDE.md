# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

An AWS Guidance (reference solution, not a maintained product) for a data-analyst agent that answers natural-language questions over hundreds of datasets queryable via Amazon Athena. Built on **Strands Agents** + **strands-code-agent** (a *coding* agent that writes/executes Python in a sandboxed REPL rather than making structured tool calls), deployed on **Amazon Bedrock AgentCore**, with semantic dataset discovery via **Amazon S3 Vectors**. See `README.md` for the full architecture write-up, design rationale, and cost tables — read it before making non-trivial changes.

Three-part repo, each with its own dependency set:

- `agent/` — Python agent code, deployed as a container image to Bedrock AgentCore.
- `infrastructure/` — AWS CDK (Python) app defining all AWS resources.
- `user-interface/` — React + TypeScript (Vite) web app.

## Commands

### Infrastructure (CDK)

```bash
pip install -r requirements.txt      # from repo root: CDK + agent + eval deps
cd infrastructure
cdk deploy --all                     # deploy all 4 stacks (DataStack, AgentCoreStack, WebAppStack, WafStack)
cdk destroy --all                    # teardown (empty S3 buckets first, see README Cleanup)
```

`cdk-nag` AwsSolutionsChecks run on every synth (`infrastructure/app.py`) — new resources that trip a check need a `NagSuppressions` entry with a reason, following the existing pattern in `infrastructure/stacks/data_stack.py`.

### Agent (local)

```bash
cd agent
python -m aws_data_analyst.data_analyst_agent_service   # serves on http://localhost:8080; needs AWS creds (Bedrock, S3, Athena, SSM)
```

The agent reads its own config (bucket names, vector DB params, agent ARN) from SSM parameters under `/data-analyst/*` (see `agent/aws_data_analyst/infrastructure.py`) — these only exist once `DataStack`/`AgentCoreStack` are deployed.

### UI

```bash
./scripts/start-ui-local.sh          # points UI at the deployed (remote) AgentCore endpoint
./scripts/start-ui-local.sh --local  # points UI at an agent running on localhost:8080
```

Both pull Cognito/AgentCore config from the deployed `WebAppStack` CloudFormation outputs and write `user-interface/.env.local`. Requires infra to already be deployed. Under the hood: `cd user-interface && npm install && npm start` (Vite dev server). `npm run build` builds for production.

### Full deploy (infra + UI)

```bash
./scripts/deploy.sh          # UI build+deploy only (assumes infra already deployed)
./scripts/deploy.sh --cdk    # also runs `cdk deploy --all` first
```

### Dataset ingestion (sample data)

```bash
cd agent
python aws_data_analyst/datasets/ons/download_datasets.py
python aws_data_analyst/datasets/ons/preprocess_datasets.py
python aws_data_analyst/datasets/oecd/oecd_data.py
python aws_data_analyst/datasets/upload_datasets_to_s3.py
```

### Benchmarks / evaluation

There is no unit test suite — verification is via evaluation scripts under `agent/aws_data_analyst/evaluation/`:

```bash
cd agent
python aws_data_analyst/evaluation/benchmark_dataset_discovery.py   # embedding model recall@K
python aws_data_analyst/evaluation/benchmark_agent.py               # end-to-end agent quality/cost/latency, LLM-as-judge
```

`benchmark_agent.py` calls the **deployed** AgentCore runtime through `AgentCoreClient` (`agent/aws_data_analyst/data_analyst_agent_client.py`), not the local agent — it exercises real infrastructure and incurs Bedrock cost.

## Architecture

### Two runtime flows

- **Ingestion flow**: an admin uploads a Parquet file (`datasets/<namespace>/<id>/data.parquet`) and a JSON metadata file (`metadata/<namespace>/<id>/dataset.json`) to the data S3 bucket. Two S3-triggered Lambdas react independently: `infrastructure/lambda/parse_dataset` registers the Parquet as an external Glue/Athena table; `infrastructure/lambda/indexer_dataset` embeds the metadata's `indexing-description` and writes it to the S3 Vectors index. A dataset's ID is `<namespace>.<dataset-name>`, and its Athena table name is `dataset_<namespace>_<dataset-name-with-dashes-as-underscores>`.
- **Query flow**: UI (Cognito-authenticated, behind CloudFront/WAF) invokes the agent on Bedrock AgentCore. The agent does an initial top-K vector search for relevant datasets, then reasons by writing/running Python (via `strands-code-agent`'s sandboxed REPL) — calling Athena for data, joining/reshaping with pandas, and producing charts — rather than doing conventional JSON-in/JSON-out tool calls. This is why: large query results stay as in-process pandas objects and never re-enter the LLM's context window; only what the agent explicitly prints does. Two toolkits back this: `DATA_ANALYSIS_TOOLKIT`/`VISUALIZATION_TOOLKIT` (from `strands-code-agent`) and a custom `QUERY_HANDLER_TOOLKIT` that exposes `query_handler.query_dataset(dataset_id, dimension_filters)` (`agent/aws_data_analyst/data_analyst_agent.py`).

### Agent internals (`agent/aws_data_analyst/`)

- `data_analyst_agent.py` — `DataAnalystAgent`: builds the `CodeAgent`, system prompt, toolkits, and the `visualize_image`/`visualize_interactive_chart`/`search_datasets` tools. `stream_async` yields structured events (`text`, `toolUse`, `toolResult`, `result`) consumed by the service and UI.
- `data_analyst_agent_service.py` — the `BedrockAgentCoreApp` entrypoint (`invoke`). Does the *initial* dataset search itself (before constructing the agent) so the UI can show "datasets used" immediately; the agent can call `search_datasets` again mid-reasoning if the initial set is insufficient (agentic search, not one-shot RAG — see README "Design Decisions").
- `datasets_db.py` (`DatasetsDB`) — thin wrapper over the `s3vectors` boto3 client (put/query/delete vectors); embedder is swappable (`EMBEDDERS = {"nova": ..., "cohere": ...}`), selected via SSM param `vectordb_embedder`.
- `cloud_datasets.py` — `CloudDatasetLoader` (loads/caches dataset JSON metadata from S3 via `s3fs`, pre-warms a cache in a background thread) and `CloudQueryHandler` (the object the agent's generated code calls as `query_handler`; tracks per-dataset query counts/latencies for cost reporting).
- `athena_query.py` — raw Athena query execution/polling and the dataset-ID → Athena-table-name mapping.
- `infrastructure.py` — all cross-cutting config (bucket names, vector DB settings, agent ARN) is resolved from SSM `/data-analyst/*` parameters at import time. If you add new CDK-provisioned config, wire it through an SSM `StringParameter` (CDK side) + a `get_infrastructure_param` call here, matching the existing pattern.
- `bedrock_models.py` — the catalog of supported Bedrock model IDs + per-token on-demand pricing (used to compute `on_demand_cost` in agent responses). `DEFAULT_MODEL_ID` is used unless the caller passes `model_id`.
- `datasets/` — one-off scripts to fetch/preprocess the ~1,775 sample ONS/OECD datasets and upload them to S3 in the expected layout. `datasets/*/DIMENSION_LABELS`-style maps are how raw codes get expanded to human-readable labels before writing Parquet (cheap due to Parquet dictionary encoding — see README Design Decision 3).
- `evaluation/` — `llm_as_a_judge.py` scores agent answers; `load_tests.py` loads the eval query set; the two `benchmark_*.py` scripts are the entrypoints.

### Infrastructure (`infrastructure/`)

Four CDK stacks wired together in `infrastructure/app.py`:

- `DataStack` — S3 buckets (dataset data, Athena query results, access logs), Glue database/Athena workgroup, the two ingestion Lambdas + S3 event notifications, and **two** S3 Vectors indexes (prod + `-dev`, via the `cdk-s3-vectors` construct) — dev index exists for experimentation without touching prod embeddings. Publishes all cross-stack config as SSM parameters under `/data-analyst/`.
- `AgentCoreStack` — builds the agent container from `../agent` via CodeBuild/ECR and creates the Bedrock AgentCore runtime resource. Depends on `DataStack`.
- `WebAppStack` — Cognito user pool, S3 bucket + CloudFront distribution for the React build. Depends on `AgentCoreStack` and `WafStack` (cross-region reference).
- `WafStack` — AWS WAF web ACL, forced to `us-east-1` regardless of the app's primary region (CloudFront/WAF requirement).

### UI (`user-interface/src/`)

React 19 + TypeScript + Vite, Cloudscape Design components. `auth/` (Cognito via `aws-amplify`), `components/ChatPane.tsx` (main chat UI, renders streamed agent events) and `InteractiveChart.tsx` (Plotly rendering for `visualize_interactive_chart` events), `services/api.ts` (talks to the AgentCore runtime — directly via `@aws-sdk/client-bedrock-agentcore` in the deployed setup, or the local agent when `VITE_USE_LOCAL_AGENT=true`). No test suite or linter is configured for the UI.
