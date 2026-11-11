// Fake deployment configuration baked into the e2e builds (see playwright.config.ts).
// None of these resources exist: every AWS request is intercepted by fake-aws.ts.
export const REGION = 'us-east-1';
export const USER_POOL_ID = 'us-east-1_E2eTest01';
export const CLIENT_ID = 'e2etestclient00000000000000';
export const IDENTITY_POOL_ID = 'us-east-1:00000000-0000-4000-8000-000000000000';
export const AGENT_RUNTIME_ARN = 'arn:aws:bedrock-agentcore:us-east-1:123456789012:runtime/e2e_test_runtime-AbCdEf1234';

export const PORTS = {
  cognito: 4173, // production-like build: Cognito sign-in + AgentCore SDK
  localAgent: 4174, // VITE_USE_LOCAL_AGENT=true build: POST /invocations (proxied to :8080 by vite preview)
  agent: 8080, // offline agent service for the full-stack project (agent/tests/e2e_agent_server.py)
};
