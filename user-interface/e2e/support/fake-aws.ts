import type { Page, Request, Route } from '@playwright/test';
import { randomBytes } from 'node:crypto';
import { AGENT_RUNTIME_ARN, CLIENT_ID, IDENTITY_POOL_ID, REGION, USER_POOL_ID } from './constants';
import { toSse } from './agent-events';

// In-browser fakes for the AWS endpoints used by the production web-app build:
// Cognito User Pools (Amplify SRP sign-in), Cognito Identity (temporary credentials)
// and Bedrock AgentCore InvokeAgentRuntime (SigV4-signed by the AWS SDK).
// They let the real aws-amplify and @aws-sdk/client-bedrock-agentcore code run unmodified.

export const VALID_USER = { username: 'e2e-user', password: 'Correct-Password-1' };
export const NEW_PASSWORD = 'Brand-New-Password-2';

const CORS_HEADERS = {
  'access-control-allow-origin': '*',
  'access-control-allow-headers': '*',
  'access-control-allow-methods': 'GET,POST,OPTIONS',
  'access-control-expose-headers': '*',
};

const b64url = (obj: object) => Buffer.from(JSON.stringify(obj)).toString('base64url');
const fakeJwt = (payload: object) => `${b64url({ alg: 'RS256', kid: 'e2e' })}.${b64url(payload)}.ZmFrZS1zaWduYXR1cmU`;

function tokens(username: string) {
  const now = Math.floor(Date.now() / 1000);
  const common = { sub: '11111111-2222-4333-8444-555555555555', iss: `https://cognito-idp.${REGION}.amazonaws.com/${USER_POOL_ID}`, iat: now, auth_time: now, exp: now + 3600 };
  return {
    AccessToken: fakeJwt({ ...common, token_use: 'access', client_id: CLIENT_ID, username, scope: 'aws.cognito.signin.user.admin' }),
    IdToken: fakeJwt({ ...common, token_use: 'id', aud: CLIENT_ID, 'cognito:username': username }),
    RefreshToken: 'fake-refresh-token',
    ExpiresIn: 3600,
    TokenType: 'Bearer',
  };
}

export interface FakeAwsOptions {
  /** Return NEW_PASSWORD_REQUIRED after the SRP password check, like a freshly created Cognito user. */
  requireNewPassword?: boolean;
  /** SSE events streamed back by InvokeAgentRuntime. */
  agentEvents?: () => object[];
}

export interface AwsCall {
  service: 'cognito-idp' | 'cognito-identity' | 'bedrock-agentcore';
  operation: string;
  headers: Record<string, string>;
  url: string;
  body: any;
}

async function fulfillJson(route: Route, status: number, body: object, extraHeaders: Record<string, string> = {}) {
  await route.fulfill({
    status,
    headers: { ...CORS_HEADERS, 'content-type': 'application/x-amz-json-1.1', ...extraHeaders },
    body: JSON.stringify(body),
  });
}

const parseBody = (request: Request) => {
  try { return request.postDataJSON(); } catch { return request.postData(); }
};

export async function installFakeAws(page: Page, options: FakeAwsOptions = {}): Promise<AwsCall[]> {
  const calls: AwsCall[] = [];
  const record = (service: AwsCall['service'], operation: string, request: Request) => {
    calls.push({ service, operation, headers: request.headers(), url: request.url(), body: parseBody(request) });
  };

  // --- Cognito User Pools (InitiateAuth / RespondToAuthChallenge)
  await page.route(`https://cognito-idp.${REGION}.amazonaws.com/**`, async (route) => {
    const request = route.request();
    if (request.method() === 'OPTIONS') return route.fulfill({ status: 204, headers: CORS_HEADERS });
    const operation = (request.headers()['x-amz-target'] || '').split('.').pop() || '';
    record('cognito-idp', operation, request);
    const body = request.postDataJSON();

    if (operation === 'InitiateAuth') {
      if (body.AuthParameters?.USERNAME !== VALID_USER.username) {
        return fulfillJson(route, 400, { __type: 'NotAuthorizedException', message: 'Incorrect username or password.' },
          { 'x-amzn-errortype': 'NotAuthorizedException:' });
      }
      return fulfillJson(route, 200, {
        ChallengeName: 'PASSWORD_VERIFIER',
        ChallengeParameters: {
          SALT: randomBytes(16).toString('hex'),
          SECRET_BLOCK: randomBytes(64).toString('base64'),
          SRP_B: 'f' + randomBytes(255).toString('hex'),
          USERNAME: VALID_USER.username,
          USER_ID_FOR_SRP: VALID_USER.username,
        },
      });
    }
    if (operation === 'RespondToAuthChallenge' && body.ChallengeName === 'PASSWORD_VERIFIER') {
      if (options.requireNewPassword) {
        return fulfillJson(route, 200, {
          ChallengeName: 'NEW_PASSWORD_REQUIRED',
          Session: 'fake-session',
          ChallengeParameters: { USER_ID_FOR_SRP: VALID_USER.username, requiredAttributes: '[]', userAttributes: '{}' },
        });
      }
      return fulfillJson(route, 200, { AuthenticationResult: tokens(VALID_USER.username), ChallengeParameters: {} });
    }
    if (operation === 'RespondToAuthChallenge' && body.ChallengeName === 'NEW_PASSWORD_REQUIRED') {
      return fulfillJson(route, 200, { AuthenticationResult: tokens(VALID_USER.username), ChallengeParameters: {} });
    }
    return fulfillJson(route, 400, { __type: 'InvalidParameterException', message: `Unexpected fake Cognito call ${operation}` });
  });

  // --- Cognito Identity (GetId / GetCredentialsForIdentity)
  await page.route(`https://cognito-identity.${REGION}.amazonaws.com/**`, async (route) => {
    const request = route.request();
    if (request.method() === 'OPTIONS') return route.fulfill({ status: 204, headers: CORS_HEADERS });
    const operation = (request.headers()['x-amz-target'] || '').split('.').pop() || '';
    record('cognito-identity', operation, request);
    const identityId = `${REGION}:99999999-8888-4777-8666-555555555555`;
    if (operation === 'GetId') return fulfillJson(route, 200, { IdentityId: identityId });
    if (operation === 'GetCredentialsForIdentity') {
      return fulfillJson(route, 200, {
        IdentityId: identityId,
        Credentials: {
          AccessKeyId: 'ASIAE2ETESTACCESSKEY',
          SecretKey: 'e2e-test-secret-key',
          SessionToken: 'e2e-test-session-token',
          Expiration: Math.floor(Date.now() / 1000) + 3600,
        },
      });
    }
    return fulfillJson(route, 400, { __type: 'InvalidParameterException', message: `Unexpected fake Identity call ${operation}` });
  });

  // --- Bedrock AgentCore Runtime (InvokeAgentRuntime)
  await page.route(`https://bedrock-agentcore.${REGION}.amazonaws.com/**`, async (route) => {
    const request = route.request();
    if (request.method() === 'OPTIONS') return route.fulfill({ status: 204, headers: CORS_HEADERS });
    const path = decodeURIComponent(new URL(request.url()).pathname);
    const operation = path === `/runtimes/${AGENT_RUNTIME_ARN}/invocations` ? 'InvokeAgentRuntime' : `Unknown ${path}`;
    calls.push({ service: 'bedrock-agentcore', operation, headers: request.headers(), url: request.url(), body: JSON.parse(request.postDataBuffer()?.toString() || '{}') });
    if (operation !== 'InvokeAgentRuntime') {
      return route.fulfill({ status: 404, headers: { ...CORS_HEADERS, 'content-type': 'application/json' }, body: '{"message":"not found"}' });
    }
    await route.fulfill({
      status: 200,
      headers: { ...CORS_HEADERS, 'content-type': 'text/event-stream', 'x-amzn-bedrock-agentcore-runtime-session-id': request.headers()['x-amzn-bedrock-agentcore-runtime-session-id'] || '' },
      body: toSse((options.agentEvents ?? (() => []))()),
    });
  });

  return calls;
}
