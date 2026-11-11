import { test, expect, ask, expectAnalysisRendered, questionBox, visible } from './support/test';
import { installFakeAws, NEW_PASSWORD, VALID_USER } from './support/fake-aws';
import { analysisEvents, DATASET } from './support/agent-events';
import { AGENT_RUNTIME_ARN, CLIENT_ID } from './support/constants';

// Production configuration: Amplify/Cognito sign-in and the AgentCore SDK client,
// both running unmodified against in-browser fakes of the AWS endpoints.

test.describe('production build (Cognito + AgentCore SDK)', () => {
  test('renders the sign-in page', async ({ page }) => {
    await page.goto('/');
    await expect(page).toHaveTitle('Data Analyst Agent');
    await expect(page.getByRole('heading', { name: 'Data Analyst Agent' })).toBeVisible();
    await expect(page.getByLabel('Username')).toBeVisible();
    await expect(page.getByLabel('Password')).toHaveAttribute('type', 'password');
    await expect(page.getByRole('button', { name: 'Sign In' })).toBeEnabled();
  });

  test.describe('with invalid credentials', () => {
    // The fake Cognito answers 400, which Chromium always logs as a resource error.
    test.use({ allowedConsoleErrors: [/Failed to load resource: the server responded with a status of 400/] });

    test('shows an authentication error', async ({ page }) => {
      const calls = await installFakeAws(page);
      await page.goto('/');
      await page.getByLabel('Username').fill('someone-else');
      await page.getByLabel('Password').fill('wrong-password');
      await page.getByRole('button', { name: 'Sign In' }).click();

      await expect(page.getByText('Authentication Error')).toBeVisible();
      await expect(page.getByText('Invalid username or password').first()).toBeVisible();
      await expect(page.getByLabel('Username')).toBeEnabled();

      const initiate = calls.find((c) => c.operation === 'InitiateAuth');
      expect(initiate?.body).toMatchObject({ AuthFlow: 'USER_SRP_AUTH', ClientId: CLIENT_ID, AuthParameters: { USERNAME: 'someone-else' } });
    });
  });

  test('first sign-in, forced password change, signed AgentCore invocation and logout', async ({ page }) => {
    const calls = await installFakeAws(page, { requireNewPassword: true, agentEvents: analysisEvents });
    await page.goto('/');

    // Sign in with the temporary password (SRP) -> NEW_PASSWORD_REQUIRED challenge.
    await page.getByLabel('Username').fill(VALID_USER.username);
    await page.getByLabel('Password').fill(VALID_USER.password);
    await page.getByRole('button', { name: 'Sign In' }).click();
    await expect(page.getByRole('heading', { name: 'Set New Password' })).toBeVisible();

    // Password confirmation mismatch is caught client-side.
    const passwordInputs = page.locator('input[type="password"]');
    await passwordInputs.nth(0).fill(NEW_PASSWORD);
    await passwordInputs.nth(1).fill('something-else');
    await page.getByRole('button', { name: 'Set Password' }).click();
    await expect(page.getByText(/do not match/i)).toBeVisible();

    await passwordInputs.nth(1).fill(NEW_PASSWORD);
    await page.getByRole('button', { name: 'Set Password' }).click();

    // Authenticated: chat pane.
    await expect(page.getByRole('heading', { name: 'Data Analyst Agent' })).toBeVisible();
    await expect(page.getByText('Start a conversation...')).toBeVisible();

    const verifier = calls.find((c) => c.operation === 'RespondToAuthChallenge' && c.body.ChallengeName === 'PASSWORD_VERIFIER');
    expect(Object.keys(verifier?.body.ChallengeResponses ?? {})).toEqual(
      expect.arrayContaining(['USERNAME', 'PASSWORD_CLAIM_SIGNATURE', 'PASSWORD_CLAIM_SECRET_BLOCK', 'TIMESTAMP']));
    const newPassword = calls.find((c) => c.operation === 'RespondToAuthChallenge' && c.body.ChallengeName === 'NEW_PASSWORD_REQUIRED');
    expect(newPassword?.body.ChallengeResponses).toMatchObject({ NEW_PASSWORD, USERNAME: VALID_USER.username });

    // Ask a question: Cognito Identity credentials -> SigV4-signed InvokeAgentRuntime -> SSE stream.
    await ask(page, 'What was the highest unemployment rate?');
    await expectAnalysisRendered(page, {
      datasetKey: DATASET.key,
      datasetUrl: DATASET.url,
      codeSnippet: 'query_handler.query_dataset',
      output: 'ROWS=3 MAX=5.1',
      answer: /peaked at 5\.1% in 2021/,
    });

    expect(calls.filter((c) => c.service === 'cognito-identity').map((c) => c.operation))
      .toEqual(expect.arrayContaining(['GetId', 'GetCredentialsForIdentity']));
    const invoke = calls.find((c) => c.operation === 'InvokeAgentRuntime');
    expect(invoke, 'AgentCore InvokeAgentRuntime was called').toBeTruthy();
    expect(decodeURIComponent(new URL(invoke!.url).pathname)).toBe(`/runtimes/${AGENT_RUNTIME_ARN}/invocations`);
    expect(invoke!.headers['authorization']).toMatch(/^AWS4-HMAC-SHA256 Credential=ASIAE2ETESTACCESSKEY\/\d{8}\/us-east-1\/bedrock-agentcore\/aws4_request/);
    expect(invoke!.headers['x-amz-security-token']).toBe('e2e-test-session-token');
    expect(invoke!.headers['x-amzn-bedrock-agentcore-runtime-session-id']).toMatch(/^[0-9a-f-]{36}$/);
    expect(invoke!.headers['x-amz-user-agent'] ?? invoke!.headers['user-agent']).toContain('AWSSOLUTION/SO0354');
    expect(invoke!.body).toEqual({ message: 'What was the highest unemployment rate?', history: [] });

    // Logout returns to the sign-in page.
    await visible(page.getByRole('button', { name: 'Logout' })).click();
    await expect(page.getByRole('button', { name: 'Sign In' })).toBeVisible();
    await expect(questionBox(page)).toHaveCount(0);
  });
});
