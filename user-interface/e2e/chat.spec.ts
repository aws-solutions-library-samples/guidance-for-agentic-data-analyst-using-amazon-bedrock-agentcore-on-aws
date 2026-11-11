import type { Page, Request } from '@playwright/test';
import { test, expect, ask, expectAnalysisRendered, questionBox, visible } from './support/test';
import { analysisEvents, DATASET, errorEvents, toSse } from './support/agent-events';

// Local-agent build (VITE_USE_LOCAL_AGENT=true): the UI POSTs to /invocations.
// The agent is mocked here to check rendering and conversation behaviour in detail.

async function mockAgent(page: Page, respond: (request: Request) => { status?: number; events?: object[] }) {
  const requests: any[] = [];
  await page.route('**/invocations', async (route) => {
    requests.push(route.request().postDataJSON());
    const { status = 200, events = [] } = respond(route.request());
    await route.fulfill({
      status,
      headers: { 'content-type': status === 200 ? 'text/event-stream' : 'application/json' },
      body: status === 200 ? toSse(events) : '{"message":"Internal Server Error"}',
    });
  });
  return requests;
}

test.describe('chat pane (local agent, mocked stream)', () => {
  test('renders the empty chat without sign-in', async ({ page }) => {
    await page.goto('/');
    await expect(page.getByRole('heading', { name: 'Data Analyst Agent' })).toBeVisible();
    await expect(page.getByText('Ask questions about the 1,775 OECD and ONS datasets.')).toBeVisible();
    await expect(page.getByText('Start a conversation...')).toBeVisible();
    await expect(page.getByRole('button', { name: 'Send' })).toBeDisabled();
    await expect(page.getByRole('button', { name: 'Print' })).toBeDisabled();
    await expect(page.getByRole('button', { name: 'Sign In' })).toHaveCount(0);
  });

  test('renders every agent event type', async ({ page }) => {
    const requests = await mockAgent(page, () => ({ events: analysisEvents() }));
    await page.goto('/');
    await ask(page, 'What was the highest unemployment rate?');

    await expect(visible(page.getByText('What was the highest unemployment rate?'))).toBeVisible();
    await expectAnalysisRendered(page, {
      datasetKey: DATASET.key,
      datasetUrl: DATASET.url,
      codeSnippet: 'query_handler.query_dataset',
      output: 'ROWS=3 MAX=5.1',
      answer: /peaked at 5\.1% in 2021/,
    });
    await expect(visible(page.locator('strong', { hasText: '5.1%' }))).toBeVisible();
    await expect(visible(page.getByText(/Latency: 3s$/))).toBeVisible();
    await expect(visible(page.getByText(/Cycles: 4$/))).toBeVisible();
    await expect(page.getByRole('button', { name: 'Print' })).toBeEnabled();

    expect(requests).toHaveLength(1);
    expect(requests[0]).toMatchObject({ message: 'What was the highest unemployment rate?', history: [] });
    expect(requests[0].sessionId).toMatch(/^[0-9a-f-]{36}$/);
  });

  test('keeps conversation history within a session and resets it on New Chat', async ({ page }) => {
    const requests = await mockAgent(page, () => ({ events: [{ msg_type: 'text', text: 'Reply from the agent' }] }));
    await page.goto('/');

    await ask(page, 'First question');
    await expect(visible(page.getByText('Reply from the agent'))).toHaveCount(1);
    await expect(questionBox(page)).toBeEnabled();

    // Enter submits (Shift+Enter would insert a newline).
    await questionBox(page).fill('Second question');
    await questionBox(page).press('Enter');
    await expect(visible(page.getByText('Reply from the agent'))).toHaveCount(2);

    expect(requests).toHaveLength(2);
    expect(requests[1].sessionId).toBe(requests[0].sessionId);
    expect(requests[1].history).toEqual([['assistant', 'Reply from the agent']]);

    await page.getByRole('button', { name: 'New Chat' }).click();
    await expect(page.getByText('Start a conversation...')).toBeVisible();

    await ask(page, 'Third question');
    await expect(visible(page.getByText('Reply from the agent'))).toHaveCount(1);
    expect(requests[2].sessionId).not.toBe(requests[0].sessionId);
    expect(requests[2].history).toEqual([]);
  });

  test('shows errors streamed by the agent', async ({ page }) => {
    await mockAgent(page, () => ({ events: errorEvents() }));
    await page.goto('/');
    await ask(page, 'Trigger an agent error');
    await expect(visible(page.getByText('Error: Model unavailable'))).toBeVisible();
    await expect(questionBox(page)).toBeEnabled();
  });

  test.describe('when the agent endpoint fails', () => {
    test.use({ allowedConsoleErrors: [/Failed to load resource: the server responded with a status of 500/] });

    test('shows the HTTP error and recovers', async ({ page }) => {
      await mockAgent(page, () => ({ status: 500 }));
      await page.goto('/');
      await ask(page, 'Trigger an HTTP error');
      await expect(visible(page.getByText('Error: Error: HTTP 500'))).toBeVisible();
      await expect(questionBox(page)).toBeEnabled();
    });
  });
});
