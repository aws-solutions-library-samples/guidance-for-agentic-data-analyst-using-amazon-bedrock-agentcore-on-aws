import { test, ask, expectAnalysisRendered, expect, visible } from './support/test';

// Full stack: the local-agent UI build talks (through the vite preview proxy) to the
// real agent service started by agent/tests/e2e_agent_server.py, which runs offline
// with moto-emulated AWS and a scripted LLM. This catches drift between the stream
// the agent actually emits and what the UI parses.

test('answers a question end-to-end through the real agent service', async ({ page }) => {
  await page.goto('/');
  await ask(page, 'What was the highest unemployment rate?');

  await expectAnalysisRendered(page, {
    datasetKey: 'ons.labour-market-test',
    datasetUrl: 'https://example.com/datasets/labour-market-test',
    codeSnippet: "query_handler.query_dataset('ons.labour-market-test'",
    output: 'ROWS=3 MAX=5.1',
    answer: 'The unemployment rate peaked at 5.1% in 2021.',
  });
  await expect(visible(page.getByText('Let me look for more datasets.'))).toBeVisible();
  await expect(visible(page.getByText(/^Error:/))).toHaveCount(0);
});
