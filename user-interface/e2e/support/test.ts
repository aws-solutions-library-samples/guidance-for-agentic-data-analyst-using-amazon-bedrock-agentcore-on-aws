import { test as base, expect, type Locator, type Page } from '@playwright/test';

// Shared fixture for all specs. Every test automatically fails if the page:
//  * throws an uncaught exception (typical symptom of a broken dependency at runtime),
//  * logs a console error not explicitly allowed by the test,
//  * sends a request outside localhost that no test route handled (keeps CI hermetic).

type Options = { allowedConsoleErrors: RegExp[] };

export const test = base.extend<Options & { pageHealth: void }>({
  allowedConsoleErrors: [[], { option: true }],

  pageHealth: [async ({ page, context, allowedConsoleErrors }, use, testInfo) => {
    const problems: string[] = [];

    page.on('pageerror', (error) => problems.push(`Uncaught exception: ${error.stack || error.message}`));
    page.on('console', (msg) => {
      if (msg.type() !== 'error') return;
      const text = msg.text();
      if (!allowedConsoleErrors.some((re) => re.test(text))) problems.push(`console.error: ${text}`);
    });

    // Page-level routes (fake AWS) take precedence over this context-level catch-all.
    await context.route((url) => !['127.0.0.1', 'localhost'].includes(url.hostname), async (route) => {
      problems.push(`Unexpected external request: ${route.request().method()} ${route.request().url()}`);
      await route.abort('blockedbyclient');
    });

    await use();

    if (problems.length) {
      await testInfo.attach('page-problems', { body: problems.join('\n\n'), contentType: 'text/plain' });
    }
    expect(problems, 'The page reported runtime problems').toEqual([]);
  }, { auto: true }],
});

export { expect };

/** The chat transcript is rendered twice (visible pane + hidden print report); only the visible copy matters. */
export const visible = (locator: Locator) => locator.filter({ visible: true });

export const questionBox = (page: Page) => page.getByPlaceholder('Enter your question here...');

export async function ask(page: Page, question: string) {
  await questionBox(page).fill(question);
  await page.getByRole('button', { name: 'Send' }).click();
}

/** Assert that every agent event type is rendered by the chat pane (shared by mocked and full-stack specs). */
export async function expectAnalysisRendered(page: Page, expected: {
  datasetKey: string; datasetUrl: string; codeSnippet: string; output: string; answer: string | RegExp;
}) {
  // Metrics are the last event: once visible, the whole stream has been consumed.
  await expect(visible(page.getByText(/On demand cost: \$\d+\.\d{2}/))).toBeVisible({ timeout: 60_000 });
  await expect(visible(page.getByText('Processing...'))).toHaveCount(0);

  // Datasets list (markdown links) from the initial retrieval and the metrics.
  await expect(visible(page.getByText('Relevant Datasets:'))).toBeVisible();
  const datasetLinks = visible(page.getByRole('link', { name: expected.datasetKey }));
  await expect(datasetLinks.first()).toHaveAttribute('href', expected.datasetUrl);
  await expect(visible(page.getByText('Used Datasets:'))).toBeVisible();

  // Agentic search tool use / result.
  await expect(visible(page.getByText(/Searching datasets for: "unemployment rate"/))).toBeVisible();
  await expect(visible(page.getByText('Datasets relevant to the Agent query:'))).toBeVisible();

  // Python code, syntax-highlighted by react-syntax-highlighter.
  await expect(visible(page.locator('code.language-python')).filter({ hasText: expected.codeSnippet })).toHaveCount(1);
  // REPL output in a fenced block.
  await expect(visible(page.locator('pre code')).filter({ hasText: expected.output })).toHaveCount(1);

  // Interactive Plotly chart actually drawn (SVG rendered, no error boundary message).
  const plot = visible(page.locator('.js-plotly-plot')).first();
  await expect(plot.locator('svg.main-svg').first()).toBeVisible({ timeout: 30_000 });
  await expect(plot.locator('.scatterlayer .trace')).toHaveCount(1);
  await expect(page.getByText(/Chart (rendering )?error/)).toHaveCount(0);

  // Static image decoded by the browser.
  const image = visible(page.getByRole('img', { name: 'Chart 1' }));
  await expect(image).toBeVisible();
  expect(await image.evaluate((img: HTMLImageElement) => img.complete && img.naturalWidth > 0)).toBe(true);

  // Final answer rendered as markdown.
  await expect(visible(page.getByText(expected.answer)).first()).toBeVisible();
}
