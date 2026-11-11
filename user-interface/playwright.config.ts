import { defineConfig, devices, type PlaywrightTestConfig } from '@playwright/test';
import { AGENT_RUNTIME_ARN, CLIENT_ID, IDENTITY_POOL_ID, PORTS, REGION, USER_POOL_ID } from './e2e/support/constants';

// End-to-end tests for the web UI, run against production builds (`vite build` + `vite preview`).
//
//   npm run test:e2e                                      # production-build + local-agent projects
//   E2E_AGENT_PYTHON=/path/to/python npm run test:e2e     # + full-stack project (real agent service)
//
// E2E_AGENT_PYTHON must be a Python with agent/requirements.txt and agent/requirements-test.txt installed.

const CI = !!process.env.CI;
const AGENT_PYTHON = process.env.E2E_AGENT_PYTHON;

// Every VITE_* variable is set explicitly so a developer's .env.local cannot leak into the builds.
const buildEnv = (useLocalAgent: boolean) => ({
  ...(process.env as Record<string, string>),
  BROWSER: 'none', // never open a browser from `vite preview`
  VITE_USE_LOCAL_AGENT: String(useLocalAgent),
  VITE_AWS_REGION: REGION,
  VITE_COGNITO_USER_POOL_ID: USER_POOL_ID,
  VITE_COGNITO_CLIENT_ID: CLIENT_ID,
  VITE_COGNITO_IDENTITY_POOL_ID: IDENTITY_POOL_ID,
  VITE_AGENT_RUNTIME_ARN: AGENT_RUNTIME_ARN,
});

type WebServer = Extract<NonNullable<PlaywrightTestConfig['webServer']>, { command: string }>;

const previewServer = (name: string, port: number, useLocalAgent: boolean): WebServer => ({
  command: `npx vite build --outDir e2e/.dist/${name} --emptyOutDir --logLevel warn && npx vite preview --outDir e2e/.dist/${name} --host 127.0.0.1 --port ${port} --strictPort`,
  url: `http://127.0.0.1:${port}`,
  env: buildEnv(useLocalAgent),
  reuseExistingServer: !CI,
  timeout: 180_000,
  stdout: 'pipe',
});

const webServer: WebServer[] = [
  previewServer('production', PORTS.cognito, false),
  previewServer('local-agent', PORTS.localAgent, true),
];
if (AGENT_PYTHON) {
  webServer.push({
    command: `"${AGENT_PYTHON}" tests/e2e_agent_server.py`,
    cwd: '../agent',
    url: `http://127.0.0.1:${PORTS.agent}/ping`,
    env: { ...(process.env as Record<string, string>), E2E_AGENT_PORT: String(PORTS.agent) },
    reuseExistingServer: !CI,
    timeout: 180_000,
    stdout: 'pipe',
  });
}

export default defineConfig({
  testDir: './e2e',
  outputDir: './test-results',
  fullyParallel: true,
  forbidOnly: CI,
  retries: CI ? 1 : 0,
  timeout: 90_000,
  expect: { timeout: 15_000 },
  reporter: CI
    ? [['github'], ['list'], ['html', { open: 'never', outputFolder: 'playwright-report' }]]
    : [['list'], ['html', { open: 'never', outputFolder: 'playwright-report' }]],
  use: {
    ...devices['Desktop Chrome'],
    trace: 'retain-on-failure',
    screenshot: 'only-on-failure',
    video: 'retain-on-failure',
  },
  projects: [
    {
      name: 'production-build',
      testMatch: 'production-build.spec.ts',
      use: { baseURL: `http://127.0.0.1:${PORTS.cognito}` },
    },
    {
      name: 'local-agent',
      testMatch: 'chat.spec.ts',
      use: { baseURL: `http://127.0.0.1:${PORTS.localAgent}` },
    },
    ...(AGENT_PYTHON ? [{
      name: 'full-stack',
      testMatch: 'full-stack.spec.ts',
      use: { baseURL: `http://127.0.0.1:${PORTS.localAgent}` },
    }] : []),
  ],
  webServer,
});
