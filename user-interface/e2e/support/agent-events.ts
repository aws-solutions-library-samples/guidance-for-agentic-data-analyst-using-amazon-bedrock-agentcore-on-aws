// Agent SSE events, shaped exactly like the stream emitted by the agent service
// (`aws_data_analyst/data_analyst_agent.py`), as asserted by the agent tests in
// `agent/tests/test_agent_service.py`. The full-stack project checks the same UI
// behaviour against the real service, so drift between this file and the agent is caught.

// 1x1 transparent PNG.
export const PNG_BASE64 =
  'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg==';

export const DATASET = {
  key: 'ons.labour-market-test',
  title: 'Labour market test dataset',
  url: 'https://example.com/datasets/labour-market-test',
};

export const PYTHON_CODE = "df = query_handler.query_dataset('ons.labour-market-test')\nprint(f\"ROWS={len(df)} MAX={df['observation'].max()}\")";
export const PYTHON_OUTPUT = 'STDOUT:\nROWS=3 MAX=5.1';
export const ANSWER = 'The unemployment rate peaked at **5.1%** in 2021.';

const PLOTLY_FIGURE = {
  data: [{ type: 'scatter', mode: 'lines', x: ['2020', '2021', '2022'], y: [4.5, 5.1, 3.9], name: 'observation' }],
  layout: { title: { text: 'Unemployment rate' } },
};

export const analysisEvents = (): object[] => [
  { msg_type: 'datasets', datasets: { entries: [{ ...DATASET, distance: 0.1, metadata: {} }], metrics: {} } },
  { msg_type: 'text', text: 'Let me look for more datasets.' },
  { msg_type: 'toolUse', toolUseId: 't1', name: 'search_datasets', input: { query: 'unemployment rate' } },
  {
    msg_type: 'toolResult', toolUseId: 't1', name: 'search_datasets', status: 'success',
    content: [{ text: `Datasets:\n\n# ID [${DATASET.key}]\nONS Dataset - Labour market` }],
  },
  { msg_type: 'toolUse', toolUseId: 't2', name: 'python_repl', input: { code: PYTHON_CODE } },
  { msg_type: 'toolResult', toolUseId: 't2', name: 'python_repl', status: 'success', content: [{ text: PYTHON_OUTPUT }] },
  {
    msg_type: 'toolUse', toolUseId: 't3', name: 'visualize_interactive_chart',
    input: { chart_path: '/tmp/chart.json', chart_type: 'plotly' }, chart_spec: JSON.stringify(PLOTLY_FIGURE),
  },
  { msg_type: 'toolUse', toolUseId: 't4', name: 'visualize_image', input: { image_path: '/tmp/chart.png' }, image: PNG_BASE64 },
  {
    msg_type: 'result',
    result: {
      answer: ANSWER,
      supported_by_data: true,
      metrics: {
        agent: { total_cycles: 4, total_duration: 3.2, on_demand_cost: 0.0123 },
        query: { datasets: [{ ...DATASET, count: 1 }], latencies: [0.1] },
      },
    },
  },
];

export const errorEvents = (): object[] => [
  { error: 'Model unavailable', error_type: 'RuntimeError', message: 'Model unavailable' },
];

export const toSse = (events: object[]): string => events.map((e) => `data: ${JSON.stringify(e)}\n\n`).join('');
