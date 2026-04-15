import type { StreamEvent } from '../types';
import { fetchAuthSession } from 'aws-amplify/auth';
import { BedrockAgentCoreClient, InvokeAgentRuntimeCommand } from '@aws-sdk/client-bedrock-agentcore';

const AGENT_RUNTIME_ARN = import.meta.env.VITE_AGENT_RUNTIME_ARN || '';
const REGION = import.meta.env.VITE_AWS_REGION || 'us-east-1';
const USE_LOCAL_AGENT = import.meta.env.VITE_USE_LOCAL_AGENT === 'true';
const LOCAL_AGENT_URL = '/invocations';

export async function* streamAgentInvoke(
  prompt: string,
  sessionId: string,
  history?: Array<[string, string]>,
): AsyncGenerator<StreamEvent> {
  const payload: { message: string; history?: Array<[string, string]> } = { message: prompt };
  if (history) payload.history = history;

  let reader: ReadableStreamDefaultReader<Uint8Array>;

  if (USE_LOCAL_AGENT) {
    const response = await fetch(LOCAL_AGENT_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Accept': 'text/event-stream' },
      body: JSON.stringify({ ...payload, sessionId }),
    });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    if (!response.body) throw new Error('No response stream');
    reader = response.body.getReader();
  } else {
    const session = await fetchAuthSession();
    if (!session.credentials) throw new Error('No AWS credentials');

    const client = new BedrockAgentCoreClient({ region: REGION, credentials: session.credentials });
    const command = new InvokeAgentRuntimeCommand({
      agentRuntimeArn: AGENT_RUNTIME_ARN,
      runtimeSessionId: sessionId,
      contentType: 'application/json',
      accept: 'text/event-stream',
      payload: new TextEncoder().encode(JSON.stringify(payload)),
    });

    const response = await client.send(command);
    const stream = response.response;
    if (!stream) throw new Error('No response stream');
    reader = stream.transformToWebStream().getReader();
  }
  const decoder = new TextDecoder();
  let buffer = '';

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;

    buffer += decoder.decode(value, { stream: true });
    const lines = buffer.split('\n');
    buffer = lines.pop() || '';

    for (const line of lines) {
      if (!line.startsWith('data: ')) continue;
      try {
        const event = JSON.parse(line.slice(6));

        if (event.error) {
          yield { type: 'error', content: String(event.message || event.error) };

        } else if (event.msg_type === 'datasets') {
          const entries = event.datasets?.entries;
          if (entries?.length) {
            const text = entries.map((d: any) =>
              `* 📊 [${d.key}](${d.url}): ${d.title}`
            ).join('\n');
            yield { type: 'text', content: `Relevant Datasets:\n${text}` };
          }

        } else if (event.msg_type === 'text' && event.text) {
          yield { type: 'text', content: event.text };

        } else if (event.msg_type === 'toolUse' && event.image) {
          yield { type: 'image', content: event.image };

        } else if (event.msg_type === 'toolUse' && event.name === 'python_repl') {
          yield { type: 'python_code', content: event.input.code };

        } else if (event.msg_type === 'toolUse' && event.name === 'search_datasets') {
          yield { type: 'text', content: `🔍 Searching datasets for: "${event.input.query}"` };

        } else if (event.msg_type === 'toolResult' && event.name === 'python_repl') {
          for (const result of event.content || []) {
            if (result.text) yield { type: 'execution_output', content: result.text };
          }

        } else if (event.msg_type === 'toolResult' && event.name === 'search_datasets') {
          const text = (event.content || []).map((r: any) => r.text || '').join('\n');
          const lines = text.split('\n').filter((l: string) => l.startsWith('# ID [')).map((l: string) => l.replace('# ID', ' * 📊'));
          if (lines.length) yield { type: 'text', content: `Datasets relevant to the Agent query:\n${lines.join('\n')}` };

        } else if (event.msg_type === 'result') {
          const result = event.result;
          if (result?.answer) {
            yield { type: 'text', content: result.answer };
          }
          if (result?.metrics?.agent) {
            const m = result.metrics.agent;
            let metricsText = `Metrics:\n * ⏱️ Latency: ${m.total_duration?.toFixed(0)}s\n * 💸 On demand cost: $${m.on_demand_cost?.toFixed(2)}\n * 🔄 Cycles: ${m.total_cycles}`;
            if (result.metrics.query?.datasets?.length) {
              metricsText += '\n\nUsed Datasets:\n' + result.metrics.query.datasets.map(
                (d: any) => `* 📊 [${d.key}](${d.url}): ${d.title}`
              ).join('\n');
            }
            yield { type: 'result', content: metricsText };
          }
        }
      } catch {}
    }
  }
}
