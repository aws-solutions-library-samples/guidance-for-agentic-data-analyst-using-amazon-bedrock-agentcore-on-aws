import type { StreamEvent } from '../types';
import { fetchAuthSession } from 'aws-amplify/auth';
import { BedrockAgentCoreClient, InvokeAgentRuntimeCommand } from '@aws-sdk/client-bedrock-agentcore';

const AGENT_RUNTIME_ARN = process.env.REACT_APP_AGENT_RUNTIME_ARN || '';
const REGION = process.env.REACT_APP_AWS_REGION || 'us-east-1';

export async function* streamAgentInvoke(
  prompt: string,
  sessionId: string,
  history?: Array<[string, string]>,
): AsyncGenerator<StreamEvent> {
  const payload: { message: string; session_history?: Array<[string, string]> } = { message: prompt };
  if (history) payload.session_history = history;

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

  const reader = stream.transformToWebStream().getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  const toolNames: Record<string, string> = {};

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
          // Initial dataset retrieval results
          const entries = event.datasets?.entries;
          if (entries?.length) {
            const text = entries.map((d: any) =>
              `* 📊 [${d.key}](https://www.ons.gov.uk/datasets/${d.key}): ${d.title}`
            ).join('\n');
            yield { type: 'text', content: `Relevant ONS Datasets:\n${text}` };
          }

        } else if (event.msg_type === 'message') {
          // Unwrap message.content array (contains text, toolUse, toolResult items)
          for (const item of event.message?.content || []) {
            if (item.text) {
              yield { type: 'text', content: item.text };
            } else if (item.toolUse) {
              const tu = item.toolUse;
              toolNames[tu.toolUseId] = tu.name;
              if (tu.name === 'python_repl') {
                yield { type: 'python_code', content: tu.input.code };
              } else if (tu.name === 'search_datasets') {
                yield { type: 'text', content: `🔍 Searching datasets for: "${tu.input.query}"` };
              }
            } else if (item.toolResult) {
              const tr = item.toolResult;
              const name = toolNames[tr.toolUseId] || '';
              if (name === 'python_repl') {
                for (const r of tr.content || []) {
                  if (r.text) yield { type: 'execution_output', content: r.text };
                }
              } else if (name === 'search_datasets') {
                const text = (tr.content || []).map((r: any) => r.text).filter(Boolean).join('\n');
                if (text) yield { type: 'text', content: text };
              }
            }
          }

        } else if (event.msg_type === 'result') {
          // Final result with answer, visualization, and metrics
          const result = event.result;
          if (result?.answer) {
            yield { type: 'text', content: result.answer };
          }
          if (result?.visualization) {
            yield { type: 'image', content: result.visualization };
          }
          if (result?.metrics?.agent) {
            const m = result.metrics.agent;
            let metricsText = `Metrics:\n * ⏱️ Latency: ${m.total_duration?.toFixed(0)}s\n * 💸 On demand cost: $${m.on_demand_cost?.toFixed(2)}\n * 🔄 Cycles: ${m.total_cycles}`;
            if (result.metrics.query?.datasets?.length) {
              metricsText += '\n\nUsed Datasets:\n' + result.metrics.query.datasets.map(
                (d: any) => `* 📊 [${d.key}](https://www.ons.gov.uk/datasets/${d.key}): ${d.title}`
              ).join('\n');
            }
            yield { type: 'result', content: metricsText };
          }
        }
      } catch {}
    }
  }
}
