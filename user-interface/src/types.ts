export interface Message {
  role: 'user' | 'assistant';
  content: string;
  images?: string[];
  charts?: Array<{ spec: string; type: string }>;
}

export interface StreamEvent {
  type: 'text' | 'error' | 'image' | 'python_code' | 'execution_output' | 'result' | 'done' | 'interactive_chart';
  content?: string;
  chartType?: string;
}
