export interface Message {
  role: 'user' | 'assistant';
  content: string;
  images?: string[];
}

export interface StreamEvent {
  type: 'text' | 'error' | 'image' | 'python_code' | 'execution_output' | 'result' | 'done';
  content?: string;
}
