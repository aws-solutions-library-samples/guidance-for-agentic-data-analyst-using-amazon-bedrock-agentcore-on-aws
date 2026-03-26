import { useState, useEffect, useRef } from 'react';
import {
  Container,
  Header,
  SpaceBetween,
  Button,
  Textarea,
  Box,
  Spinner,
} from '@cloudscape-design/components';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';
import { vscDarkPlus } from 'react-syntax-highlighter/dist/esm/styles/prism';
import type { Message } from '../types';
import { streamAgentInvoke } from '../services/api';
import { useAuth } from '../auth';
import { useReactToPrint } from 'react-to-print';
import { v4 as uuidv4 } from 'uuid';

const CodeBlock = ({ inline, className, children, ...props }: { inline?: boolean; className?: string; children?: React.ReactNode }) => {
  const match = /language-(\w+)/.exec(className || '');
  return !inline && match ? (
    <SyntaxHighlighter style={vscDarkPlus} language={match[1]} PreTag="div" {...props}>{String(children).replace(/\n$/, '')}</SyntaxHighlighter>
  ) : (
    <code className={className} {...props}>{children}</code>
  );
};

export function ChatPane() {
  const { signOut } = useAuth();
  const [sessionId, setSessionId] = useState(uuidv4());
  const [messages, setMessages] = useState<Message[]>([]);
  const [history, setHistory] = useState<Array<[string, string]>>([]);
  const [userInput, setUserInput] = useState('');
  const [isProcessing, setIsProcessing] = useState(false);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const printRef = useRef<HTMLDivElement>(null);

  const handlePrint = useReactToPrint({
    contentRef: printRef,
    documentTitle: `chat-report-${sessionId.slice(0, 8)}`,
  });

  useEffect(() => { messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' }); }, [messages]);

  const handleReset = () => {
    setSessionId(uuidv4());
    setMessages([]);
    setHistory([]);
    setUserInput('');
    setIsProcessing(false);
  };

  const sendMessage = async () => {
    if (!userInput.trim() || isProcessing) return;
    const prompt = userInput;
    setUserInput('');
    setIsProcessing(true);
    setMessages(prev => [...prev, { role: 'user', content: prompt }]);
    const newHistory: Array<[string, string]> = [...history];

    try {
      for await (const event of streamAgentInvoke(prompt, sessionId, history)) {
        if (event.type === 'image' && event.content) {
          setMessages(prev => [...prev, { role: 'assistant', content: '', images: [event.content!] }]);
        } else if (event.type === 'text' && event.content) {
          setMessages(prev => [...prev, { role: 'assistant', content: event.content! }]);
          newHistory.push(['assistant', event.content!]);
        } else if (event.type === 'python_code' && event.content) {
          setMessages(prev => [...prev, { role: 'assistant', content: '```python\n' + event.content!.trim() + '\n```' }]);
          newHistory.push(['assistant', `python_repl Tool:\n${event.content}`]);
        } else if (event.type === 'execution_output' && event.content) {
          setMessages(prev => [...prev, { role: 'assistant', content: '```\n' + event.content!.trim() + '\n```' }]);
          newHistory.push(['assistant', `Tool Output:\n${event.content}`]);
        } else if (event.type === 'result' && event.content) {
          setMessages(prev => [...prev, { role: 'assistant', content: event.content! }]);
        } else if (event.type === 'error' && event.content) {
          setMessages(prev => [...prev, { role: 'assistant', content: `Error: ${event.content}` }]);
        }
      }
      setHistory(newHistory);
    } catch (error) {
      setMessages(prev => [...prev, { role: 'assistant', content: `Error: ${error}` }]);
    } finally {
      setIsProcessing(false);
    }
  };

  return (
    <Box padding="l">
      <Container
        header={
          <Header variant="h1" actions={
            <SpaceBetween direction="horizontal" size="xs">
              <Button onClick={() => handlePrint()} disabled={messages.length === 0}>Print</Button>
              <Button onClick={handleReset}>New Chat</Button>
              <Button onClick={signOut}>Logout</Button>
            </SpaceBetween>
          }>
            Data Analyst Agent
          </Header>
        }
      >
        <SpaceBetween size="m">
          <Box color="text-status-inactive" fontSize="body-s">
            Ask questions about the 1,775 OECD and ONS datasets. For example:
            <ul>
              <li><em>"Graph the employment rate through the years"</em></li>
              <li><em>"How has the gender wage gap evolved relative to female educational attainment gains?"</em></li>
              <li><em>"When was the highest inflation rate in the UK?"</em></li>
            </ul>
          </Box>

          <div style={{ maxHeight: 'calc(100vh - 300px)', overflowY: 'auto' }}>
            {messages.length === 0 ? (
              <Box color="text-status-inactive" padding="l" textAlign="center">Start a conversation...</Box>
            ) : (
              <SpaceBetween size="s">
                {messages.map((msg, i) => (
                  <div key={i}>
                    <Box fontWeight="bold">{msg.role === 'user' ? 'You' : 'Assistant'}</Box>
                    <Box variant="div" padding={{ left: 's' }}>
                      <ReactMarkdown remarkPlugins={[remarkGfm]} components={{ code: CodeBlock }}>{msg.content}</ReactMarkdown>
                      {msg.images?.map((img, idx) => (
                        <img key={idx} src={`data:image/png;base64,${img}`} alt={`Chart ${idx + 1}`} style={{ maxWidth: '100%', marginTop: 8, borderRadius: 4 }} />
                      ))}
                    </Box>
                  </div>
                ))}
              </SpaceBetween>
            )}
            {isProcessing && (
              <Box padding={{ top: 's' }}>
                <Box fontWeight="bold">Assistant <Spinner /></Box>
                <Box padding={{ left: 's' }}>Processing...</Box>
              </Box>
            )}
            <div ref={messagesEndRef} />
          </div>

          <Textarea
            value={userInput}
            onChange={({ detail }) => setUserInput(detail.value)}
            placeholder="Enter your question here..."
            disabled={isProcessing}
            onKeyDown={(e) => e.detail.key === 'Enter' && !e.detail.shiftKey && sendMessage()}
          />

          <SpaceBetween direction="horizontal" size="xs">
            <Button variant="primary" onClick={sendMessage} disabled={isProcessing || !userInput.trim()}>
              {isProcessing ? <Spinner /> : 'Send'}
            </Button>
          </SpaceBetween>

          {/* Hidden printable content */}
          <div style={{ display: 'none' }}>
            <div ref={printRef} className="print-report">
              <style>{`
                @media print {
                  .print-report { padding: 20px; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; }
                  .print-report h2 { margin-bottom: 4px; }
                  .print-report .session-id { color: #666; font-size: 12px; margin-bottom: 16px; }
                  .print-report .message { margin-bottom: 12px; page-break-inside: avoid; }
                  .print-report .role { font-weight: bold; margin-bottom: 4px; }
                  .print-report .content { padding-left: 12px; }
                  .print-report img { max-width: 100%; page-break-inside: avoid; }
                  .print-report pre { page-break-inside: avoid; }
                }
              `}</style>
              <h2>Chat Report</h2>
              <div className="session-id">Session: {sessionId}</div>
              {messages.map((msg, i) => (
                <div key={i} className="message">
                  <div className="role">{msg.role === 'user' ? 'You' : 'Assistant'}</div>
                  <div className="content">
                    <ReactMarkdown remarkPlugins={[remarkGfm]} components={{ code: CodeBlock }}>{msg.content}</ReactMarkdown>
                    {msg.images?.map((img, idx) => (
                      <img key={idx} src={`data:image/png;base64,${img}`} alt={`Chart ${idx + 1}`} style={{ maxWidth: '100%' }} />
                    ))}
                  </div>
                </div>
              ))}
            </div>
          </div>

          <Box variant="small" color="text-status-inactive">Session: {sessionId.slice(0, 8)}...</Box>
        </SpaceBetween>
      </Container>
    </Box>
  );
}
