import { Component, type ReactNode, useEffect, useRef, useState } from 'react';

class ChartErrorBoundary extends Component<{ children: ReactNode }, { error: string | null }> {
  state = { error: null as string | null };
  static getDerivedStateFromError(error: Error) { return { error: error.message }; }
  render() {
    if (this.state.error) return <div>Chart rendering error: {this.state.error}</div>;
    return this.props.children;
  }
}

interface InteractiveChartProps {
  spec: string;
  type: string;
}

function PlotlyChart({ spec }: { spec: string }) {
  const containerRef = useRef<HTMLDivElement>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!containerRef.current) return;
    let cancelled = false;

    import('plotly.js/dist/plotly.min.js').then((Plotly) => {
      if (cancelled || !containerRef.current) return;
      try {
        const figure = JSON.parse(spec);
        Plotly.newPlot(containerRef.current, figure.data, {
          ...figure.layout,
          autosize: true,
          margin: { l: 40, r: 20, t: 40, b: 40 },
        }, { responsive: true, displayModeBar: true });
      } catch (e: any) {
        setError(e.message);
      }
    }).catch((e: any) => setError(e.message));

    return () => {
      cancelled = true;
      if (containerRef.current) {
        import('plotly.js/dist/plotly.min.js').then((Plotly) => {
          if (containerRef.current) Plotly.purge(containerRef.current);
        });
      }
    };
  }, [spec]);

  if (error) return <div>Chart error: {error}</div>;
  return <div ref={containerRef} style={{ width: '100%', minHeight: '350px' }} />;
}

export function InteractiveChart({ spec, type }: InteractiveChartProps) {
  if (type !== 'plotly') {
    return <div>Unsupported chart type: {type}</div>;
  }

  return (
    <ChartErrorBoundary>
      <PlotlyChart spec={spec} />
    </ChartErrorBoundary>
  );
}
