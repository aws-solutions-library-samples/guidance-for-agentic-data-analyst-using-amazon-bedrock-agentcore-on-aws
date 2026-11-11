/// <reference types="vite/client" />

// Pre-built Plotly bundle, loaded lazily by InteractiveChart (no bundled typings).
declare module 'plotly.js/dist/plotly.min.js' {
  import Plotly from 'plotly.js';
  export = Plotly;
}
