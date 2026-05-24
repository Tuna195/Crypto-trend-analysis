// src/main.jsx — Entry point
import React from "react";
import ReactDOM from "react-dom/client";
import Dashboard from "./components/Dashboard";

// Reset CSS tối thiểu
const globalStyle = document.createElement("style");
globalStyle.textContent = `
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: #0a0d14; overflow-x: hidden; }
  ::-webkit-scrollbar { width: 4px; height: 4px; }
  ::-webkit-scrollbar-track { background: #111620; }
  ::-webkit-scrollbar-thumb { background: #1e2a40; border-radius: 2px; }
  a { color: inherit; }
`;
document.head.appendChild(globalStyle);

ReactDOM.createRoot(document.getElementById("root")).render(
  <React.StrictMode>
    <Dashboard />
  </React.StrictMode>
);
