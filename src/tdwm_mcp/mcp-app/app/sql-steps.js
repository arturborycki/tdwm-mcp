// Visual EXPLAIN bundle: renders a Teradata query plan as an echarts graph.
//
// Expected structuredContent (produced by the visualize_sql_steps tool in PR5):
//   {
//     title: "Visual EXPLAIN — session 12345",
//     nodes: [{
//       id:           "1",
//       name:         "Step 1",                     // short label rendered on the node
//       category:     "HIGH" | "LOW" | "NO" | "JOIN" | "UNKNOWN",
//       value:        12345,                         // estimated rows; drives symbolSize
//       tooltip:      "...",                         // pre-formatted multi-line string
//       est_rows?:    12345,
//       act_rows?:    12000,
//       est_ms?:      120,
//       act_ms?:      95,
//       step_text?:   "..."                          // raw SQL step text
//     }, ...],
//     links: [{ source: "1", target: "2" }, ...],
//     meta?: { total_steps: 14, est_elapsed_ms: 8200, ... }
//   }
//
// Categories map to colors and to legend entries. Force layout keeps the chain
// readable for typical < 30-step plans without a dagre dependency.

const toolbar = document.getElementById("toolbar");
const chartDiv = document.getElementById("chart");
const statusEl = document.getElementById("status");

const CONFIDENCE_ORDER = ["HIGH", "LOW", "NO", "JOIN", "UNKNOWN"];

// Soft palette that reads in both light and dark; echarts theme override
// recolors them when the host flips to dark.
const CATEGORY_COLORS = {
  HIGH:    "#3b82f6", // blue
  LOW:     "#f59e0b", // amber
  NO:      "#ef4444", // red
  JOIN:    "#10b981", // emerald
  UNKNOWN: "#94a3b8", // slate
};

let chart = null;
let currentTheme = null; // null | "dark"
let state = {
  title: "",
  nodes: [],
  links: [],
  meta: null,
};

function setStatus(msg) {
  if (statusEl) statusEl.textContent = msg;
}

function ensureChart() {
  if (!chart) {
    chart = echarts.init(chartDiv, currentTheme);
    window.addEventListener("resize", () => chart && chart.resize());
    // TODO(PR5+): node-click → host callback to fetch full SQL step text
    // when the API surface stabilizes in @modelcontextprotocol/ext-apps.
    chart.on("click", (params) => {
      if (params && params.dataType === "node") {
        setStatus(`Selected: ${params.data && params.data.name} (id=${params.data && params.data.id})`);
      }
    });
  }
  return chart;
}

function buildToolbar() {
  toolbar.innerHTML = "";
  if (state.title) {
    const t = document.createElement("strong");
    t.textContent = state.title;
    t.style.marginRight = "12px";
    toolbar.appendChild(t);
  }
  const fit = document.createElement("button");
  fit.type = "button";
  fit.textContent = "Fit view";
  fit.addEventListener("click", () => {
    if (chart) chart.dispatchAction({ type: "restore" });
  });
  toolbar.appendChild(fit);
}

// Pure: turn the structuredContent into an echarts option object. Tested in Node.
function buildOption(payload) {
  const nodes = Array.isArray(payload && payload.nodes) ? payload.nodes : [];
  const links = Array.isArray(payload && payload.links) ? payload.links : [];
  if (nodes.length === 0) {
    return { title: { text: "No EXPLAIN steps", left: "center", top: "middle" } };
  }

  const categories = CONFIDENCE_ORDER.map((name) => ({
    name,
    itemStyle: { color: CATEGORY_COLORS[name] },
  }));
  const catIndex = new Map(CONFIDENCE_ORDER.map((name, i) => [name, i]));

  const values = nodes.map((n) => (typeof n.value === "number" ? n.value : 0));
  const maxVal = Math.max(1, ...values);
  const minVal = Math.min(...values.filter((v) => v > 0), maxVal);
  const symbolSize = (v) => {
    if (!Number.isFinite(v) || v <= 0) return 18;
    // Log-scale so large rows don't dwarf the rest.
    const t = Math.log10(v + 1) / Math.log10(maxVal + 1);
    return Math.round(18 + 36 * (t || 0));
  };

  const renderNode = (n) => {
    const rawCat = (n.category || "UNKNOWN").toUpperCase();
    const ci = catIndex.has(rawCat) ? catIndex.get(rawCat) : catIndex.get("UNKNOWN");
    const v = typeof n.value === "number" ? n.value : 0;
    return {
      id: String(n.id),
      name: n.name || `Step ${n.id}`,
      value: v,
      category: ci,
      symbolSize: symbolSize(v),
      label: { show: true, fontSize: 11 },
      // Echarts tooltip formatter can be a string template referencing {b}/{c},
      // but multi-line node-specific text is easier via per-node tooltip.formatter.
      tooltip: n.tooltip ? { formatter: escapeTooltip(n.tooltip) } : undefined,
      raw: n, // kept for click handler / future host callbacks
    };
  };

  const renderLink = (l) => ({
    source: String(l.source),
    target: String(l.target),
    lineStyle: { opacity: 0.6, curveness: 0.05 },
    symbol: ["none", "arrow"],
    symbolSize: [6, 9],
  });

  return {
    tooltip: { trigger: "item", confine: true },
    legend: [{ data: CONFIDENCE_ORDER, top: 0, type: "scroll" }],
    series: [{
      type: "graph",
      layout: "force",
      roam: true,
      draggable: true,
      categories,
      data: nodes.map(renderNode),
      links: links.map(renderLink),
      force: {
        // Tuned for short-to-medium chains. Repulsion grows with node count.
        repulsion: Math.max(140, 40 * Math.min(nodes.length, 30)),
        edgeLength: [60, 120],
        gravity: 0.04,
        layoutAnimation: nodes.length <= 60,
      },
      edgeSymbol: ["none", "arrow"],
      emphasis: { focus: "adjacency", lineStyle: { width: 3 } },
      labelLayout: { hideOverlap: true },
    }],
  };
}

// Escape HTML in tool-supplied tooltip text. Echarts renders the formatter
// string as HTML, so unescaped angle brackets / ampersands would break it.
function escapeTooltip(s) {
  return String(s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\n/g, "<br/>");
}

function render() {
  buildToolbar();
  const inst = ensureChart();
  inst.setOption(buildOption(state), true);
  if (state.nodes.length === 0) {
    setStatus("Plan was empty.");
  } else {
    const m = state.meta || {};
    const tail = m.est_elapsed_ms ? ` · est ${Number(m.est_elapsed_ms).toFixed(0)} ms` : "";
    setStatus(`${state.nodes.length} steps · ${state.links.length} edges${tail}`);
  }
}

function ingest(structured) {
  state.title = (structured && structured.title) || "";
  state.nodes = Array.isArray(structured && structured.nodes) ? structured.nodes : [];
  state.links = Array.isArray(structured && structured.links) ? structured.links : [];
  state.meta = structured && structured.meta ? structured.meta : null;
}

function showText(text) {
  toolbar.innerHTML = "";
  chartDiv.innerHTML = "";
  const pre = document.createElement("pre");
  pre.style.cssText = "margin:12px;padding:12px;background:rgba(127,127,127,0.08);border-radius:6px;overflow:auto;font:12px/1.4 ui-monospace,Menlo,monospace;";
  pre.textContent = text;
  chartDiv.appendChild(pre);
}

// Exposed for Node-side smoke tests; harmless in-browser. The bundle is
// concatenated inside a module body, so this assignment doesn't leak globals
// to the host iframe — `globalThis.__tdwmSqlSteps__` is module-scoped to
// scripts in the same module load.
globalThis.__tdwmSqlSteps__ = { buildOption, escapeTooltip };

const app = new App();

app.ontoolinput = () => setStatus("Tool invoked, awaiting result…");

app.ontoolresult = (result) => {
  const structured = result && result.structuredContent;
  if (structured && (Array.isArray(structured.nodes) || Array.isArray(structured.data))) {
    // Be lenient if the shaper accidentally uses `data` (chart bundle shape).
    const payload = Array.isArray(structured.nodes)
      ? structured
      : { ...structured, nodes: structured.data, links: structured.links || [] };
    ingest(payload);
    render();
    return;
  }
  const text = result && result.content && result.content[0] && result.content[0].text;
  if (text) {
    showText(text);
    setStatus("Text-only result; no graph to render.");
    return;
  }
  setStatus("No payload.");
};

app.onhostcontextchanged = (ctx) => {
  applyHostTheme(ctx);
  const newEchartsTheme = hostEchartsTheme(ctx);
  if (newEchartsTheme !== currentTheme) {
    currentTheme = newEchartsTheme;
    if (chart) {
      chart.dispose();
      chart = null;
    }
    if (state.nodes.length > 0) render();
  }
};

app.onteardown = () => {
  if (chart) {
    chart.dispose();
    chart = null;
  }
  setStatus("Torn down.");
};

await app.connect();
setStatus("Connected. Awaiting tool result…");
