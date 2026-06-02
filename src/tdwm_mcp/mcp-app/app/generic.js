// Generic chart bundle for tabular tdwm-mcp tool results.
//
// Expected structuredContent shape:
//   { title?: string, data: [{...rowObj}, ...], meta?: {...} }
// Each row is a plain object; column union is computed across all rows.
//
// Renders an echarts chart with a chart-type dropdown and an X-axis selector.
// Handlers are registered before app.connect() per the Apps SDK Quickstart.

const toolbar = document.getElementById("toolbar");
const chartDiv = document.getElementById("chart");
const statusEl = document.getElementById("status");

const CHART_TYPES = ["bar", "line", "area", "pie", "scatter"];

let chart = null;        // echarts.ECharts instance
let currentTheme = null; // null | "dark"
let state = {
  title: "",
  rows: [],
  columns: [],
  xKey: null,
  yKeys: [],
  chartType: "bar",
};

function setStatus(msg) {
  if (statusEl) statusEl.textContent = msg;
}

function ensureChart() {
  if (!chart || chart.isDisposed && chart.isDisposed()) {
    chart = echarts.init(chartDiv, currentTheme);
    window.addEventListener("resize", () => chart && chart.resize());
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

  const typeSel = document.createElement("select");
  typeSel.title = "Chart type";
  for (const t of CHART_TYPES) {
    const o = document.createElement("option");
    o.value = t;
    o.textContent = t;
    if (t === state.chartType) o.selected = true;
    typeSel.appendChild(o);
  }
  typeSel.addEventListener("change", (e) => {
    state.chartType = e.target.value;
    render();
  });
  toolbar.appendChild(label("Chart", typeSel));

  if (state.columns.length > 0) {
    const xSel = document.createElement("select");
    xSel.title = "X-axis / category column";
    for (const c of state.columns) {
      const o = document.createElement("option");
      o.value = c.name;
      o.textContent = `${c.name} (${c.type})`;
      if (c.name === state.xKey) o.selected = true;
      xSel.appendChild(o);
    }
    xSel.addEventListener("change", (e) => {
      state.xKey = e.target.value;
      // Recompute y-axis defaults excluding the new x.
      state.yKeys = state.columns
        .filter((c) => c.type === "numeric" && c.name !== state.xKey)
        .map((c) => c.name);
      render();
    });
    toolbar.appendChild(label("X", xSel));
  }
}

function label(text, control) {
  const wrap = document.createElement("label");
  wrap.style.cssText = "display:inline-flex;align-items:center;gap:4px;";
  const span = document.createElement("span");
  span.textContent = text + ":";
  span.style.opacity = "0.7";
  wrap.appendChild(span);
  wrap.appendChild(control);
  return wrap;
}

function categories() {
  return state.rows.map((r) => r && r[state.xKey]);
}

function numericSeriesFor(key) {
  return state.rows.map((r) => coerceNumber(r ? r[key] : null));
}

function buildOption() {
  if (state.rows.length === 0) {
    return { title: { text: "No data", left: "center", top: "middle" } };
  }
  if (state.yKeys.length === 0 && state.chartType !== "pie") {
    return {
      title: {
        text: "No numeric columns to plot",
        subtext: `Columns detected: ${state.columns.map((c) => `${c.name}:${c.type}`).join(", ")}`,
        left: "center",
        top: "middle",
      },
    };
  }
  const base = {
    tooltip: { trigger: "axis" },
    legend: { type: "scroll", top: 0 },
    grid: { left: 50, right: 20, top: 36, bottom: 40, containLabel: true },
  };
  switch (state.chartType) {
    case "pie": return pieOption();
    case "scatter": return { ...base, ...xyOption("scatter") };
    case "line": return { ...base, ...xyOption("line", { smooth: false }) };
    case "area": return { ...base, ...xyOption("line", { smooth: false, areaStyle: {} }) };
    case "bar":
    default: return { ...base, ...xyOption("bar") };
  }
}

function xyOption(seriesType, extraSeriesProps = {}) {
  return {
    xAxis: { type: "category", data: categories(), axisLabel: { rotate: state.rows.length > 12 ? 30 : 0 } },
    yAxis: { type: "value" },
    series: state.yKeys.map((k) => ({
      name: k,
      type: seriesType,
      data: numericSeriesFor(k),
      ...extraSeriesProps,
    })),
  };
}

function pieOption() {
  // For pie: use the first numeric column (or coerce yKeys[0]) and the xKey
  // as labels. Aggregate duplicate labels by summing.
  const valueKey = state.yKeys[0] || null;
  if (!valueKey) {
    return { title: { text: "Pie needs a numeric column", left: "center", top: "middle" } };
  }
  const agg = new Map();
  for (const row of state.rows) {
    if (!row) continue;
    const name = String(row[state.xKey]);
    const v = coerceNumber(row[valueKey]);
    if (v === null) continue;
    agg.set(name, (agg.get(name) || 0) + v);
  }
  return {
    tooltip: { trigger: "item" },
    legend: { type: "scroll", top: 0 },
    series: [{
      name: valueKey,
      type: "pie",
      radius: ["35%", "70%"],
      data: [...agg.entries()].map(([name, value]) => ({ name, value })),
    }],
  };
}

function render() {
  buildToolbar();
  const inst = ensureChart();
  inst.setOption(buildOption(), true);
  if (state.rows.length === 0) {
    setStatus("No rows.");
  } else {
    setStatus(`${state.rows.length} rows · ${state.columns.length} columns · ${state.chartType}`);
  }
}

function ingest(structured) {
  state.title = (structured && structured.title) || "";
  state.rows = Array.isArray(structured && structured.data) ? structured.data : [];
  state.columns = profileColumns(state.rows);
  const defaults = pickAxisDefaults(state.columns);
  state.xKey = defaults.xKey;
  state.yKeys = defaults.yKeys;
  state.chartType = "bar";
}

function showText(text) {
  toolbar.innerHTML = "";
  chartDiv.innerHTML = "";
  const pre = document.createElement("pre");
  pre.style.cssText = "margin:12px;padding:12px;background:rgba(127,127,127,0.08);border-radius:6px;overflow:auto;font:12px/1.4 ui-monospace,Menlo,monospace;";
  pre.textContent = text;
  chartDiv.appendChild(pre);
}

// SDK wiring — handlers registered before connect().
const app = new App();

app.ontoolinput = () => {
  setStatus("Tool invoked, awaiting result…");
};

app.ontoolresult = (result) => {
  const structured = result && result.structuredContent;
  if (structured && Array.isArray(structured.data)) {
    ingest(structured);
    render();
    return;
  }
  const text = result && result.content && result.content[0] && result.content[0].text;
  if (text) {
    showText(text);
    setStatus("Text-only result; nothing to chart.");
    return;
  }
  setStatus("No payload.");
};

app.onhostcontextchanged = (ctx) => {
  const theme = applyHostTheme(ctx);
  const newEchartsTheme = hostEchartsTheme(ctx);
  if (newEchartsTheme !== currentTheme) {
    currentTheme = newEchartsTheme;
    if (chart) {
      chart.dispose();
      chart = null;
    }
    if (state.rows.length > 0) render();
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
