// Shared helpers for tdwm-mcp visualization bundles.
//
// This file is *concatenated* (not imported) into each per-app module by
// _mcp_app_render. That means: no `export` keywords here. Just declare
// functions/constants — they become module-scope visible to whatever app
// code is appended afterward.

function inferColumnType(values) {
  let nNumeric = 0;
  let nDateish = 0;
  let nNonEmpty = 0;
  for (const v of values) {
    if (v === null || v === undefined || v === "") continue;
    nNonEmpty += 1;
    if (typeof v === "number" && Number.isFinite(v)) {
      nNumeric += 1;
      continue;
    }
    if (typeof v === "string") {
      if (/^-?\d+(\.\d+)?$/.test(v.trim())) {
        nNumeric += 1;
        continue;
      }
      if (/^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2})?/.test(v)) {
        nDateish += 1;
        continue;
      }
    }
  }
  if (nNonEmpty === 0) return "empty";
  if (nNumeric / nNonEmpty >= 0.8) return "numeric";
  if (nDateish / nNonEmpty >= 0.8) return "date";
  return "string";
}

function profileColumns(rows) {
  if (!Array.isArray(rows) || rows.length === 0) return [];
  const seen = new Set();
  const order = [];
  for (const row of rows) {
    if (row && typeof row === "object") {
      for (const k of Object.keys(row)) {
        if (!seen.has(k)) {
          seen.add(k);
          order.push(k);
        }
      }
    }
  }
  return order.map((name) => {
    const values = rows.map((r) => (r ? r[name] : undefined));
    return { name, type: inferColumnType(values) };
  });
}

function pickAxisDefaults(columns) {
  let xKey = null;
  for (const c of columns) {
    if (c.type === "date") { xKey = c.name; break; }
  }
  if (!xKey) {
    for (const c of columns) {
      if (c.type === "string") { xKey = c.name; break; }
    }
  }
  if (!xKey && columns.length > 0) xKey = columns[0].name;
  const yKeys = columns.filter((c) => c.type === "numeric" && c.name !== xKey).map((c) => c.name);
  return { xKey, yKeys };
}

function applyHostTheme(hostContext) {
  const theme = hostContext && hostContext.theme === "dark" ? "dark" : "light";
  document.documentElement.style.colorScheme = theme;
  document.documentElement.dataset.theme = theme;
  return theme;
}

function hostEchartsTheme(hostContext) {
  return hostContext && hostContext.theme === "dark" ? "dark" : null;
}

function coerceNumber(v) {
  if (v === null || v === undefined || v === "") return null;
  if (typeof v === "number") return Number.isFinite(v) ? v : null;
  if (typeof v === "string") {
    const n = Number(v.trim());
    return Number.isFinite(n) ? n : null;
  }
  return null;
}
