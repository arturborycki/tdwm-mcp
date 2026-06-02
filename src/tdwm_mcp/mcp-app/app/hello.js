// PR1 smoke-test app. Confirms the ui:// resource is fetched, the SDK
// instantiates, and the tool result reaches the bundle.
//
// Pre-connect handler registration: required by the Apps SDK Quickstart —
// otherwise the initial ontoolresult fires before the handler exists.

const statusEl = document.getElementById("status");
const chartEl = document.getElementById("chart");

function showStatus(msg) {
  if (statusEl) statusEl.textContent = msg;
}

function showPayload(payload) {
  showStatus("Received tool result.");
  if (!chartEl) return;
  const pre = document.createElement("pre");
  pre.style.cssText = "margin:12px;padding:12px;background:rgba(127,127,127,0.08);border-radius:6px;overflow:auto;font:12px/1.4 ui-monospace,Menlo,monospace;";
  pre.textContent = JSON.stringify(payload, null, 2);
  chartEl.innerHTML = "";
  chartEl.appendChild(pre);
}

const app = new App();

app.ontoolresult = (result) => {
  const structured = result && result.structuredContent;
  if (structured) {
    showPayload(structured);
    return;
  }
  const text = result && result.content && result.content[0] && result.content[0].text;
  if (text) {
    showStatus("Received text-only result.");
    showPayload({ text });
    return;
  }
  showStatus("Tool result had no recognized payload.");
};

app.onhostcontextchanged = (ctx) => {
  if (ctx && ctx.theme === "dark") {
    document.documentElement.style.colorScheme = "dark";
  } else {
    document.documentElement.style.colorScheme = "light";
  }
};

app.onteardown = () => {
  showStatus("Torn down.");
};

await app.connect();
showStatus("Connected. Awaiting tool result…");
