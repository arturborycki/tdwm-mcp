# Contributing

## Running tests

```bash
uv run pytest tests/ -v
```

The full suite is fast (< 1 s) and does not require a Teradata connection —
SQL is mocked at the dispatcher boundary and the EXPLAIN shaper is exercised
against captured fixtures under `tests/fixtures/explain/`.

When adding a new `visualize_*` tool, also add an entry to the
[Visualizations table in the README](./README.md#available-visualize-tools).

## Refreshing the vendored MCP Apps JS

The visualization bundles use two vendored JavaScript files served from
`src/tdwm_mcp/mcp-app/vendor/`:

| File | Upstream |
|------|----------|
| `ext-apps-<version>.mjs` | `@modelcontextprotocol/ext-apps@<version>/dist/src/app-with-deps.js` |
| `echarts-<version>.min.js` | `echarts@<version>/dist/echarts.min.js` |

Both ship as published, pre-bundled artifacts — **no Node toolchain is
required to install or build tdwm-mcp**.

### When to refresh

- The upstream ext-apps SDK ships a release that includes a fix or feature
  we want.
- echarts ships a security fix.
- Quarterly heartbeat refresh (see "Last checked" in
  [`VENDOR_VERSIONS.md`](./src/tdwm_mcp/mcp-app/VENDOR_VERSIONS.md)).

Do **not** refresh casually — every bump is a chance for breakage in a host
client. Smoke-test before merging.

### How to refresh

1. Edit the version pins at the top of `scripts/update-vendor.sh`.
2. Run the script:
   ```bash
   ./scripts/update-vendor.sh
   ```
   It downloads the new files, regenerates `SHA256SUMS`, and prints the new
   checksums.
3. If the version number changed, update the filename constants in
   `src/tdwm_mcp/_mcp_app_render.py`
   (`_EXT_APPS_FILENAME` / `_ECHARTS_FILENAME`).
4. If the MCP Apps spec MIME type changed, update `MCP_APP_MIME_TYPE` in
   `src/tdwm_mcp/_mcp_app_constants.py`. (The constant is intentionally
   hard-coded; the upstream SDK exports it as a single trivial string.)
5. Update [`VENDOR_VERSIONS.md`](./src/tdwm_mcp/mcp-app/VENDOR_VERSIONS.md):
   bump the version column, refresh the file size, refresh the "Last
   checked" date.
6. Smoke-test against a UI-supporting MCP client (Claude Desktop, VS Code
   Copilot Chat, or similar). Confirm at minimum that `visualize_ping`
   renders.
7. Commit `vendor/`, `SHA256SUMS`, `VENDOR_VERSIONS.md`, and any constant
   changes together — they must move as one unit.

### Why we vendor

The MCP Apps SDK is designed to be loaded as a self-contained ESM bundle
(`app-with-deps.js`). Echarts ships a UMD bundle for the same reason. We
concatenate them with our app code into the served HTML at runtime, which
removes the entire npm/Node toolchain from the build path. The tradeoffs:

- ✅ No build step. No `package.json`. No CI Node version pinning.
- ✅ Locked CSP (`default-src 'none'`) — nothing fetches from a CDN at
  render time.
- ✅ Integrity verified by committed `SHA256SUMS`.
- ⚠️  Vendored files must be refreshed manually; there's no Dependabot to
  nag us.
- ⚠️  We're slightly off the "paved road" — the SDK README assumes a
  bundler. Document any quirks here as we hit them.

## Adding a `visualize_*` tool

1. Pick the existing data tool you're complementing. The visualize tool
   should **reuse its SQL** verbatim, not invent a new one.
2. In `src/tdwm_mcp/fnc_tools_visualize.py`:
   - Add the tool name to `_TOOL_TO_APP` mapping it to the right bundle
     (almost always `"generic"` unless you're adding a bespoke renderer).
   - Add a `_tool(...)` entry inside `list_visualize_tools()`. Include
     `_meta.ui.resourceUri` via the `_tool` helper.
   - Add an `async def _visualize_<name>(...)` handler that runs the SQL
     and returns `_build_result(...)`.
   - Add a dispatch line at the bottom of `handle_visualize_tool_call`.
3. Update the README table.
4. For a bespoke bundle, write a new `app/<name>.js` and add an entry to the
   client-support matrix if rendering changes.

The generic bundle auto-detects columns. If your data fits a `[{key:
value}, ...]` shape, it'll render with no further wiring. If you need a
different shape, write a new bundle next to `sql-steps.js`.
