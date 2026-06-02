# Vendored client-side JS

These files are committed verbatim and served by the Python resource handler.
**No build step.** No Node/npm required to install or run tdwm-mcp.

| File | Upstream | Version | Format | Size |
|------|----------|---------|--------|------|
| `vendor/ext-apps-1.7.3.mjs` | https://unpkg.com/@modelcontextprotocol/ext-apps@1.7.3/dist/src/app-with-deps.js | 1.7.3 | ESM (self-contained, deps inlined) | ~330 KB |
| `vendor/echarts-6.1.0.min.js` | https://cdn.jsdelivr.net/npm/echarts@6.1.0/dist/echarts.min.js | 6.1.0 | UMD (exposes global `echarts`) | ~1.1 MB |

Integrity is verified by `vendor/SHA256SUMS`. CI re-runs `shasum -a 256 -c` against
the committed checksums on every push.

## Refreshing

Run `scripts/update-vendor.sh`. Review the diff and the resulting `SHA256SUMS`
before committing. Smoke-test against at least one UI-supporting MCP client
(e.g. Claude Desktop) before merging the bump.

## Why no bundler

The MCP Apps SDK ships `app-with-deps.js` as a self-contained ESM bundle for
exactly this use case. Echarts ships a UMD `echarts.min.js` for the same reason.
Loading them via `<script>` and `<script type="module">` removes the npm/Node
toolchain entirely.

Last checked: 2026-06-02.
