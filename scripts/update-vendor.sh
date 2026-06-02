#!/usr/bin/env bash
# Refresh vendored client-side JS for the MCP Apps bundles.
#
# Pins are intentionally separate from VENDOR_VERSIONS.md so the script
# can be diffed for an upgrade in one place. After editing the pinned
# versions, run this script, review the diff in the vendor/ tree, and
# update VENDOR_VERSIONS.md (size, last-checked date).
set -euo pipefail

EXT_APPS_VERSION="1.7.3"
ECHARTS_VERSION="6.1.0"

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
VENDOR_DIR="$ROOT/src/tdwm_mcp/mcp-app/vendor"

mkdir -p "$VENDOR_DIR"

curl -fL -o "$VENDOR_DIR/ext-apps-${EXT_APPS_VERSION}.mjs" \
  "https://unpkg.com/@modelcontextprotocol/ext-apps@${EXT_APPS_VERSION}/dist/src/app-with-deps.js"

curl -fL -o "$VENDOR_DIR/echarts-${ECHARTS_VERSION}.min.js" \
  "https://cdn.jsdelivr.net/npm/echarts@${ECHARTS_VERSION}/dist/echarts.min.js"

cd "$VENDOR_DIR"
# Generate checksums for whatever versions are now present.
shasum -a 256 ext-apps-*.mjs echarts-*.min.js > SHA256SUMS

echo
echo "Vendor refreshed. New SHA256SUMS:"
cat SHA256SUMS
echo
echo "Next steps:"
echo "  1. Update VENDOR_VERSIONS.md with new sizes / check date."
echo "  2. Update _mcp_app_constants.py vendor filenames if the version changed."
echo "  3. Smoke-test against a UI-supporting MCP client."
