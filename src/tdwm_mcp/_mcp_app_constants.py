"""Constants for the MCP Apps extension integration.

The MCP Apps extension (SEP-1865, stabilized 2026-01-26) defines a contract
between a tool and a UI bundle: the tool carries `_meta.ui.resourceUri`
pointing at a resource that serves an HTML bundle with a specific MIME type.

We hard-code the MIME type because the upstream SDK exports it as a single
trivial constant. Source: @modelcontextprotocol/ext-apps@1.7.3
dist/src/server/index.js — `var p = "text/html;profile=mcp-app"`.
"""

MCP_APP_MIME_TYPE = "text/html;profile=mcp-app"

# Restrictive default: all JS is inlined into the served HTML, so 'self'
# covers script/style. No network calls, no external fonts, no workers.
MCP_APP_CSP = (
    "default-src 'none'; "
    "script-src 'self' 'unsafe-inline'; "
    "style-src 'self' 'unsafe-inline'; "
    "img-src 'self' data:; "
    "connect-src 'none'; "
    "font-src 'self' data:; "
    "frame-ancestors *"
)

UI_URI_SCHEME = "ui"
UI_URI_PATH = "mcp-app.html"


def ui_uri_for(tool_name: str) -> str:
    """Canonical UI resource URI for a tool."""
    return f"{UI_URI_SCHEME}://{tool_name}/{UI_URI_PATH}"
