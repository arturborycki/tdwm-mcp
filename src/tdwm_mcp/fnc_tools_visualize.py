"""Visualization companion tools that render through the MCP Apps extension.

Each ``visualize_*`` tool is a thin sibling of an existing data tool. The data
path is unchanged; the difference is the return envelope:

* ``content`` carries a short human-readable summary so clients that don't
  support ``_meta.ui`` still degrade gracefully (mcp-inspector, Cline,
  Continue, Codex Desktop).
* ``structuredContent`` carries the typed payload the UI bundle renders.
* ``_meta.ui.resourceUri`` points the UI-supporting host at the HTML bundle.

PR1 ships only ``visualize_ping`` as a smoke test. Tier-2 chart tools land in
PR3; ``visualize_sql_steps`` lands in PR5.
"""

from __future__ import annotations

import logging
from typing import Any, Iterable

import mcp.types as types

from ._mcp_app_constants import MCP_APP_CSP, MCP_APP_MIME_TYPE, ui_uri_for

logger = logging.getLogger(__name__)


# Tool name -> bundle ("app") name in mcp-app/app/<name>.js.
# Multiple tools can share one bundle (tier-2 chart tools will share "generic").
_TOOL_TO_APP: dict[str, str] = {
    "visualize_ping": "hello",
}


def app_name_for_ui_uri(tool_name: str) -> str | None:
    """Resolve the bundle name for a tool's UI URI; ``None`` if unknown."""
    return _TOOL_TO_APP.get(tool_name)


def list_visualize_tools() -> list[types.Tool]:
    """Tool definitions to merge into the server's ``list_tools`` result.

    Uses ``model_validate`` with the JSON-side alias (``_meta``) so the MCP
    Apps extension field is emitted under its spec name, not Pydantic's
    Python-side ``meta``.
    """
    return [
        types.Tool.model_validate(
            {
                "name": "visualize_ping",
                "description": (
                    "Smoke-test the MCP Apps integration: returns a tiny payload "
                    "and a UI bundle. UI-supporting clients render the payload "
                    "in-place; other clients show a one-line text summary. "
                    "Use this to verify _meta.ui plumbing end-to-end."
                ),
                "inputSchema": {"type": "object", "properties": {}},
                "_meta": {"ui": {"resourceUri": ui_uri_for("visualize_ping")}},
            }
        )
    ]


def list_visualize_resources() -> list[types.Resource]:
    """UI resources to merge into the server's ``list_resources`` result.

    One resource per *tool* so each tool's ``_meta.ui.resourceUri`` is
    individually discoverable, even when several tools share one bundle.
    """
    return [
        types.Resource.model_validate(
            {
                "uri": ui_uri_for(tool_name),
                "name": f"{tool_name} UI bundle",
                "description": (
                    f"MCP Apps HTML bundle rendered by the {tool_name} tool. "
                    "Single self-contained file; no external network access."
                ),
                "mimeType": MCP_APP_MIME_TYPE,
                "_meta": {"ui": {"csp": MCP_APP_CSP, "permissions": []}},
            }
        )
        for tool_name in _TOOL_TO_APP
    ]


# Tool handlers ----------------------------------------------------------------

ToolResult = tuple[list[types.TextContent], dict[str, Any]]


async def _visualize_ping() -> ToolResult:
    structured = {
        "title": "MCP Apps smoke test",
        "message": "pong",
        "ok": True,
    }
    summary = types.TextContent(
        type="text",
        text="visualize_ping: pong (MCP Apps bundle delivered when supported)",
    )
    return ([summary], structured)


async def handle_visualize_tool_call(
    name: str, arguments: dict[str, Any] | None
) -> ToolResult | None:
    """Dispatch a ``visualize_*`` call.

    Returns ``None`` if ``name`` is not a visualize tool — lets the main
    dispatcher fall through to its existing tools.
    """
    if name == "visualize_ping":
        return await _visualize_ping()
    return None


def visualize_tool_names() -> Iterable[str]:
    """Names of all visualize tools (used by the main dispatcher to detect us)."""
    return _TOOL_TO_APP.keys()
