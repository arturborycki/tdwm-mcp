"""Render a self-contained HTML bundle for an MCP App.

Concatenates the static template with vendored JS (ext-apps SDK + echarts)
and the per-app code. Output is deterministic per process so MCP hosts can
cache the resource.
"""

from __future__ import annotations

import logging
from functools import lru_cache
from pathlib import Path

from ._mcp_app_constants import MCP_APP_CSP

logger = logging.getLogger(__name__)

_MCP_APP_DIR = Path(__file__).parent / "mcp-app"
_VENDOR_DIR = _MCP_APP_DIR / "vendor"
_APP_DIR = _MCP_APP_DIR / "app"
_TEMPLATE_PATH = _MCP_APP_DIR / "template.html"

_EXT_APPS_FILENAME = "ext-apps-1.7.3.mjs"
_ECHARTS_FILENAME = "echarts-6.1.0.min.js"


class MCPAppRenderError(RuntimeError):
    """Raised when an app bundle cannot be assembled."""


@lru_cache(maxsize=None)
def _read(path: Path) -> str:
    if not path.is_file():
        raise MCPAppRenderError(f"missing required mcp-app file: {path}")
    return path.read_text(encoding="utf-8")


@lru_cache(maxsize=None)
def render_app_html(app_name: str) -> str:
    """Return the rendered HTML for a named app (e.g. ``"hello"``).

    Result is cached for the lifetime of the process; vendor and template
    files are static, so this is safe and gives hosts a stable byte stream
    to cache.
    """
    template = _read(_TEMPLATE_PATH)
    echarts = _read(_VENDOR_DIR / _ECHARTS_FILENAME)
    ext_apps = _read(_VENDOR_DIR / _EXT_APPS_FILENAME)
    app_code = _read(_APP_DIR / f"{app_name}.js")

    html = (
        template
        .replace("__CSP__", MCP_APP_CSP)
        .replace("__ECHARTS_UMD__", echarts)
        .replace("__EXT_APPS_ESM__", ext_apps)
        .replace("__APP_CODE__", app_code)
    )
    return html


def available_apps() -> list[str]:
    """List app names with a corresponding ``app/<name>.js`` file."""
    if not _APP_DIR.is_dir():
        return []
    return sorted(p.stem for p in _APP_DIR.glob("*.js"))
