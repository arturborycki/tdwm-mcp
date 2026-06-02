"""Visualization companion tools that render through the MCP Apps extension.

Each ``visualize_*`` tool is a thin sibling of an existing data tool. The data
path is unchanged; the difference is the return envelope:

* ``content`` carries a short human-readable summary so clients that don't
  support ``_meta.ui`` still degrade gracefully (mcp-inspector, Cline,
  Continue, Codex Desktop).
* ``structuredContent`` carries the typed payload the UI bundle renders
  (``{title, data: [...row dicts], meta: {...}}``).
* ``_meta.ui.resourceUri`` points the UI-supporting host at the HTML bundle.

The SQL is duplicated verbatim from the sibling tools in ``fnc_tools.py``.
That's deliberate: keeping these wrappers self-contained avoids any risk of
regressing the existing tools, and the bodies are small.
"""

from __future__ import annotations

import asyncio
import base64
import datetime
import decimal
import logging
from typing import Any, Iterable

import mcp.types as types

from ._mcp_app_constants import MCP_APP_CSP, MCP_APP_MIME_TYPE, ui_uri_for
from .fnc_common import _set_queryband, acquire_connection
from .retry_utils import with_connection_retry

logger = logging.getLogger(__name__)


# Tool name -> bundle ("app") name in mcp-app/app/<name>.js.
# - "hello" stays as the bundle smoke test.
# - All tier-2 chart tools share the "generic" bundle.
_TOOL_TO_APP: dict[str, str] = {
    "visualize_ping": "hello",
    "visualize_tdwm_summary": "generic",
    "visualize_top_users": "generic",
    "visualize_throttle_statistics": "generic",
    "visualize_tasm_statistics": "generic",
    "visualize_tasm_event_history": "generic",
    "visualize_delay_queue": "generic",
    "visualize_amp_load": "generic",
    "visualize_awt": "generic",
    "visualize_query_log": "generic",
    "visualize_sql_steps": "sql-steps",
}


def app_name_for_ui_uri(tool_name: str) -> str | None:
    """Resolve the bundle name for a tool's UI URI; ``None`` if unknown."""
    return _TOOL_TO_APP.get(tool_name)


def visualize_tool_names() -> Iterable[str]:
    """Names of all visualize tools (used by the main dispatcher to detect us)."""
    return _TOOL_TO_APP.keys()


# Tool / resource definitions --------------------------------------------------

_EMPTY_OBJECT_SCHEMA: dict[str, Any] = {"type": "object", "properties": {}}


def _tool(name: str, description: str, input_schema: dict | None = None) -> types.Tool:
    """Build a Tool with the MCP Apps ``_meta.ui.resourceUri`` field.

    Uses ``model_validate`` with the JSON-side alias so the field is emitted
    under its spec name on the wire (Pydantic's Python-side ``meta`` would
    serialize as ``"meta"``, which isn't what the Apps extension expects).
    """
    return types.Tool.model_validate(
        {
            "name": name,
            "description": description,
            "inputSchema": input_schema or _EMPTY_OBJECT_SCHEMA,
            "_meta": {"ui": {"resourceUri": ui_uri_for(name)}},
        }
    )


def list_visualize_tools() -> list[types.Tool]:
    """Tool definitions to merge into the server's ``list_tools`` result."""
    return [
        _tool(
            "visualize_ping",
            "Smoke-test the MCP Apps integration: returns a tiny payload and a UI "
            "bundle. UI-supporting clients render the payload in-place; other "
            "clients show a one-line text summary. Use this to verify _meta.ui "
            "plumbing end-to-end.",
        ),
        _tool(
            "visualize_tdwm_summary",
            "Workload-summary chart. Same data as show_tdwm_summary, but returns "
            "structuredContent for the MCP Apps bundle to render as a bar/pie "
            "chart of query counts and resource usage per workload.",
        ),
        _tool(
            "visualize_top_users",
            "Top-users chart. Same data as show_top_users; renders AMPCPUTime by "
            "username so the heaviest consumers are visible at a glance.",
            {
                "type": "object",
                "properties": {
                    "type": {
                        "type": "string",
                        "description": "TOP for top 15, otherwise all heavy users",
                    },
                },
            },
        ),
        _tool(
            "visualize_throttle_statistics",
            "Throttle-statistics chart. Same data as show_trottle_statistics; "
            "renders limit vs current usage vs delayed per throttle.",
            {
                "type": "object",
                "properties": {
                    "type": {
                        "type": "string",
                        "description": "ALL, QUERY, SESSION, or WORKLOAD",
                    },
                },
            },
        ),
        _tool(
            "visualize_tasm_statistics",
            "TASM statistics time-series. Same data as show_tasm_statistics; "
            "renders CPU%, IO/s, AWT, and wait-time metrics per workload over "
            "today's hours.",
        ),
        _tool(
            "visualize_tasm_event_history",
            "TASM event-history timeline. Same data as show_tasm_even_history; "
            "renders the chronological event log as a sortable/zoomable chart.",
        ),
        _tool(
            "visualize_delay_queue",
            "Delay-queue chart. Same data as display_delay_queue; renders queue "
            "depth and wait times per queue type.",
            {
                "type": "object",
                "properties": {
                    "type": {
                        "type": "string",
                        "description": "WORKLOAD, SYSTEM, UTILITY, or ALL (default)",
                    },
                },
            },
        ),
        _tool(
            "visualize_amp_load",
            "AMP-load chart. Same data as monitor_amp_load; renders per-AMP CPU "
            "utilization to make skew obvious.",
        ),
        _tool(
            "visualize_awt",
            "AWT chart. Same data as monitor_awt; renders AMP Worker Task slot "
            "usage and queue depth.",
        ),
        _tool(
            "visualize_query_log",
            "Query-log chart for a user. Same data as show_query_log; renders "
            "CPU/IO over recent queries so heavy ones stand out.",
            {
                "type": "object",
                "properties": {
                    "user": {"type": "string", "description": "User name"},
                },
                "required": ["user"],
            },
        ),
        _tool(
            "visualize_sql_steps",
            "Visual EXPLAIN: render a query plan from MonitorSQLSteps as an "
            "interactive directed graph. Same source data as "
            "show_sql_steps_for_session, but nodes are sized by estimated rows, "
            "colored by confidence (HIGH/LOW/NO/JOIN), and tooltips carry the "
            "step text + estimated vs actual timing. Linear edges by step "
            "number (parallel-branch detection deferred).",
            {
                "type": "object",
                "properties": {
                    "sessionNo": {
                        "type": "integer",
                        "description": "Session Number to render the EXPLAIN graph for",
                    },
                },
                "required": ["sessionNo"],
            },
        ),
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


# Result helpers ---------------------------------------------------------------

ToolResult = tuple[list[types.TextContent], dict[str, Any]]


def _to_json_value(v: Any) -> Any:
    """Coerce a teradatasql/DB-API value into something the JSON serializer
    in the lowlevel SDK can encode safely.

    The MCP CallToolResult serialization will eventually call ``json.dumps``
    on ``structuredContent``, so anything that isn't a JSON primitive needs
    a representation here. We pick representations the chart bundle can
    still detect (e.g. ISO strings remain date-ish for column typing).
    """
    if v is None:
        return None
    if isinstance(v, bool) or isinstance(v, (int, float, str)):
        return v
    if isinstance(v, decimal.Decimal):
        # Lossy but fine for charting; preserves shape.
        return float(v)
    if isinstance(v, (datetime.datetime, datetime.date, datetime.time)):
        return v.isoformat()
    if isinstance(v, datetime.timedelta):
        return v.total_seconds()
    if isinstance(v, bytes):
        try:
            return v.decode("utf-8")
        except UnicodeDecodeError:
            return base64.b64encode(v).decode("ascii")
    # Fallback: stringify so the row stays plottable as a label.
    return str(v)


def _rows_as_dicts(description, rows) -> tuple[list[str], list[dict[str, Any]]]:
    """Convert (cursor.description, rows) into ([column_names], [row_dicts]).

    ``description`` follows DB-API 2.0: a sequence of 7-tuples where index 0
    is the column name.
    """
    if description is None:
        return [], []
    cols = [d[0] for d in description]
    data = [{c: _to_json_value(v) for c, v in zip(cols, row)} for row in rows]
    return cols, data


def _summary(tool: str, row_count: int, cols: list[str], note: str = "") -> str:
    head = f"{tool}: {row_count} row{'s' if row_count != 1 else ''}"
    if cols:
        # Cap displayed column list so the summary stays a single short line.
        shown = ", ".join(cols[:4])
        if len(cols) > 4:
            shown += f", … (+{len(cols) - 4})"
        head += f" · columns: {shown}"
    if note:
        head += f" · {note}"
    return head


def _build_result(
    tool_name: str,
    title: str,
    description,
    rows,
    *,
    note: str = "",
    extra_meta: dict[str, Any] | None = None,
) -> ToolResult:
    cols, data = _rows_as_dicts(description, rows)
    meta: dict[str, Any] = {"row_count": len(data), "columns": cols}
    if extra_meta:
        meta.update(extra_meta)
    structured = {"title": title, "data": data, "meta": meta}
    summary = types.TextContent(type="text", text=_summary(tool_name, len(data), cols, note))
    return ([summary], structured)


# Visual EXPLAIN shaper --------------------------------------------------------

# Confidence codes returned by ``Confidence`` in MonitorSQLSteps. Teradata
# encodes confidence as a SMALLINT but the column is wrapped in
# ``(format '9')`` so we may get either the integer or its 1-char string form
# back. Accept both, plus the letter form some clients return after
# downstream tooling normalizes the value. Anything unrecognized → UNKNOWN so
# the bundle still renders the node.
_CONFIDENCE_MAP: dict[str, str] = {
    "0": "LOW",
    "1": "HIGH",
    "2": "NO",
    "3": "JOIN",
    "L": "LOW",
    "H": "HIGH",
    "N": "NO",
    "J": "JOIN",
    "LOW": "LOW",
    "HIGH": "HIGH",
    "NO": "NO",
    "JOIN": "JOIN",
}

# Max length of the per-step SQL text included in the tooltip. Keeps the
# hover panel readable even for big multi-join steps.
_STEP_TEXT_TOOLTIP_CHARS = 320


def _confidence_label(raw: Any) -> str:
    if raw is None:
        return "UNKNOWN"
    s = str(raw).strip().upper()
    if not s:
        return "UNKNOWN"
    return _CONFIDENCE_MAP.get(s, "UNKNOWN")


def _row_get(row: dict, *keys: str, default=None):
    """Case-insensitive lookup over a row dict.

    teradatasql preserves the alias casing chosen in the SQL (``Num``, ``ERC``),
    but a future query change or copy might land lowercase. Walking the keys
    once is cheap and saves us from constant case fights.
    """
    if not isinstance(row, dict):
        return default
    for k in keys:
        if k in row:
            return row[k]
    lower = {kk.lower(): kk for kk in row.keys()}
    for k in keys:
        real = lower.get(k.lower())
        if real is not None:
            return row[real]
    return default


def _as_int(v: Any) -> int | None:
    if v is None:
        return None
    try:
        if isinstance(v, str):
            v = v.strip()
            if not v:
                return None
        return int(float(v))
    except (TypeError, ValueError):
        return None


def _format_number(v: Any) -> str:
    n = _as_int(v)
    if n is None:
        return "—"
    return f"{n:,}"


def _make_tooltip(step_num: int, conf: str, fields: dict[str, Any]) -> str:
    step_text = fields.get("step_text") or ""
    if len(step_text) > _STEP_TEXT_TOOLTIP_CHARS:
        step_text = step_text[: _STEP_TEXT_TOOLTIP_CHARS - 1].rstrip() + "…"
    lines = [
        f"Step {step_num}",
        "",
        step_text,
        "",
        f"Confidence: {conf}",
        f"Est rows: {_format_number(fields.get('est_rows'))}",
        f"Act rows: {_format_number(fields.get('act_rows'))}",
        f"Est time: {_format_number(fields.get('est_ms'))} cs",
        f"Act time: {_format_number(fields.get('act_ms'))} cs",
    ]
    return "\n".join(lines)


def build_explain_graph(rows: list[dict], *, session_no: int | None = None) -> dict[str, Any]:
    """Convert MonitorSQLSteps rows into the ``sql-steps`` bundle's payload.

    Output shape (matches what ``app/sql-steps.js`` expects):

    .. code-block:: python

        {
            "title": "Visual EXPLAIN — session 123",
            "nodes": [{"id", "name", "category", "value", "tooltip",
                        "est_rows", "act_rows", "est_ms", "act_ms",
                        "step_text"}, ...],
            "links": [{"source", "target"}],          # linear by step number
            "meta":  {"total_steps", "est_elapsed_cs",
                       "act_elapsed_cs", "max_est_rows",
                       "session_no", "row_count"}
        }

    Linear edges only in v1: parallel branches and step-dependency detection
    are deferred. Missing or unknown fields degrade gracefully: a row with no
    ``StepNum`` is dropped (without one we can't draw an edge), all others
    fall back to ``"—"`` in the tooltip and produce a UNKNOWN-coloured node.
    """
    cleaned: list[tuple[int, dict]] = []
    for row in rows or []:
        num = _as_int(_row_get(row, "Num", "StepNum", "num"))
        if num is None:
            continue
        cleaned.append((num, row))
    cleaned.sort(key=lambda x: x[0])

    nodes: list[dict[str, Any]] = []
    sum_est_ms = 0
    sum_act_ms = 0
    max_est_rows = 0
    for num, row in cleaned:
        conf = _confidence_label(_row_get(row, "C", "Confidence"))
        est_rows = _as_int(_row_get(row, "ERC", "EstRowCount"))
        act_rows = _as_int(_row_get(row, "ARC", "ActRowCount"))
        est_ms = _as_int(_row_get(row, "EET", "EstElapsedTime"))
        act_ms = _as_int(_row_get(row, "AET", "ActElapsedTime"))
        step_text = _row_get(row, "SQLStep", "sqlstep") or ""
        if not isinstance(step_text, str):
            step_text = str(step_text)

        # Negative est_rows is Teradata's "unknown" sentinel — surface as 0 for
        # the symbolSize scale so it doesn't dwarf the rest. Real value is
        # still in node["est_rows"] / tooltip for inspection.
        plot_value = max(0, est_rows) if est_rows is not None else 0

        if est_rows is not None and est_rows > max_est_rows:
            max_est_rows = est_rows
        if est_ms is not None:
            sum_est_ms += est_ms
        if act_ms is not None:
            sum_act_ms += act_ms

        nodes.append({
            "id": str(num),
            "name": f"Step {num}",
            "category": conf,
            "value": plot_value,
            "tooltip": _make_tooltip(num, conf, {
                "step_text": step_text,
                "est_rows": est_rows,
                "act_rows": act_rows,
                "est_ms": est_ms,
                "act_ms": act_ms,
            }),
            "est_rows": est_rows,
            "act_rows": act_rows,
            "est_ms": est_ms,
            "act_ms": act_ms,
            "step_text": step_text,
        })

    links = [
        {"source": nodes[i]["id"], "target": nodes[i + 1]["id"]}
        for i in range(len(nodes) - 1)
    ]

    title = (
        f"Visual EXPLAIN — session {session_no}"
        if session_no is not None
        else "Visual EXPLAIN"
    )

    return {
        "title": title,
        "nodes": nodes,
        "links": links,
        "meta": {
            "total_steps": len(nodes),
            "est_elapsed_cs": sum_est_ms,
            "act_elapsed_cs": sum_act_ms,
            "max_est_rows": max_est_rows,
            "session_no": session_no,
            "row_count": len(nodes),
        },
    }


# Tool handlers ----------------------------------------------------------------


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


@with_connection_retry()
async def _visualize_tdwm_summary() -> ToolResult:
    async with acquire_connection() as tdconn:
        def _run():
            _set_queryband(tdconn, "visualize_tdwm_summary")
            cur = tdconn.cursor()
            cur.execute("SELECT * FROM TABLE (TDWM.TDWMSummary()) AS t2")
            rows = cur.fetchall()
            return cur.description, list(rows)
        description, rows = await asyncio.to_thread(_run)
    return _build_result("visualize_tdwm_summary", "TDWM workload summary", description, rows)


@with_connection_retry()
async def _visualize_top_users(type_: str) -> ToolResult:
    async with acquire_connection() as tdconn:
        def _run():
            _set_queryband(tdconn, "visualize_top_users")
            cur = tdconn.cursor()
            if (type_ or "").upper() == "TOP":
                query = """
                    Sel top 15 Username (Format 'x(10)'), queryband(Format 'x(40)'),AppID, ClientAddr, StartTime, AMPCPUTime, QueryText from dbc.qrylogV
                    where ampcputime > .154 order by ampcputime desc"""
            else:
                query = """
                    Sel Username (Format 'x(10)'), queryband(Format 'x(40)'),AppID, ClientAddr, StartTime, AMPCPUTime, QueryText from dbc.qrylogV
                    where ampcputime > .154 order by ampcputime desc"""
            cur.execute(query)
            rows = cur.fetchall()
            return cur.description, list(rows)
        description, rows = await asyncio.to_thread(_run)
    return _build_result(
        "visualize_top_users",
        "Top users by AMPCPUTime",
        description,
        rows,
        extra_meta={"type": (type_ or "ALL").upper()},
    )


@with_connection_retry()
async def _visualize_throttle_statistics(type_: str) -> ToolResult:
    async with acquire_connection() as tdconn:
        def _run():
            _set_queryband(tdconn, "visualize_throttle_statistics")
            cur = tdconn.cursor()
            t = (type_ or "").upper()
            if t == "QUERY":
                sql = "SELECT * FROM TABLE (TDWM.TDWMTHROTTLESTATISTICS('Q')) AS t1"
            elif t == "SESSION":
                sql = "SELECT * FROM TABLE (TDWM.TDWMTHROTTLESTATISTICS('S')) AS t1"
            elif t == "WORKLOAD":
                sql = "SELECT * FROM TABLE (TDWM.TDWMTHROTTLESTATISTICS('W')) AS t1"
            elif t == "ALL":
                sql = "SELECT * FROM TABLE (TDWM.TDWMTHROTTLESTATISTICS('A')) AS t1"
            else:
                sql = (
                    "SELECT ObjectType(FORMAT 'x(10)'), rulename(FORMAT 'x(17)'), "
                    "ObjectName(FORMAT 'x(13)'), active(FORMAT 'Z9'), "
                    "throttlelimit as ThrLimit, delayed(FORMAT 'Z9'), throttletype as ThrType "
                    "FROM TABLE (TDWM.TDWMTHROTTLESTATISTICS('A')) AS t1 ORDER BY 1,2"
                )
            cur.execute(sql)
            rows = cur.fetchall()
            return cur.description, list(rows)
        description, rows = await asyncio.to_thread(_run)
    return _build_result(
        "visualize_throttle_statistics",
        "Throttle statistics",
        description,
        rows,
        extra_meta={"type": (type_ or "ALL").upper()},
    )


@with_connection_retry()
async def _visualize_tasm_statistics() -> ToolResult:
    sql = """
    select
        TheDatePN (FORMAT'yy/mm/dd', TITLE '// //Date'),
        TheHour (TITLE '// //Hour'),
        TheMinute (TITLE '// //Minute'),
        DayOfWeek (TITLE 'Day of Week'),
        NodeID (TITLE '//Node ID'),
        rulenamePN (TITLE '//Workload//Name'),
        ppidPN (FORMAT '9', TITLE '// //PP ID'),
        pgidPN (FORMAT 'ZZ9', TITLE '// //PG ID')
        ,average(CPUPctPN) (FORMAT 'ZZ9.9', TITLE 'CPU//Util// %')
        ,average(PhysicalIOPN) (FORMAT 'ZZ9.9', TITLE 'Avg//I/Os//per Sec')
        ,average(PhysicalIOMBPN) (FORMAT 'ZZ9.9', TITLE 'Avg//I/O Mbytes//per Sec')
        ,average(WorkMsgSendDelayCntPN) (FORMAT 'ZZ9.9', TITLE '# AWT Requests//Successfully Sent//per AMP')
        ,average(NumRequestsPN) (FORMAT 'ZZ9.9', TITLE '# Tasks//Assigned AWTs//per AMP')
        ,average(AwtReleasesPN) (FORMAT 'ZZ9.9', TITLE '# AWTs//Released//per AMP')
        ,average(QLengthAmpAvgAPN) (FORMAT 'ZZ9.9', TITLE '# Requests//Still Waiting//for AWT')
        ,max(WorkMsgSendDelayMPN) (FORMAT 'ZZ9.99', TITLE 'Max//Send-Side//Wait')
        ,max(QWaitTimeMaxMPN) (FORMAT 'ZZ9.99', TITLE 'Max//Receive-Side//Wait')
        ,max(WorkMsgReceiveDelayMPN) (FORMAT 'ZZ9.99', TITLE 'Max//Receive-Side//Still Waiting')
        ,average(zeroifnull(WorkMsgSendDelayRequestAPN)) (FORMAT 'ZZ9.99', TITLE 'Avg//Send-Side//Wait')
        ,average(zeroifnull(QwaitTimeRequestAPN)) (FORMAT 'ZZ9.99', TITLE 'Avg//Receive- Side//Wait')
        ,average(zeroifnull(WorkMsgReceiveDelayRequestAPN)) (FORMAT 'ZZ9.99', TITLE 'Avg//Receive-Side//Still Waiting')
        ,max(ServiceTimeMPN) (FORMAT 'ZZ9.99', TITLE 'Max//Time//AWT Held')
        ,average(zeroifnull(ServiceTimeAPN)) (FORMAT 'ZZ9.99', TITLE 'Avg//Time//AWT Held')
        ,max(WorkTimeInUseMPN) (FORMAT 'ZZ9.99', TITLE 'Max//Time//AWT Held or Still Held')
        ,average(AwtUsedAPN) (FORMAT 'ZZ9.9', TITLE 'Avg//AWTs//In Use')
    FROM
    (
        select
            t1.TheDate as TheDatePN
            ,extract(hour from t1.thetime) TheHour
            ,extract(Minute from t1.thetime) TheMinute
            ,CASE WHEN day_of_week = 1 THEN 'Sunday'
            WHEN day_of_week = 2 THEN 'Monday'
            WHEN day_of_week = 3 THEN 'Tuesday'
            WHEN day_of_week = 4 THEN 'Wednesday'
            WHEN day_of_week = 5 THEN 'Thursday'
            WHEN day_of_week = 6 THEN 'Friday'
            WHEN day_of_week = 7 THEN 'Saturday'
            END AS dayofweek,
            NodeId,
            rulename as rulenamePN,
            ppid as ppidPN,
            pgid as pgidPN
            ,SUM(CPUPct) as CPUPctPN
            ,sum((PhysicalReadPerm + PhysicalWritePerm+PhysicalReadOther+PhysicalWriteOther)/(CentiSecs/100)) as PhysicalIOPN
            ,sum((PhysicalReadPermKB + PhysicalWritePermKB+PhysicalReadOtherKB+PhysicalWriteOtherKB)/(1024*CentiSecs/100)) as PhysicalIOMBPN
            ,sum(WorkMsgSendDelayCnt/AmpCount) as WorkMsgSendDelayCntPN
            ,sum(NumRequests/AmpCount) as NumRequestsPN
            ,sum(AwtReleases/AmpCount) as AwtReleasesPN
            ,sum(WorkMsgReceiveDelayCnt/AmpCount) as QLengthAmpAvgAPN
            ,max(WorkMsgSendDelayMax) as WorkMsgSendDelayMPN
            ,max(WorkMsgReceiveDelayMax) as WorkMsgReceiveDelayMPN
            ,max(QWaitTimeMax) as QWaitTimeMaxMPN
            ,sum(WorkMsgSendDelayRequestAvg) as WorkMsgSendDelayRequestAPN
            ,sum(WorkMsgReceiveDelayRequestAvg) as WorkMsgReceiveDelayRequestAPN
            ,sum(QWaitTimeRequestAvg) as QWaitTimeRequestAPN
            ,sum(ServiceTimeRequestAvg) as ServiceTimeAPN
            ,max(ServiceTimeMax) as ServiceTimeMPN
            ,max(WorkTimeInUseMax) as WorkTimeInUseMPN
            ,sum(AWTUsedAvg/AmpCount) as AwtUsedAPN
        FROM
            DBC.ResSpsView as T1
            LEFT OUTER JOIN tdwm.RuleDefs as T2
            on (T1.WDid = T2.RuleId AND T2.RuleType =5)
            inner join sys_calendar.CALENDAR b
            on calendar_date = thedate
        where thedate = date and active >0 group by 1,2,3,4,5,6,7,8
    ) as SumPNTbl
    group by 1,2,3,4,5,6,7,8 order by 1,2,3,4,5,6,7
    """
    async with acquire_connection() as tdconn:
        def _run():
            _set_queryband(tdconn, "visualize_tasm_statistics")
            cur = tdconn.cursor()
            cur.execute(sql)
            rows = cur.fetchall()
            return cur.description, list(rows)
        description, rows = await asyncio.to_thread(_run)
    return _build_result("visualize_tasm_statistics", "TASM statistics", description, rows)


@with_connection_retry()
async def _visualize_tasm_event_history() -> ToolResult:
    async with acquire_connection() as tdconn:
        def _run():
            _set_queryband(tdconn, "visualize_tasm_event_history")
            cur = tdconn.cursor()
            cur.execute(
                """
                SELECT entryts,
                    SUBSTR(entrykind,1,10) "kind",
                    SUBSTR (entryname,1,20) "name",
                    CAST (eventvalue as float format '999.9999') "evt value",
                    CAST (lastvalue as float format '999.9999') "last value",
                    spare2 "spare Int",
                    SUBSTR (activity,1,10) "activity id",
                    SUBSTR (activityname,1,20) "act name", seqno
                FROM tdwmeventhistory order by entryts, seqno
                """
            )
            rows = cur.fetchall()
            return cur.description, list(rows)
        description, rows = await asyncio.to_thread(_run)
    return _build_result(
        "visualize_tasm_event_history",
        "TASM event history",
        description,
        rows,
    )


@with_connection_retry()
async def _visualize_delay_queue(type_: str) -> ToolResult:
    async with acquire_connection() as tdconn:
        def _run():
            _set_queryband(tdconn, "visualize_delay_queue")
            cur = tdconn.cursor()
            t = (type_ or "ALL").upper()
            if t == "WORKLOAD":
                sql = "SELECT * FROM TABLE (TDWM.TDWMGetDelayedQueries('W')) AS t1"
            elif t == "SYSTEM":
                sql = "SELECT * FROM TABLE (TDWM.TDWMGetDelayedQueries('O')) AS t1"
            elif t == "UTILITY":
                sql = "SELECT * FROM TABLE (TDWM.TDWMGetDelayedUtilities()) AS t1"
            else:
                sql = "SELECT * FROM TABLE (TDWM.TDWMGetDelayedQueries('A')) AS t1"
            cur.execute(sql)
            rows = cur.fetchall()
            return cur.description, list(rows)
        description, rows = await asyncio.to_thread(_run)
    return _build_result(
        "visualize_delay_queue",
        "Delay queue",
        description,
        rows,
        extra_meta={"type": (type_ or "ALL").upper()},
    )


@with_connection_retry()
async def _visualize_amp_load() -> ToolResult:
    async with acquire_connection() as tdconn:
        def _run():
            _set_queryband(tdconn, "visualize_amp_load")
            cur = tdconn.cursor()
            cur.execute("SELECT * FROM TABLE (MonitorAMPLoad()) AS t1")
            rows = cur.fetchall()
            return cur.description, list(rows)
        description, rows = await asyncio.to_thread(_run)
    return _build_result("visualize_amp_load", "AMP load", description, rows)


@with_connection_retry()
async def _visualize_awt() -> ToolResult:
    async with acquire_connection() as tdconn:
        def _run():
            _set_queryband(tdconn, "visualize_awt")
            cur = tdconn.cursor()
            cur.execute("SELECT * FROM TABLE (MonitorAWTResource(1,2,3,4)) AS t1")
            rows = cur.fetchall()
            return cur.description, list(rows)
        description, rows = await asyncio.to_thread(_run)
    return _build_result("visualize_awt", "AMP Worker Task resources", description, rows)


@with_connection_retry()
async def _visualize_query_log(user: str) -> ToolResult:
    async with acquire_connection() as tdconn:
        def _run():
            _set_queryband(tdconn, "visualize_query_log")
            cur = tdconn.cursor()
            cur.execute(
                "sel * from dbc.qrylogv where upper(username)=upper(?) "
                "and trunc(collectTimeStamp) = trunc(date) ORDER BY queryid",
                [user],
            )
            rows = cur.fetchall()
            return cur.description, list(rows)
        description, rows = await asyncio.to_thread(_run)
    return _build_result(
        "visualize_query_log",
        f"Query log for {user}",
        description,
        rows,
        extra_meta={"user": user},
    )


@with_connection_retry()
async def _visualize_sql_steps(session_no: int) -> ToolResult:
    session_no = int(session_no)
    async with acquire_connection() as tdconn:
        def _run():
            _set_queryband(tdconn, "visualize_sql_steps")
            # Step 1: resolve host id + logon PE for the target session via the
            # caller's own session list (same path as show_sql_steps_for_session).
            resolver = tdconn.cursor()
            resolver.execute(
                "SELECT HostId, LogonPENo FROM TABLE (monitormysessions()) as t1 "
                "where SessionNo = ?",
                [session_no],
            )
            head = resolver.fetchall()
            if not head:
                return None, [], None, None
            host_id = int(head[0][0])
            logon_pe = int(head[0][1])
            # Step 2: pull plan steps. Column aliases match the existing tool.
            query = (
                "select "
                "SQLStep, "
                "StepNum (format '99') Num, "
                "Confidence (format '9') C, "
                "EstRowCount (format '-99999999') ERC, "
                "ActRowCount (format '99999999') ARC, "
                "EstRowCountSkew (format '-99999999') ERCS, "
                "ActRowCountSkew (format '99999999') ARCS, "
                "EstRowCountSkewMatch (format '-99999999') ERCSM, "
                "ActRowCountSkewMatch (format '99999999') ARCSM, "
                "EstElapsedTime (format '99999') EET, "
                "ActElapsedTime (format '99999') AET "
                f"from table (MonitorSQLSteps({host_id},{session_no},{logon_pe})) as t2"
            )
            steps = tdconn.cursor()
            steps.execute(query)
            return steps.description, list(steps.fetchall()), host_id, logon_pe
        description, rows, host_id, logon_pe = await asyncio.to_thread(_run)

    if host_id is None:
        # No matching session — return an empty graph rather than an error so
        # the UI bundle still renders cleanly.
        graph = build_explain_graph([], session_no=session_no)
        summary = types.TextContent(
            type="text",
            text=f"visualize_sql_steps: session {session_no} not found",
        )
        return ([summary], graph)

    _cols, dicts = _rows_as_dicts(description, rows)
    graph = build_explain_graph(dicts, session_no=session_no)
    meta = graph["meta"]
    summary = types.TextContent(
        type="text",
        text=(
            f"visualize_sql_steps: session {session_no} · "
            f"{meta['total_steps']} steps · "
            f"est {meta['est_elapsed_cs']:,} cs · "
            f"max est rows {meta['max_est_rows']:,}"
        ),
    )
    return ([summary], graph)


# Dispatcher -------------------------------------------------------------------


async def handle_visualize_tool_call(
    name: str, arguments: dict[str, Any] | None
) -> ToolResult | None:
    """Dispatch a ``visualize_*`` call.

    Returns ``None`` if ``name`` is not a visualize tool — lets the main
    dispatcher fall through to its existing tools.
    """
    args = arguments or {}
    if name == "visualize_ping":
        return await _visualize_ping()
    if name == "visualize_tdwm_summary":
        return await _visualize_tdwm_summary()
    if name == "visualize_top_users":
        return await _visualize_top_users(args.get("type", "ALL"))
    if name == "visualize_throttle_statistics":
        return await _visualize_throttle_statistics(args.get("type", "ALL"))
    if name == "visualize_tasm_statistics":
        return await _visualize_tasm_statistics()
    if name == "visualize_tasm_event_history":
        return await _visualize_tasm_event_history()
    if name == "visualize_delay_queue":
        return await _visualize_delay_queue(args.get("type", "ALL"))
    if name == "visualize_amp_load":
        return await _visualize_amp_load()
    if name == "visualize_awt":
        return await _visualize_awt()
    if name == "visualize_query_log":
        return await _visualize_query_log(args["user"])
    if name == "visualize_sql_steps":
        return await _visualize_sql_steps(args["sessionNo"])
    return None
