import argparse
import asyncio
import logging
import os
import signal
import re
import teradatasql
import yaml
from urllib.parse import urlparse
from pydantic import AnyUrl
from typing import Literal
from typing import Any
from typing import List
import io
from contextlib import redirect_stdout
import mcp.server.stdio
import mcp.types as types
from mcp.server import NotificationOptions, Server
from mcp.server.models import InitializationOptions
from mcp.server.stdio import stdio_server
from .tdsql import obfuscate_password
from .tdsql import TDConn



logger = logging.getLogger(__name__)
ResponseType = List[types.TextContent | types.ImageContent | types.EmbeddedResource]
_tdconn = TDConn()

def format_text_response(text: Any) -> ResponseType:
    """Format a text response."""
    return [types.TextContent(type="text", text=str(text))]


def format_error_response(error: str) -> ResponseType:
    """Format an error response."""
    return format_text_response(f"Error: {error}")
logger = logging.getLogger("teradata_mcp")

async def list_sessions() -> ResponseType:
    """Show my sessions"""
    try:
        global _tdconn
        cur = _tdconn.cursor()
        rows = cur.execute("SELECT * FROM TABLE (monitormysessions()) as t1")
        return format_text_response(list([row for row in rows.fetchall()]))
    except Exception as e:
        logger.error(f"Error showing sessions: {e}")
        return format_error_response(str(e))

async def monitor_amp_load() -> ResponseType:
    """Monitor AMP load"""
    try:
        global _tdconn
        cur = _tdconn.cursor()
        rows = cur.execute("SELECT * FROM TABLE (MonitorAMPLoad()) AS t1")
        return format_text_response(list([row for row in rows.fetchall()]))
    except Exception as e:
        logger.error(f"Error showing AMPs: {e}")
        return format_error_response(str(e))

async def monitor_awt() -> ResponseType:
    """Monitor AWT (Amp Worker Tasks) resources """
    try:
        global _tdconn
        cur = _tdconn.cursor()
        rows = cur.execute("SELECT * FROM TABLE (MonitorAWTResource(1,2,3,4)) AS t1")
        return format_text_response(list([row for row in rows.fetchall()]))
    except Exception as e:
        logger.error(f"Error showing AMPs: {e}")
        return format_error_response(str(e))

async def monitor_config() -> ResponseType:
    """Monitor Teradata config """
    try:
        global _tdconn
        cur = _tdconn.cursor()
        rows = cur.execute("SELECT t2.* FROM TABLE (MonitorVirtualConfig()) AS t2")
        return format_text_response(list([row for row in rows.fetchall()]))
    except Exception as e:
        logger.error(f"Error showing AMPs: {e}")
        return format_error_response(str(e))

async def list_resources() -> ResponseType:
    """Show physical resources"""
    try:
        global _tdconn
        cur = _tdconn.cursor()
        rows = cur.execute("SELECT t2.* from table (MonitorPhysicalResource()) as t2")
        return format_text_response(list([row for row in rows.fetchall()]))
    except Exception as e:
        logger.error(f"Error showing sessions: {e}")
        return format_error_response(str(e))

async def identify_blocking() -> ResponseType:
    """Identify blocking users"""
    try:
        global _tdconn
        cur = _tdconn.cursor()
        rows = cur.execute("""
            SELECT 
                IdentifyUser(blk1userid) as "blocking user",
                IdentifyTable(blk1objtid) as "blocking table",
                IdentifyDatabase(blk1objdbid) as "blocking db"
            FROM TABLE (MonitorSession(-1,'*',0)) AS t1
            WHERE Blk1UserId > 0""")
        return format_text_response(list([row for row in rows.fetchall()]))
    except Exception as e:
        logger.error(f"Error showing sessions: {e}")
        return format_error_response(str(e))

async def abort_sessions_user(usr: str) -> ResponseType:
    """Abort sessions for a user {usr}"""
    try:
        global _tdconn
        cur = _tdconn.cursor()
        rows = cur.execute("""
            SELECT AbortSessions (HostId, UserName, SessionNo, 'Y', 'Y')
            FROM TABLE (MonitorSession(-1, '*', 0)) AS t1
            WHERE username= ?""", [usr])
        return format_text_response(list([row for row in rows.fetchall()]))
    except Exception as e:
        logger.error(f"Error showing sessions: {e}")
        return format_error_response(str(e))

async def list_active_WD() -> ResponseType:
    """List active workloads (WD)"""
    try:
        global _tdconn
        cur = _tdconn.cursor()
        rows = cur.execute("""sel * from table (tdwm.TDWMActiveWDs()) as t1""")
        return format_text_response(list([row for row in rows.fetchall()]))
    except Exception as e:
        logger.error(f"Error showing sessions: {e}")
        return format_error_response(str(e))

async def list_WDs() -> ResponseType:
    """List workloads (WD)"""
    try:
        global _tdconn
        cur = _tdconn.cursor()
        rows = cur.execute("""SELECT * FROM TABLE (TDWM.TDWMListWDs('Y')) AS t1""")
        return format_text_response(list([row for row in rows.fetchall()]))
    except Exception as e:
        logger.error(f"Error showing sessions: {e}")
        return format_error_response(str(e))


async def show_session_sql_steps(SessionNo: int) -> ResponseType:
    """Show sql steps for a session {SessionNo}"""
    try:
        global _tdconn
        cur = _tdconn.cursor()
        rows = cur.execute("SELECT HostId, LogonPENo FROM TABLE (monitormysessions()) as t1 where SessionNo = ?", [SessionNo])
        row = rows.fetchall()[0]
        hostId = row[0]
        logonPENo = row[1]
        query = """
            select 
                SQLStep,
                StepNum (format '99') Num,
                Confidence (format '9') C,
                EstRowCount (format '-99999999') ERC,
                ActRowCount (format '99999999') ARC,
                EstRowCountSkew (format '-99999999') ERCS,
                ActRowCountSkew (format '99999999') ARCS,
                EstRowCountSkewMatch (format '-99999999') ERCSM,
                ActRowCountSkewMatch (format '99999999') ARCSM,
                EstElapsedTime (format '99999') EET,
                ActElapsedTime (format '99999') AET
            from 
                table (MonitorSQLSteps({hostId},{SessionNo},{logonPENo})) as t2
            """.format(hostId=hostId, SessionNo=SessionNo, logonPENo=logonPENo)
        cur1 = _tdconn.cursor()
        rows1 = cur1.execute(query)
        return format_text_response(list([row for row in rows1.fetchall()]))
    except Exception as e:
        logger.error(f"Error showing sessions: {e}")
        return format_error_response(str(e))

async def show_session_sql_text(SessionNo: int) -> ResponseType:
    """Show sql text for a session {SessionNo}"""
    try:
        global _tdconn
        cur = _tdconn.cursor()
        rows = cur.execute("SELECT HostId, LogonPENo FROM TABLE (monitormysessions()) as t1 where SessionNo = ?", [SessionNo])
        row = rows.fetchall()[0]
        hostId = row[0]
        logonPENo = row[1]
        query = "SELECT SQLTxt FROM TABLE (MonitorSQLText({hostId},{SessionNo},{logonPENo})) as t2".format(hostId=hostId, SessionNo=SessionNo, logonPENo=logonPENo)
        cur1 = _tdconn.cursor()
        rows1 = cur1.execute(query)
        return format_text_response(list([row for row in rows1.fetchall()]))
    except Exception as e:
        logger.error(f"Error showing sessions: {e}")
        return format_error_response(str(e))

async def list_delayed_request(SessionNo: int) -> ResponseType:
    """List all of the delayed queries"""
    try:
        global _tdconn
        cur = _tdconn.cursor()
        rows = cur.execute("""
            SELECT * FROM TABLE (TDWM.TDWMGetDelayedQueries('O')) AS t1""")
        return format_text_response(list([row for row in rows.fetchall()]))
    except Exception as e:
        logger.error(f"Error showing sessions: {e}")
        return format_error_response(str(e))


async def abort_delayed_request(SessionNo: int) -> ResponseType:
    """Abort delay requests on session {SessionNo}"""
    try:
        global _tdconn
        cur = _tdconn.cursor()
        rows = cur.execute("""
            SELECT TDWM.TDWMAbortDelayedRequest(HostId, SessionNo, RequestNo, 0)
            FROM TABLE (TDWM.TDWMGetDelayedQueries('O')) AS t1
            WHERE SessionNo=?""",[SessionNo])
        return format_text_response(list([row for row in rows.fetchall()]))
    except Exception as e:
        logger.error(f"Error showing sessions: {e}")
        return format_error_response(str(e))

async def list_utility_stats() -> ResponseType:
    """List statistics for use utilitites"""
    try:
        global _tdconn
        cur = _tdconn.cursor()
        rows = cur.execute("""
            SELECT * FROM TABLE (TDWM.TDWMLoadUtilStatistics()) AS t1""")
        return format_text_response(list([row for row in rows.fetchall()]))
    except Exception as e:
        logger.error(f"Error showing sessions: {e}")
        return format_error_response(str(e))

async def display_delay_queue(Type: str) -> ResponseType:
    """Display {Type} delay queue details"""
    try:
        global _tdconn
        cur = _tdconn.cursor()
        if Type.upper == "WORKLOAD":
            rows = cur.execute("""
                SELECT * FROM TABLE (TDWM.TDWMGetDelayedQueries('W')) AS t1;""")
        elif Type.upper == "SYSTEM":
            rows = cur.execute("""
                SELECT * FROM TABLE (TDWM.TDWMGetDelayedQueries('O')) AS t1""")
        elif Type.upper == "UTILITY":
            rows = cur.execute("""
                SELECT * FROM TABLE (TDWM.TDWMGetDelayedUtilities()) AS t1;""")
        else:
            rows = cur.execute("""
                SELECT * FROM TABLE (TDWM.TDWMGetDelayedQueries('A')) AS t1""")
        return format_text_response(list([row for row in rows.fetchall()]))
    except Exception as e:
        logger.error(f"Error showing sessions: {e}")
        return format_error_response(str(e))

async def release_delay_queue(SessionNo: int, UserName: str) -> ResponseType:
    """Releases a request or utility session in the queue for session or user"""
    try:
        global _tdconn
        cur = _tdconn.cursor()
        if SessionNo:
            rows = cur.execute("""
                SELECT TDWM.TDWMReleaseDelayedRequest(HostId, SessionNo, RequestNo, 0)
                FROM TABLE (TDWMGetDelayedQueries('O')) AS t1
                WHERE SessionNo=?""",[SessionNo])
        elif UserName:
            rows = cur.execute("""
                SELECT TDWM.TDWMReleaseDelayedRequest(HostId, SessionNo, RequestNo, 0)
                FROM TABLE (TDWMGetDelayedQueries('O')) AS t1
                WHERE t1.Username=?""",[UserName])
        return format_text_response(list([row for row in rows.fetchall()]))
    except Exception as e:
        logger.error(f"Error showing sessions: {e}")
        return format_error_response(str(e))

async def show_tdwm_summary() -> ResponseType:
    """Show workloads summary information"""
    try:
        global _tdconn
        cur = _tdconn.cursor()
        rows = cur.execute("""SELECT * FROM TABLE (TDWM.TDWMSummary()) AS t2""")
        return format_text_response(list([row for row in rows.fetchall()]))
    except Exception as e:
        logger.error(f"Error showing sessions: {e}")
        return format_error_response(str(e))
    
async def show_trottle_statistics(type: str) -> ResponseType:
    """Show throttle statistics for {type}"""
    try:
        global _tdconn
        cur = _tdconn.cursor()
        if type.upper() == "ALL":
            rows = cur.execute("""SELECT * FROM TABLE (TDWM.TDWMTHROTTLESTATISTICS('A')) AS t1""")
        elif type.upper() == "QUERY":
            rows = cur.execute("""SELECT * FROM TABLE (TDWM.TDWMTHROTTLESTATISTICS('Q')) AS t1""")
        elif type.upper() == "SESSION":
            rows = cur.execute("""SELECT * FROM TABLE (TDWM.TDWMTHROTTLESTATISTICS('S')) AS t1""")
        elif type.upper() == "WORKLOAD":
            rows = cur.execute("""SELECT * FROM TABLE (TDWM.TDWMTHROTTLESTATISTICS('W')) AS t1""")
        else:
            rows = cur.execute("""
                    SELECT ObjectType(FORMAT 'x(10)'), rulename(FORMAT 'x(17)'),
                        ObjectName(FORMAT 'x(13)'), active(FORMAT 'Z9'),
                        throttlelimit as ThrLimit, delayed(FORMAT 'Z9'), throttletype as ThrType
                    FROM TABLE (TDWM.TDWMTHROTTLESTATISTICS('A')) AS t1
                    ORDER BY 1,2""")     
        return format_text_response(list([row for row in rows.fetchall()]))
    except Exception as e:
        logger.error(f"Error showing sessions: {e}")
        return format_error_response(str(e))

async def main():
    logger.info("Starting Teradata Workload Management MCP Server")
    server = Server("teradata-mcp")
    logger.info("Registering handlers")
    parser = argparse.ArgumentParser(description="TDWM MCP Server")
    parser.add_argument("database_url", help="Database connection URL", nargs="?")
    args = parser.parse_args()
    database_url = os.environ.get("DATABASE_URI", args.database_url)
    parsed_url = urlparse(database_url)
    if not database_url:
        raise ValueError(
            "Error: No database URL provided. Please specify via 'DATABASE_URI' environment variable or command-line argument.",
        )
        global _tdconn
    try:
        _tdconn = TDConn(database_url)
        logger.info("Successfully connected to database and initialized connection")
    except Exception as e:
        logger.warning(
            f"Could not connect to database: {obfuscate_password(str(e))}",
        )
        logger.warning(
            "The MCP server will start but database operations will fail until a valid connection is established.",
        )

    logger.info("Registering handlers")
        
    @server.list_tools()
    async def handle_list_tools() -> list[types.Tool]:
        """
        List available tools.
        Each tool specifies its arguments using JSON Schema validation.
        """
        logger.info("Listing tools")
        return [
            types.Tool(
                name="show_sessions",
                description="Show my sessions",
                inputSchema={
                    "type": "object",
                    "properties": {},
                },
            ),
            types.Tool(
                name="show_physical_resources",
                description="Monitor system resources",
                inputSchema={
                    "type": "object",
                    "properties": {},
                },
            ),
            types.Tool(
                name="monitor_amp_load",
                description="Monitor AMP load",
                inputSchema={
                    "type": "object",
                    "properties": {},
                },
            ),
              types.Tool(
                name="monitor_awt",
                description="Monitor AWT (Amp Worker Task) resources",
                inputSchema={
                    "type": "object",
                    "properties": {},
                },
            ),
            types.Tool(
                name="monitor_config",
                description="Monitor virtual config",
                inputSchema={
                    "type": "object",
                    "properties": {},
                },
            ),
            types.Tool(
                name="show_sql_steps_for_session",
                description="Show SQL steps for a session",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "sessionNo": {
                            "type": "integer",
                            "description": "Session Number",
                        },
                    },
                    "required": ["sessionNo"],
                },
            ),
            types.Tool(
                name="show_sql_text_for_session",
                description="Show SQL text for a session",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "sessionNo": {
                            "type": "integer",
                            "description": "Session Number",
                        },
                    },
                    "required": ["sessionNo"],
                },
            ),
            types.Tool(
                name="identify_blocking",
                description="Identify blocking users",
                inputSchema={
                    "type": "object",
                    "properties": {},
                },
            ),
            types.Tool(
                name="list_active_WD",
                description="List active workloads (WD)",
                inputSchema={
                    "type": "object",
                    "properties": {},
                },
            ),
            types.Tool(
                name="list_WD",
                description="List workloads (WD)",
                inputSchema={
                    "type": "object",
                    "properties": {},
                },
            ),
            types.Tool(
                name="abort_sessions_user",
                description="Abort sessions for a user",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "user": {
                            "type": "string",
                            "description": "User name to abort",
                        },
                    },
                    "required": ["user"],
                },
            ),
            types.Tool(
                name="list_delayed_request",
                description="List all of the delayed queries",
                inputSchema={
                    "type": "object",
                    "properties": {},
                },
            ),
            types.Tool(
                name="abort_delayed_request",
                description="abort a delayed request on session {sessionNo}",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "sessionNo": {
                            "type": "integer",
                            "description": "Session Number",
                        },
                    },
                    "required": ["sessionNo"],
                },
            ),
            types.Tool(
                name="list_utility_stats",
                description="List statistics for utility use on the system",
                inputSchema={
                    "type": "object",
                    "properties": {},
                },
            ),
            types.Tool(
                name="display_delay_queue",
                description="display {type} delay queue",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "type": {
                            "type": "string",
                            "description": "type of delay queue",
                        },
                    },
                    "required": ["sessionNo"],
                },
            ),
            types.Tool(
                name="release_delay_queue",
                description="Releases a request or utility session in the queue for session {sessionNo} or user {userName}",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "sessionNo": {
                            "type": "integer",
                            "description": "Session Number",
                        },
                        "userName": {
                            "type": "string",
                            "description": "User name to release",
                        },
                    },
                    "required": [],
                },
            ), 
            types.Tool(
                name="show_tdwm_summary",
                description="Show workloads summary information",
                inputSchema={
                    "type": "object",
                    "properties": {},
                },
            ),       
            types.Tool(
                name="show_trottle_statistics",
                description="Show throttle statistics for {type}",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "type": {
                            "type": "string",
                            "description": "Type of throttle statistics",
                        },
                    },
                    "required": [],
                },
            ),    
        ]
    
    @server.call_tool()
    async def handle_tool_call(
        name: str, arguments: dict | None
    ) -> list[types.TextContent | types.ImageContent | types.EmbeddedResource]:
        """
        Handle tool execution requests.
        Tools can modify server state and notify clients of changes.
        """
        logger.info(f"Calling tool: {name}::{arguments}")
        try:
            if name == "show_sessions":
                tool_response = await list_sessions()
                return tool_response
            elif name == "show_physical_resources":
                tool_response = await list_resources()
                return tool_response
            elif name == "monitor_amp_load":
                tool_response = await monitor_amp_load()
                return tool_response
            elif name == "monitor_awt":
                tool_response = await monitor_awt()
                return tool_response
            elif name == "monitor_config":
                tool_response = await monitor_config()
                return tool_response
            elif name == "show_sql_steps_for_session":
                tool_response = await show_session_sql_steps(arguments["sessionNo"])
                return tool_response
            elif name == "show_sql_text_for_session":
                tool_response = await show_session_sql_text(arguments["sessionNo"])
                return tool_response
            elif name == "identify_blocking":
                tool_response = await identify_blocking()
                return tool_response
            elif name == "abort_sessions_user":
                tool_response = await abort_sessions_user(arguments["user"])
                return tool_response
            elif name == "list_active_WD":
                tool_response = await list_active_WD()
                return tool_response
            elif name == "list_WD":
                tool_response = await list_WDs()
                return tool_response
            elif name == "list_delayed_request":
                tool_response = await list_delayed_request()
                return tool_response
            elif name == "abort_delayed_request":
                tool_response = await abort_delayed_request(arguments["sessionNo"])
                return tool_response
            elif name == "list_utility_stats":
                tool_response = await list_utility_stats()
                return tool_response
            elif name == "display_delay_queue":
                tool_response = await display_delay_queue(arguments["type"])
                return tool_response
            elif name == "release_delay_queue":
                tool_response = await release_delay_queue(arguments["sessionNo"], arguments["userName"])
                return tool_response
            elif name == "show_tdwm_summary":
                tool_response = await show_tdwm_summary()
                return tool_response
            elif name == "show_trottle_statistics":
                tool_response = await show_trottle_statistics(arguments["type"])
                return tool_response
            return [types.TextContent(type="text", text=f"Unsupported tool: {name}")]

        except Exception as e:
            logger.error(f"Error executing tool {name}: {e}")
            raise ValueError(f"Error executing tool {name}: {str(e)}")
        
    async with stdio_server() as (read_stream, write_stream):
        logger.info("Server running with stdio transport")
        await server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="tdwm-mcp",
                server_version="0.1.0",
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(),
                    experimental_capabilities={},
                ),
            ),
        )

if __name__ == "__main__":
    main()
