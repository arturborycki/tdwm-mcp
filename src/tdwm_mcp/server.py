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

async def execute_query(query: str) -> ResponseType:
    """Execute a SQL query and return results as a list """
    logger.debug(f"Executing query: {query}")
    global _tdconn
    try:
        cur = _tdconn.cursor()
        rows = cur.execute(query)
        if rows is None:
            return format_text_response("No results")
        return format_text_response(list([row for row in rows.fetchall()]))
    except Exception as e:
        logger.error(f"Error executing query: {e}")
        return format_error_response(str(e))
    except Exception as e:
        logger.error(f"Database error executing query: {e}")
        raise


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

async def show_session_sql_steps(SessionNo: int) -> ResponseType:
    """Show sql steps for a session {SessionNo}"""
    try:
        global _tdconn
        cur = _tdconn.cursor()
        rows = cur.execute("SELECT HostId, LogonPENo FROM TABLE (monitormysessions()) as t1 where SessionNo = ?", (SessionNo,))
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
        rows = cur.execute("SELECT HostId, LogonPENo FROM TABLE (monitormysessions()) as t1 where SessionNo = ?", (SessionNo,))
        row = rows.fetchall()[0]
        hostId = row[0]
        logonPENo = row[1]
        query = "SELECT SQLTxt FROM TABLE (MonitorSQLText(({hostId},{SessionNo},{logonPENo})) as t2".format(hostId=hostId, SessionNo=SessionNo, logonPENo=logonPENo)
        cur1 = _tdconn.cursor()
        rows1 = cur1.execute(query)
        return format_text_response(list([row for row in rows1.fetchall()]))
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
                name="query",
                description="Executes a SQL query against the Teradata database",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "SQL query to execute that is a dialect of Teradata SQL",
                        },
                    },
                    "required": ["query"],
                },
            ),
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
            if name == "query":
                if arguments is None:
                    return [
                        types.TextContent(type="text", text="Error: No query provided")
                    ]
                tool_response = await execute_query(arguments["query"])
                return tool_response
            elif name == "show_sessions":
                tool_response = await list_sessions()
                return tool_response
            elif name == "show_physical_resources":
                tool_response = await list_resources()
                return tool_response
            elif name == "show_sql_steps_for_session":
                tool_response = await show_session_sql_steps(arguments["sessionNo"])
                return tool_response
            elif name == "show_sql_text_for_session":
                tool_response = await show_session_sql_steps(arguments["sessionNo"])
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
