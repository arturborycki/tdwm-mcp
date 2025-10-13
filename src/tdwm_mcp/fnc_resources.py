"""
MCP Resource Functions for TDWM Operations

This module contains all the resource functions that are exposed through the MCP server.
Provides access to database schemas, tables, and TDWM configuration as resources.
"""

import logging
from typing import Any
import mcp.types as types
import os
from urllib.parse import urlparse
from .connection_manager import TeradataConnectionManager

logger = logging.getLogger(__name__)

# Global connection variables
_connection_manager = None
_db = ""


def set_resource_connection(connection_manager, db: str):
    """Set the global database connection manager and database name."""
    global _connection_manager, _db
    _connection_manager = connection_manager
    _db = db


# Try lazy creation of a connection manager from `DATABASE_URI` if available
if not _connection_manager:
    database_url = os.environ.get("DATABASE_URI")
    if database_url:
        try:
            parsed_url = urlparse(database_url)
            _db = parsed_url.path.lstrip('/')
            _connection_manager = TeradataConnectionManager(
                database_url=database_url,
                db_name=_db
            )
            logger.info("TeradataConnectionManager (resources) created from DATABASE_URI environment variable")
        except Exception:
            _connection_manager = None


def format_text_response(text: Any) -> str:
    """Format a text response."""
    return str(text)


def format_error_response(error: str) -> str:
    """Format an error response."""
    return f"Error: {error}"


async def get_connection():
    """Get a healthy database connection."""
    global _connection_manager
    
    if not _connection_manager:
        raise ConnectionError("Database connection not initialized")
    
    return await _connection_manager.ensure_connection()


async def handle_list_resources() -> list[types.Resource]:
    """List available resources."""
    logger.debug("Handling list_resources request")
    
    resources = [
        types.Resource(
            uri="tdwm://sessions",
            name="Current Sessions",
            description="List of current database sessions",
            mimeType="application/json"
        ),
        types.Resource(
            uri="tdwm://workloads",
            name="TDWM Workloads", 
            description="List of TDWM workloads (WD)",
            mimeType="application/json"
        ),
        types.Resource(
            uri="tdwm://active-workloads",
            name="Active TDWM Workloads",
            description="List of active TDWM workloads",
            mimeType="application/json"
        ),
        types.Resource(
            uri="tdwm://summary",
            name="TDWM Summary",
            description="TDWM system summary information",
            mimeType="application/json"
        ),
        types.Resource(
            uri="tdwm://delayed-queries", 
            name="Delayed Queries",
            description="List of delayed queries in the system",
            mimeType="application/json"
        ),
        types.Resource(
            uri="tdwm://throttle-statistics",
            name="Throttle Statistics",
            description="System throttle statistics",
            mimeType="application/json"
        ),
        types.Resource(
            uri="tdwm://physical-resources",
            name="Physical Resources",
            description="Physical system resource information",
            mimeType="application/json"
        ),
        types.Resource(
            uri="tdwm://amp-load",
            name="AMP Load",
            description="AMP load monitoring information", 
            mimeType="application/json"
        ),
        types.Resource(
            uri="tdwm://classification-types",
            name="TDWM Classification Types",
            description="Available TDWM/TASM classification types",
            mimeType="application/json"
        )
    ]
    
    return resources


async def handle_read_resource(uri: str) -> str:
    """Read a specific resource."""
    logger.debug(f"Handling read_resource request for: {uri}")
    
    try:
        if uri == "tdwm://sessions":
            return await _get_sessions_resource()
        elif uri == "tdwm://workloads":
            return await _get_workloads_resource()
        elif uri == "tdwm://active-workloads":
            return await _get_active_workloads_resource()
        elif uri == "tdwm://summary":
            return await _get_summary_resource()
        elif uri == "tdwm://delayed-queries":
            return await _get_delayed_queries_resource()
        elif uri == "tdwm://throttle-statistics":
            return await _get_throttle_statistics_resource()
        elif uri == "tdwm://physical-resources":
            return await _get_physical_resources_resource()
        elif uri == "tdwm://amp-load":
            return await _get_amp_load_resource()
        elif uri == "tdwm://classification-types":
            return await _get_classification_types_resource()
        else:
            raise ValueError(f"Unknown resource URI: {uri}")
            
    except Exception as e:
        logger.error(f"Error reading resource {uri}: {e}")
        return format_error_response(str(e))


async def _get_sessions_resource() -> str:
    """Get current sessions resource."""
    try:
        global _connection_manager
        if not _connection_manager:
            return format_error_response("Database connection not initialized")
        
        tdconn = await _connection_manager.ensure_connection()
        cur = tdconn.cursor()
        rows = cur.execute("SELECT * FROM TABLE (monitormysessions()) as t1")
        result = list([row for row in rows.fetchall()])
        return format_text_response(result)
    except Exception as e:
        logger.error(f"Error getting sessions resource: {e}")
        return format_error_response(str(e))


async def _get_workloads_resource() -> str:
    """Get workloads resource."""
    try:
        global _connection_manager
        if not _connection_manager:
            return format_error_response("Database connection not initialized")
        
        tdconn = await _connection_manager.ensure_connection()
        cur = tdconn.cursor()
        rows = cur.execute("SELECT * FROM TABLE (TDWM.TDWMListWDs('Y')) AS t1")
        result = list([row for row in rows.fetchall()])
        return format_text_response(result)
    except Exception as e:
        logger.error(f"Error getting workloads resource: {e}")
        return format_error_response(str(e))


async def _get_active_workloads_resource() -> str:
    """Get active workloads resource.""" 
    try:
        global _connection_manager
        if not _connection_manager:
            return format_error_response("Database connection not initialized")
        
        tdconn = await _connection_manager.ensure_connection()
        cur = tdconn.cursor()
        rows = cur.execute("sel * from table (tdwm.TDWMActiveWDs()) as t1")
        result = list([row for row in rows.fetchall()])
        return format_text_response(result)
    except Exception as e:
        logger.error(f"Error getting active workloads resource: {e}")
        return format_error_response(str(e))


async def _get_summary_resource() -> str:
    """Get TDWM summary resource."""
    try:
        global _connection_manager
        if not _connection_manager:
            return format_error_response("Database connection not initialized")
        
        tdconn = await _connection_manager.ensure_connection()
        cur = tdconn.cursor()
        rows = cur.execute("SELECT * FROM TABLE (TDWM.TDWMSummary()) AS t2")
        result = list([row for row in rows.fetchall()])
        return format_text_response(result)
    except Exception as e:
        logger.error(f"Error getting summary resource: {e}")
        return format_error_response(str(e))


async def _get_delayed_queries_resource() -> str:
    """Get delayed queries resource."""
    try:
        global _connection_manager
        if not _connection_manager:
            return format_error_response("Database connection not initialized")
        
        tdconn = await _connection_manager.ensure_connection()
        cur = tdconn.cursor()
        rows = cur.execute("SELECT * FROM TABLE (TDWM.TDWMGetDelayedQueries('O')) AS t1")
        result = list([row for row in rows.fetchall()])
        return format_text_response(result)
    except Exception as e:
        logger.error(f"Error getting delayed queries resource: {e}")
        return format_error_response(str(e))


async def _get_throttle_statistics_resource() -> str:
    """Get throttle statistics resource."""
    try:
        global _connection_manager
        if not _connection_manager:
            return format_error_response("Database connection not initialized")
        
        tdconn = await _connection_manager.ensure_connection()
        cur = tdconn.cursor()
        rows = cur.execute("SELECT * FROM TABLE (TDWM.TDWMTHROTTLESTATISTICS('A')) AS t1")
        result = list([row for row in rows.fetchall()])
        return format_text_response(result)
    except Exception as e:
        logger.error(f"Error getting throttle statistics resource: {e}")
        return format_error_response(str(e))


async def _get_physical_resources_resource() -> str:
    """Get physical resources resource."""
    try:
        global _connection_manager
        if not _connection_manager:
            return format_error_response("Database connection not initialized")
        
        tdconn = await _connection_manager.ensure_connection()
        cur = tdconn.cursor()
        rows = cur.execute("SELECT t2.* from table (MonitorPhysicalResource()) as t2")
        result = list([row for row in rows.fetchall()])
        return format_text_response(result)
    except Exception as e:
        logger.error(f"Error getting physical resources resource: {e}")
        return format_error_response(str(e))


async def _get_amp_load_resource() -> str:
    """Get AMP load resource."""
    try:
        global _connection_manager
        if not _connection_manager:
            return format_error_response("Database connection not initialized")
        
        tdconn = await _connection_manager.ensure_connection()
        cur = tdconn.cursor()
        rows = cur.execute("SELECT * FROM TABLE (MonitorAMPLoad()) AS t1")
        result = list([row for row in rows.fetchall()])
        return format_text_response(result)
    except Exception as e:
        logger.error(f"Error getting AMP load resource: {e}")
        return format_error_response(str(e))


async def _get_classification_types_resource() -> str:
    """Get classification types resource."""
    try:
        from .tdwm_static import TDWM_CLASIFICATION_TYPE
        result = list([(entry[1], entry[2], entry[3], entry[4]) for entry in TDWM_CLASIFICATION_TYPE])
        return format_text_response(result)
    except Exception as e:
        logger.error(f"Error getting classification types resource: {e}")
        return format_error_response(str(e))