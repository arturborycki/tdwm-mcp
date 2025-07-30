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
from .tdwm_static import TDWM_CLASIFICATION_TYPE
from .prompt import PROMPTS


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

async def monitor_session_query_band(SessionNo: int) -> ResponseType:
    """Monitor query band for session {SessionNo}"""
    try:
        global _tdconn
        cur = _tdconn.cursor()
        rows = cur.execute("SELECT HostId, LogonPENo FROM TABLE (monitormysessions()) as t1 where SessionNo = ?", [SessionNo])
        row = rows.fetchall()[0]
        hostId = row[0]
        logonPENo = row[1]
        query = """
            SELECT MonitorQueryBand({hostId},{SessionNo},{logonPENo})
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
                SELECT * FROM TABLE (TDWM.TDWMGetDelayedUtilities()) AS t1""")
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
    
async def list_query_band(Type: str) -> ResponseType:
    """List query band for {Type}"""
    try:
        global _tdconn
        cur = _tdconn.cursor()
        if Type.upper == "TRANSACTION":
            rows = cur.execute("""
                SELECT * FROM TABLE(GetQueryBandPairs(1)) AS t1""")
        elif Type.upper == "PROFILE":
            rows = cur.execute("""
                SELECT * FROM TABLE(GetQueryBandPairs(3)) AS t1""")
        elif Type.upper == "SESSION":
            rows = cur.execute("""
                SELECT * FROM TABLE(GetQueryBandPairs(2)) AS t1""")
        else:
            rows = cur.execute("""
                SELECT * FROM TABLE(GetQueryBandPairs(0)) AS t1""")
        return format_text_response(list([row for row in rows.fetchall()]))
    except Exception as e:
        logger.error(f"Error showing sessions: {e}")
        return format_error_response(str(e))

async def show_query_log(User: str) -> ResponseType:
    """Show query log for user {User}"""
    try:
        global _tdconn
        cur = _tdconn.cursor()
        rows = cur.execute("""
                sel * from dbc.qrylogv where upper(username)=upper(?) and trunc(collectTimeStamp) = trunc(date) ORDER BY queryid""", [User])
        return format_text_response(list([row for row in rows.fetchall()]))
    except Exception as e:
        logger.error(f"Error showing sessions: {e}")
        return format_error_response(str(e))

async def show_cod_limits() -> ResponseType:
    """Show COD (Capacity On Demand) limits"""
    try:
        global _tdconn
        cur = _tdconn.cursor()
        rows = cur.execute("""
                SELECT * FROM TABLE (TD_SYSFNLIB.TD_get_COD_Limits( ) ) As d""")
        return format_text_response(list([row for row in rows.fetchall()]))
    except Exception as e:
        logger.error(f"Error showing sessions: {e}")
        return format_error_response(str(e))
    
async def tdwm_list_clasification() -> ResponseType:
    """List clasification types for workload (TASM) rule"""
    return format_text_response(list([(entry[1], entry[2], entry[3], entry[4]) for entry in TDWM_CLASIFICATION_TYPE]))

async def show_top_users(type: str) -> ResponseType:
    """Show {type} users using resources"""
    try:
        global _tdconn
        cur = _tdconn.cursor()
        if type.upper() == "TOP":
            query = """
                Sel top 15 Username (Format 'x(10)'), queryband(Format 'x(40)'),AppID, ClientAddr, StartTime, AMPCPUTime, QueryText from dbc.qrylogV
                where ampcputime > .154 order by ampcputime desc"""
        else:
            query = """
                Sel Username (Format 'x(10)'), queryband(Format 'x(40)'),AppID, ClientAddr, StartTime, AMPCPUTime, QueryText from dbc.qrylogV
                where ampcputime > .154 order by ampcputime desc"""
        rows = cur.execute(query)
        return format_text_response(list([row for row in rows.fetchall()]))
    except Exception as e:
        logger.error(f"Error showing sessions: {e}")
        return format_error_response(str(e))

async def show_sw_event_log(type: str) -> ResponseType:
    """Show {type} event log """
    try:
        global _tdconn
        cur = _tdconn.cursor()
        if type.upper() == "OPERATIONAL":
            query = """SELECT top 20
                TheDate, 
                TheTime, 
                Event_Tag, 
                Category, 
                Severity, 
                Text,
                PMA, 
                Vproc, 
                Partition, 
                Task, 
                TheFunction, 
                SW_Version, 
                Line 
            FROM 
                DBC.SW_EVENT_LOG  
            WHERE
                (trunc(TheDate) between trunc(date-7) and trunc(date)) and
                theFunction IS NOT NULL AND
                Text LIKE '%operational%'
            ORDER BY 
                TheDate desc, TheTime desc;"""
        else:
            query = """SELECT top 20
                TheDate, 
                TheTime, 
                Event_Tag, 
                Category, 
                Severity, 
                Text,
                PMA, 
                Vproc, 
                Partition, 
                Task, 
                TheFunction, 
                SW_Version, 
                Line 
            FROM 
                DBC.SW_EVENT_LOG  
            WHERE
                (trunc(TheDate) between trunc(date-1) and trunc(date)) and
                theFunction IS NOT NULL AND
                Text LIKE '%operational%' or Text LIKE '%Event%'
            ORDER BY 
                TheDate desc, TheTime desc;"""
        rows = cur.execute(query)
        return format_text_response(list([row for row in rows.fetchall()]))
    except Exception as e:
        logger.error(f"Error showing sessions: {e}")
        return format_error_response(str(e))

async def show_tasm_statistics() -> ResponseType:
    """Show TASM statistics"""
    try:
        global _tdconn
        cur = _tdconn.cursor()
        rows = cur.execute("""
            select
                TheDatePN (FORMAT'yy/mm/dd', TITLE '// //Date'),
                TheHour (TITLE '// //Hour'),
                TheMinute (TITLE '// //Minute'),
                DayOfWeek (TITLE 'Day of Week'),
                NodeID (TITLE '//Node ID'),
                rulenamePN (TITLE '//Workload//Name'),
                ppidPN (FORMAT '9', TITLE '// //PP ID'),
                pgidPN (FORMAT 'ZZ9', TITLE '// //PG ID')
            --	average(RelWgtPN) (FORMAT 'ZZ9', TITLE 'Active//Relative// Weight')
                ,average(CPUPctPN) (FORMAT 'ZZ9.9', TITLE 'CPU//Util// %')
                ,average(PhysicalIOPN) (FORMAT 'ZZ9.9', TITLE 'Avg//I/Os//per Sec')
                ,average(PhysicalIOMBPN) (FORMAT 'ZZ9.9', TITLE 'Avg//I/O Mbytes//per Sec')
                ,average(WorkMsgSendDelayCntPN) (FORMAT 'ZZ9.9', TITLE '# AWT Requests//Successfully Sent//per AMP')
                ,average(NumRequestsPN) (FORMAT 'ZZ9.9', TITLE '# Tasks//Assigned AWTs//per AMP')
                ,average(AwtReleasesPN) (FORMAT 'ZZ9.9', TITLE '# AWTs//Released//per AMP')
                ,average(QLengthAmpAvgAPN) (FORMAT 'ZZ9.9', TITLE '# Requests//Still Waiting//for AWT')
            --	,max(QLengthMaxMPN) (FORMAT 'ZZ9.9', TITLE 'Max #//Tasks Waiting//for AWT')
                ,max(WorkMsgSendDelayMPN) (FORMAT 'ZZ9.99', TITLE 'Max//Send-Side//Wait')
                ,max(QWaitTimeMaxMPN) (FORMAT 'ZZ9.99', TITLE 'Max//Receive-Side//Wait')
                ,max(WorkMsgReceiveDelayMPN) (FORMAT 'ZZ9.99', TITLE 'Max//Receive-Side//Still Waiting')
                ,average(zeroifnull(WorkMsgSendDelayRequestAPN)) (FORMAT 'ZZ9.99', TITLE 'Avg//Send-Side//Wait')
                ,average(zeroifnull(QwaitTimeRequestAPN)) (FORMAT 'ZZ9.99', TITLE 'Avg//Receive- Side//Wait')
                ,average(zeroifnull(WorkMsgReceiveDelayRequestAPN)) (FORMAT 'ZZ9.99', TITLE 'Avg//Receive-Side//Still Waiting')
                ,max(ServiceTimeMPN) (FORMAT 'ZZ9.99', TITLE 'Max//Time//AWT Held')
                ,average(zeroifnull(ServiceTimeAPN)) (FORMAT 'ZZ9.99', TITLE 'Avg//Time//AWT Held')
                ,max(WorkTimeInUseMPN) (FORMAT 'ZZ9.99', TITLE 'Max//Time//AWT Held or Still Held')
            --	,max(WorkTypeInUseMPN) (FORMAT 'ZZ9.9', TITLE 'Pseudo-Max//AWTs//In Use')
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
                    rulename as
                    rulenamePN,
                    ppid as ppidPN,
                    pgid as pgidPN
            --		average(RelWgt) as RelWgtPN
                    ,SUM(CPUPct) as CPUPctPN
                    ,sum((PhysicalReadPerm +
                    PhysicalWritePerm+PhysicalReadOther+PhysicalWriteOther)/(CentiSecs/100)) as
                    PhysicalIOPN
                    ,sum((PhysicalReadPermKB +
                    PhysicalWritePermKB+PhysicalReadOtherKB+PhysicalWriteOtherKB)/(1024*CentiSecs/100)) as PhysicalIOMBPN
                    ,sum(WorkMsgSendDelayCnt/AmpCount) as WorkMsgSendDelayCntPN
                    ,sum(NumRequests/AmpCount) as NumRequestsPN
                    ,sum(AwtReleases/AmpCount) as AwtReleasesPN
                    ,sum(WorkMsgReceiveDelayCnt/AmpCount) as QLengthAmpAvgAPN
            --		,max(WorkMsgReceiveDelayCntMax) as QLengthMaxMPN
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
            --		,max(WorkTypeInUseMax/AmpCount) as WorkTypeInUseMPN
                FROM 
                    DBC.ResSpsView as T1
                    LEFT OUTER JOIN
                    tdwm.RuleDefs as T2
                    on (T1.WDid = T2.RuleId AND T2.RuleType =5)
                    inner join
                    sys_calendar.CALENDAR b
                    on calendar_date = thedate
                where thedate = date and active >0 group by 1,2,3,4,5,6,7,8
            ) as SumPNTbl
            group by 1,2,3,4,5,6,7,8 order by 1,2,3,4,5,6,7""")
        return format_text_response(list([row for row in rows.fetchall()]))
    except Exception as e:
        logger.error(f"Error showing sessions: {e}")
        return format_error_response(str(e))
    
async def show_tasm_even_history() -> ResponseType:
    """Show TASM event history"""
    try:
        global _tdconn
        cur = _tdconn.cursor()
        rows = cur.execute("""
            SELECT entryts,
                SUBSTR(entrykind,1,10) "kind",
                SUBSTR (entryname,1,20) "name",
                CAST (eventvalue as float format '999.9999') "evt value",
                CAST (lastvalue as float format '999.9999') "last value",
                spare2 "spare Int",
                SUBSTR (activity,1,10) "activity id",
                SUBSTR (activityname,1,20) "act name", seqno
            FROM tdwmeventhistory order by entryts, seqno""")
        return format_text_response(list([row for row in rows.fetchall()]))
    except Exception as e:
        logger.error(f"Error showing sessions: {e}")
        return format_error_response(str(e))

async def show_tasm_rule_history_red() -> ResponseType:
    """what caused the system to enter the RED state"""
    try:
        global _tdconn
        cur = _tdconn.cursor()
        rows = cur.execute("""
            WITH RECURSIVE
            CausalAnalysis(EntryTS,
            EntryKind, EntryID, EntryName, Activity,Activityid) AS
            (
            SELECT EntryTS, EntryKind, EntryID, EntryName, Activity, Activityid
            FROM DBC.TDWMEventHistory
            WHERE EntryKind = 'SYSCON' AND EntryName = 'RED' AND Activity = 'ACTIVE'
            UNION ALL
            SELECT Cause.EntryTS,Cause.EntryKind,Cause.EntryID,
                Cause.EntryName,Cause.Activity,Cause.Activityid
            FROM CausalAnalysis Condition INNER JOIN DBC.TDWMEventHistory Cause
            ON Condition.EntryKind = Cause.Activity AND
                Condition.EntryID = Cause.Activityid)
            SELECT * FROM CausalAnalysis
            ORDER BY 1 DESC""")
        return format_text_response(list([row for row in rows.fetchall()]))
    except Exception as e:
        logger.error(f"Error showing sessions: {e}")
        return format_error_response(str(e))
    
async def create_filter_rule() -> ResponseType:
    """Create filter rule"""
    try:
        global _tdconn
        cur = _tdconn.cursor()
        rows = cur.execute("")
        return format_text_response(list([row for row in rows.fetchall()]))
    except Exception as e: 
        logger.error(f"Error showing sessions: {e}")
        return format_error_response(str(e))

async def add_class_criteria() -> ResponseType:
    """Add classification criteria """
    try:
        global _tdconn
        cur = _tdconn.cursor()
        rows = cur.execute("")
        return format_text_response(list([row for row in rows.fetchall()]))
    except Exception as e: 
        logger.error(f"Error showing sessions: {e}")
        return format_error_response(str(e))

async def enable_filter_in_default() -> ResponseType:
    """Enable the filter in the default state"""
    try:
        global _tdconn
        cur = _tdconn.cursor()
        rows = cur.execute("")
        return format_text_response(list([row for row in rows.fetchall()]))
    except Exception as e: 
        logger.error(f"Error showing sessions: {e}")
        return format_error_response(str(e))
    
async def enable_filter_rule() -> ResponseType:
    """Enable the filter rule """
    try:
        global _tdconn
        cur = _tdconn.cursor()
        rows = cur.execute("")
        return format_text_response(list([row for row in rows.fetchall()]))
    except Exception as e: 
        logger.error(f"Error showing sessions: {e}")
        return format_error_response(str(e))

async def activate_rulset(RuleName: str) -> ResponseType:
    """Activate the {RuleName} ruleset with the new filter rule. """
    try:
        global _tdconn
        cur = _tdconn.cursor()
        rows = cur.execute("")
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
        cur = _tdconn.cursor()
        cur.execute("SET QUERY_BAND = 'App=GenAI' FOR SESSION;")
        logger.info("Successfully connected to database and initialized connection")
    except Exception as e:
        logger.warning(
            f"Could not connect to database: {obfuscate_password(str(e))}",
        )
        logger.warning(
            "The MCP server will start but database operations will fail until a valid connection is established.",
        )

    logger.info("Registering handlers")

    @server.list_prompts()
    async def handle_list_prompts() -> list[types.Prompt]:
        logger.debug("Handling list_prompts request")
        return []
    
    @server.get_prompt()
    async def handle_get_prompt(name: str, arguments: dict[str, str] | None) -> types.GetPromptResult:
        """Generate a prompt based on the requested type"""
        # Simple argument handling
        if arguments is None:
            arguments = {}
        else:
            raise ValueError(f"Unknown prompt: {name}")
    
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
            types.Tool(
                name="list_query_band",
                description="List query band for {type}",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "type": {
                            "type": "string",
                            "description": "Type of query band",
                        },
                    },
                    "required": [],
                },
            ),   
            types.Tool(
                name="monitor_session_query_band",
                description="Monitor query band for session {sessionNo}",
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
                name="show_query_log",
                description="Show query log for user {user}",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "user": {
                            "type": "string",
                            "description": "Session Number",
                        },
                    },
                    "required": ["user"],
                },
            ), 
            types.Tool(
                name="show_cod_limits",
                description="Show COD (Capacity On Demand) limits",
                inputSchema={
                    "type": "object",
                    "properties": {},
                },
            ),
            types.Tool(
                name="tdwm_list_clasification",
                description="List clasification types for workload (TASM) rule",
                inputSchema={
                    "type": "object",
                    "properties": {},
                },
            ),
            types.Tool(
                name="show_top_users",
                description="Show {type} users using the most resources",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "type": {
                            "type": "string",
                            "description": "top users",
                        },
                    },
                    "required": [],
                },
            ),
            types.Tool(
                name="show_sw_event_log",
                description="Show {Type} event log",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "Type": {
                            "type": "string",
                            "description": "Type of the events",
                        },
                    },
                    "required": [],
                },
            ),
            types.Tool(
                name="show_tasm_statistics",
                description="Show TASM statistics",
                inputSchema={
                    "type": "object",
                    "properties": {},
                },
            ),
            types.Tool(
                name="show_tasm_even_history",
                description="Show TASM event history",
                inputSchema={
                    "type": "object",
                    "properties": {},
                },
            ),
            types.Tool(
                name="show_tasm_rule_history_red",
                description="Show what caused the system to enter the RED state",
                inputSchema={
                    "type": "object",
                    "properties": {},
                },
            ),
            types.Tool(
                name="create_filter_rule",
                description="Create filter rule",
                inputSchema={
                    "type": "object",
                    "properties": {},
                },
            ),
            types.Tool(
                name="add_class_criteria",
                description="Add classification criteria",
                inputSchema={
                    "type": "object",
                    "properties": {},
                },
            ),
            types.Tool(
                name="enable_filter_in_default",
                description="Enable the filter in the default state",
                inputSchema={
                    "type": "object",
                    "properties": {},
                },
            ),
            types.Tool(
                name="enable_filter_rule",
                description="Enable the filter rule",
                inputSchema={
                    "type": "object",
                    "properties": {},
                },
            ),
            types.Tool(
                name="activate_rulset",
                description="Activate the {RuleName} ruleset with the new filter rule",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "RuleName": {
                            "type": "string",
                            "description": "Name of the ruleset to activate",
                        },
                    },
                    "required": ["RuleName"],
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
            elif name == "list_query_band":
                tool_response = await list_query_band(arguments["type"])
                return tool_response
            elif name == "monitor_session_query_band":
                tool_response = await monitor_session_query_band(arguments["sessionNo"])
                return tool_response
            elif name == "show_query_log":
                tool_response = await show_query_log(arguments["user"])
                return tool_response
            elif name == "show_cod_limits":
                tool_response = await show_cod_limits()
                return tool_response
            elif name == "tdwm_list_clasification":
                tool_response = await tdwm_list_clasification()
                return tool_response
            elif name == "show_top_users":
                tool_response = await show_top_users(arguments["type"])
                return tool_response
            elif name == "show_sw_event_log":
                tool_response = await show_sw_event_log(arguments["Type"])
                return tool_response
            elif name == "show_tasm_statistics":
                tool_response = await show_tasm_statistics()
                return tool_response
            elif name == "show_tasm_even_history":
                tool_response = await show_tasm_even_history()
                return tool_response
            elif name == "show_tasm_rule_history_red":
                tool_response = await show_tasm_rule_history_red()
                return tool_response
            elif name == "create_filter_rule":
                tool_response = await create_filter_rule()
                return tool_response
            elif name == "add_class_criteria":
                tool_response = await add_class_criteria()
                return tool_response
            elif name == "enable_filter_in_default":
                tool_response = await enable_filter_in_default()
                return tool_response
            elif name == "enable_filter_rule":
                tool_response = await enable_filter_rule()
                return tool_response
            elif name == "activate_rulset":
                tool_response = await activate_rulset(arguments["RuleName"])
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
