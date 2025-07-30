# Teradata Data Warehouse Management (TDWM) MCP Server

A Model Control Protocol (MCP) server for Teradata Data Warehouse Management (TDWM) that provides comprehensive monitoring and management capabilities for Teradata systems.

## Features

This MCP server exposes tools for monitoring and managing Teradata workloads, sessions, and system resources through the TDWM (Teradata Data Warehouse Management) framework.

## Installation

```bash
pip install tdwm-mcp
```

## Configuration

Set your database connection URL either as an environment variable or command-line argument:

```bash
export DATABASE_URI="teradata://username:password@hostname"
# or
python -m tdwm_mcp.server "teradata://username:password@hostname"
```

## Available Tools

### Session Management
- **show_sessions** - Show my active sessions
- **show_sql_steps_for_session** - Show SQL execution steps for a specific session
- **show_sql_text_for_session** - Show SQL text for a specific session
- **abort_sessions_user** - Abort all sessions for a specific user
- **identify_blocking** - Identify users causing blocking situations

### Query Band and Monitoring
- **monitor_session_query_band** - Monitor query band for a specific session
- **list_query_band** - List query bands by type (TRANSACTION, PROFILE, SESSION, or ALL)
- **show_query_log** - Show query log for a specific user

### System Resource Monitoring
- **show_physical_resources** - Monitor system physical resources
- **monitor_amp_load** - Monitor AMP (Access Module Processor) load
- **monitor_awt** - Monitor AWT (AMP Worker Task) resources
- **monitor_config** - Monitor virtual configuration

### Workload Management
- **list_active_WD** - List active workloads (WD)
- **list_WD** - List all workloads (WD)
- **show_tdwm_summary** - Show workloads summary information

### Delay Queue Management
- **list_delayed_request** - List all delayed queries
- **abort_delayed_request** - Abort delayed requests for a specific session
- **display_delay_queue** - Display delay queue details by type (WORKLOAD, SYSTEM, UTILITY, or ALL)
- **release_delay_queue** - Release delayed requests for a session or user

### Throttle and Performance
- **show_trottle_statistics** - Show throttle statistics (ALL, QUERY, SESSION, WORKLOAD)
- **list_utility_stats** - List statistics for utility usage on the system

### System Information
- **show_cod_limits** - Show COD (Capacity On Demand) limits
- **show_top_users** - Show users consuming the most resources
- **show_sw_event_log** - Show system software event logs (OPERATIONAL or ALL)

### TASM (Teradata Active System Management)
- **tdwm_list_clasification** - List classification types for workload (TASM) rules
- **show_tasm_statistics** - Show TASM performance statistics
- **show_tasm_even_history** - Show TASM event history
- **show_tasm_rule_history_red** - Show what caused the system to enter RED state

### Rule Management (WIP)
- **create_filter_rule** - Create filter rule
- **add_class_criteria** - Add classification criteria
- **enable_filter_in_default** - Enable filter in default state
- **enable_filter_rule** - Enable filter rule
- **activate_rulset** - Activate ruleset with new filter rule

## Usage Examples

### Basic Session Monitoring
```python
# Show all my sessions
await call_tool("show_sessions")

# Show SQL text for session 1234
await call_tool("show_sql_text_for_session", {"sessionNo": 1234})

# Show SQL execution steps for session 1234
await call_tool("show_sql_steps_for_session", {"sessionNo": 1234})
```

### System Resource Monitoring
```python
# Monitor AMP load
await call_tool("monitor_amp_load")

# Monitor physical resources
await call_tool("show_physical_resources")

# Monitor AWT resources
await call_tool("monitor_awt")
```

### Workload Management
```python
# List active workloads
await call_tool("list_active_WD")

# Show workload summary
await call_tool("show_tdwm_summary")

# Show throttle statistics for all types
await call_tool("show_trottle_statistics", {"type": "ALL"})
```

### Query Analysis
```python
# Show query log for user 'john_doe'
await call_tool("show_query_log", {"user": "john_doe"})

# Show top resource-consuming users
await call_tool("show_top_users", {"type": "TOP"})
```

### Delay Queue Management
```python
# List all delayed requests
await call_tool("list_delayed_request")

# Display system delay queue
await call_tool("display_delay_queue", {"type": "SYSTEM"})

# Release delayed requests for session 1234
await call_tool("release_delay_queue", {"sessionNo": 1234})
```

### TASM Monitoring
```python
# Show TASM statistics
await call_tool("show_tasm_statistics")

# Show what caused RED state
await call_tool("show_tasm_rule_history_red")

# List classification types for rules
await call_tool("tdwm_list_clasification")
```

## Tool Parameters

### Required Parameters
- **sessionNo** (integer) - Session number for session-specific operations
- **user** (string) - Username for user-specific operations
- **RuleName** (string) - Rule name for rule activation

### Optional Parameters
- **type** (string) - Type specification for various tools (e.g., "ALL", "TOP", "SYSTEM", "WORKLOAD")
- **userName** (string) - Alternative username parameter for some operations

## Static Reference Data

The server includes static reference tables for TDWM classification types:

- **TDWM_CLASIFICATION_TYPE** - Classification types with their categories and expected values
- **TDWM_CLASSIFICATION_VALUE** - Classification values and their descriptions

## Error Handling

All tools include comprehensive error handling and will return descriptive error messages if operations fail.

## Logging

The server uses Python's logging module with logger name "teradata_mcp" for debugging and monitoring.

## Dependencies

- `teradatasql` - Teradata SQL driver
- `mcp` - Model Control Protocol framework
- `pydantic` - Data validation
- `PyYAML` - YAML configuration support

## License

This MCP server is licensed under the MIT License. This means you are free to use, modify, and distribute the software, subject to the terms and conditions of the MIT License. For more details, please see the LICENSE file in the project repository.