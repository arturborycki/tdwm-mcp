# Teradata Workload Management MCP Server

## Overview
A Model Context Protocol (MCP) server implementation that provides workload management (TDWM) interaction and orchestration.

## Components

### Tools
The server offers following tools:

#### Workload Management Tools:
  show_tdwm_summary - Shows workloads summary information
  list_WD - Lists workloads (WD)
  list_active_WD - Lists active workloads (WD)
  show_trottle_statistics - Shows throttle statistics for different components

#### Queue Management Tools:
  list_delayed_request - Lists all of the delayed queries
  display_delay_queue - Displays delay queue by type
  release_delay_queue - Releases a request or utility session in the queue
  abort_delayed_request - Aborts a delayed request on a specific session

#### Session Management Tools:
  show_sessions - Shows current sessions
  show_sql_steps_for_session - Shows SQL steps for a session
  show_sql_text_for_session - Shows SQL text for a session
  abort_sessions_user - Aborts sessions for a user
  identify_blocking - Identifies blocking users

#### Resource Monitoring Tools:
  show_physical_resources - Monitors system resources
  monitor_amp_load - Monitors AMP load
  monitor_awt - Monitors AWT (Amp Worker Task) resources
  monitor_config - Monitors virtual config
  list_utility_stats - Lists statistics for utility use on the system

## Usage with Claude Desktop

### uv

```bash
# Add the server to your claude_desktop_config.json
{
  "mcpServers": {
    "teradata": {
      "command": "uv",
      "args": [
        "--directory",
        "/Users/MCP/tdwm-mcp",
        "run",
        "tdwm-mcp"
      ],
      "env": {
        "DATABASE_URI": "teradata://user:passwd@host"
      }
    }
  }
}
```

## Building

UV:

```bash
uv build
```

## License

This MCP server is licensed under the MIT License. This means you are free to use, modify, and distribute the software, subject to the terms and conditions of the MIT License. For more details, please see the LICENSE file in the project repository.