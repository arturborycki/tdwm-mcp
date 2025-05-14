# Teradata Workload Management MCP Server

## Overview
A Model Context Protocol (MCP) server implementation that provides workload management (TDWM) interaction and orchestration.

## Components

### Tools
The server offers six core tools:

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