# dagster-plus-mcp

MCP server for the [Dagster+](https://dagster.io/plus) GraphQL API. Exposes
operational tools (runs, assets, schedules, sensors, backfills, mutations) via
the [Model Context Protocol](https://modelcontextprotocol.io/).

## Requirements

- Python 3.13+
- A Dagster Cloud API token

## Installation

```bash
uv pip install git+https://github.com/TEAMSchools/dagster-plus-mcp.git
```

## Configuration

Set three required environment variables:

| Variable                        | Description                          |
| ------------------------------- | ------------------------------------ |
| `DAGSTER_CLOUD_API_TOKEN`       | Dagster Cloud user or agent token    |
| `DAGSTER_CLOUD_ORGANIZATION_ID` | Organization slug (e.g. `myorg`)     |
| `DAGSTER_CLOUD_DEPLOYMENT`      | Deployment name (e.g. `prod`)        |

## Usage

### Standalone

```bash
DAGSTER_CLOUD_API_TOKEN=... \
DAGSTER_CLOUD_ORGANIZATION_ID=myorg \
DAGSTER_CLOUD_DEPLOYMENT=prod \
  uv run python -m dagster_plus_mcp
```

### MCP client configuration

```json
{
  "mcpServers": {
    "dagster": {
      "command": "uv",
      "args": ["run", "--with", "dagster-plus-mcp", "python", "-m", "dagster_plus_mcp"],
      "env": {
        "DAGSTER_CLOUD_API_TOKEN": "your-token",
        "DAGSTER_CLOUD_ORGANIZATION_ID": "myorg",
        "DAGSTER_CLOUD_DEPLOYMENT": "prod"
      }
    }
  }
}
```

## Tools

| Tool | Description |
| --- | --- |
| `list_runs` | List recent runs with filtering by job, status, tags, time range |
| `get_run` | Full details for a single run |
| `get_run_logs` | Structured event log for a run |
| `get_run_compute_logs` | Raw stdout/stderr for a step |
| `get_captured_logs_metadata` | Download URLs for compute logs |
| `get_daemon_health` | Health status of all daemons |
| `get_cloud_agents` | Agent statuses, errors, code server states |
| `list_code_locations` | Workspace code locations and load status |
| `get_asset_health` | Health status for specific assets |
| `get_asset_staleness` | Staleness status and root causes |
| `search_assets` | Browse assets with pagination and prefix filtering |
| `get_asset_materializations` | Materialization history for an asset |
| `get_asset_partition_statuses` | Partition materialization counts |
| `get_asset_check_executions` | Asset check execution history |
| `get_asset_condition_evaluations` | Automation condition evaluation history |
| `get_tick_history` | Schedule/sensor tick history |
| `list_schedules` | List schedules in a code location |
| `list_sensors` | List sensors in a code location |
| `list_backfills` | List backfills with status filtering |
| `get_backfill` | Details for a single backfill |
| `get_run_group` | Full re-execution chain for a run |
| `get_location_load_history` | Deploy timeline for a code location |
| `launch_run` | Materialize selected assets |
| `launch_multiple_runs` | Batch-launch multiple materializations |
| `reexecute_run` | Re-execute a previous run |
