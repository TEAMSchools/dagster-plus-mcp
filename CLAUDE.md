# CLAUDE.md — dagster-plus-mcp

FastMCP server exposing Dagster+ operational data via GraphQL.

## Package Structure

- `server.py` — `FastMCP` instance (with `instructions`), env vars, persistent
  `httpx.AsyncClient`, async `gql()` client, `GraphQLError` exception
- `queries.py` — GraphQL query strings (see GraphQL section below before
  modifying)
- `tools.py` — async `@server.tool()` handlers with
  `Annotated[type, Field(description=...)]`, wrapped with `@_handle_gql_errors`
  for structured error returns
- `__main__.py` — entry point (imports `tools` to trigger registration)
- `scripts/` — PEP 723 standalone scripts: `refresh_schema.py` (re-introspect
  the live API into `schema.json`), `validate_queries.py` (validate every
  query in `queries.py` against `schema.json`)
- `tests/` — pytest unit tests; `gql()` is mocked (see `GqlRecorder` in
  `conftest.py`), no API access needed. Run with `uv run --group dev pytest`.

**Adding a tool:** Add query to `queries.py`, add `@server.tool()` function to
`tools.py` with the `@_handle_gql_errors` decorator. FastMCP auto-generates JSON
schema from type hints. Use `BaseModel` subclasses for complex input types (see
`RunSpec`). Give the tool a trailing `deployment: Deployment = None` parameter
and pass `deployment=deployment` to every `gql()` call so it supports
branch-deployment targeting. Run `uv run scripts/validate_queries.py` before
committing query changes.

**Testing imports:** All three env vars (`DAGSTER_CLOUD_API_TOKEN`,
`DAGSTER_CLOUD_ORGANIZATION_ID`, `DAGSTER_CLOUD_DEPLOYMENT`) are required at
import time — use
`DAGSTER_CLOUD_API_TOKEN=test DAGSTER_CLOUD_ORGANIZATION_ID=test DAGSTER_CLOUD_DEPLOYMENT=test`
prefix when verifying outside the MCP runtime.

## Running

```bash
uv run python -m dagster_plus_mcp
```

## Environment Variables

| Variable                        | Required | Description              |
| ------------------------------- | -------- | ------------------------ |
| `DAGSTER_CLOUD_API_TOKEN`       | Yes      | User/agent token         |
| `DAGSTER_CLOUD_ORGANIZATION_ID` | Yes      | Org slug                 |
| `DAGSTER_CLOUD_DEPLOYMENT`      | Yes      | Default deployment name  |

`DAGSTER_CLOUD_DEPLOYMENT` is only the default: every tool accepts an optional
`deployment` argument that reroutes that call to another deployment's GraphQL
endpoint (e.g. a branch deployment). `list_deployments` discovers deployment
names, including active branch deployments with branch/PR metadata.

## Refreshing schema.json

```bash
DAGSTER_CLOUD_API_TOKEN=... DAGSTER_CLOUD_ORGANIZATION_ID=... \
DAGSTER_CLOUD_DEPLOYMENT=prod uv run scripts/refresh_schema.py
uv run scripts/validate_queries.py
```

The refresh overwrites the introspection dump; the validator then reports any
query in `queries.py` broken by schema drift. Reconcile the "Schema gotchas"
list below after a refresh.

## GraphQL Schema Reference

New queries are sourced from the Dagster UI TypeScript at
[`js_modules/ui-core/src`](https://github.com/dagster-io/dagster/tree/master/js_modules/ui-core/src)
— no Python package exports client-side queries. Verify field names/types
against the Python schema in
[`dagster-graphql/dagster_graphql/schema`](https://github.com/dagster-io/dagster/tree/master/python_modules/dagster-graphql/dagster_graphql/schema).
**Do not write or modify queries from memory.** Cloud-only queries (e.g.,
`agents` root field) are not in the OSS repo — capture from browser Network tab
or check `dagster_plus_mcp/schema.json` (full introspection dump of the Cloud
API).

### Known API limitations

- `agents` query: zero args, no server-side filtering on `errors`,
  `runWorkerStates`, or `codeServerStates`. Response is 200KB+ — must filter
  client-side.

### Schema gotchas

- `AssetConditionEvaluationRecordsOrError` union does not include `PythonError`
  — never add `... on PythonError` to that query
- `AssetNode.assetMaterializations` does not accept `afterTimestampMillis` —
  only the top-level `assetMaterializations` query field does
- `RunsFilter` uses `pipelineName`, not `jobName` (legacy naming)
- `Run` has `hasTerminatePermission`, not `hasCancelPermission`
  (`PartitionBackfill` does have `hasCancelPermission`)
- `MessageEvent` is an interface — use inline fragments for `DagsterRunEvent`
- `capturedLogs` returns both stdout/stderr — no `ioType` selector
- `AssetConditionEvaluationRecord` has `numRequested` only — no
  `numSkipped`/`numDiscarded`
- `evaluationNodes` is top-level, not nested under `evaluation`
- `AssetGroupSelector` requires all three fields (`groupName`,
  `repositoryLocationName`, `repositoryName`)
- Timestamps (`createdAfter`, etc.) are Unix floats, not ISO strings

## Pagination

`list_runs`, `get_run_logs`, `get_asset_condition_evaluations`,
`get_asset_check_executions`, `list_backfills`, `search_assets`, and
`get_location_load_history` return a `cursor`. Pass it back to page forward.

Server-side timestamp filtering is available on:

- `get_tick_history` — `after_timestamp`/`before_timestamp` (Unix epoch floats)
- `get_asset_materializations` —
  `before_timestamp_millis`/`after_timestamp_millis` (millisecond epoch as
  **string**, not float)
- `list_backfills` — `created_after`/`created_before` (Unix epoch floats)

Other tools require client-side filtering after pagination.

## Discovery tools

- `list_schedules` / `list_sensors` — require `repository_location_name`; use
  `list_code_locations` first to get location names. Both accept an optional
  status filter (`RUNNING` or `STOPPED`).
- `list_sensors` returns `nextTick.timestamp` (predicted next evaluation) and
  `sensorType` (e.g. `AUTOMATION` for automation condition sensors).
- `get_tick_history` also returns `nextTick.timestamp` on the instigation state.
- `get_run_group` — pass any run ID to get the full re-execution chain (all runs
  sharing the same root). More efficient than traversing
  `parentRunId`/`rootRunId` manually.
- `get_location_load_history` — shows deploy timeline per code location with
  load status (`LOADED`/`ERROR`), timestamps, and error details. Use for
  diagnosing failed deploys.
- `get_run` now includes `stepStats` — per-step timing and status without
  fetching full logs via `get_run_logs`.

## Diagnosing assets (cross-tool)

Tool selection and diagnostic workflows are in the server `instructions` (see
`server.py`).

## API quirks

- `get_cloud_agents` supports server-side filtering via optional `agent_id`
  (substring match), `status` (`RUNNING`/`NOT_RUNNING`), and `errors_after`
  (Unix epoch). Always returns compact JSON with truncated error messages (300
  chars). Use filters to avoid 200KB+ unfiltered responses.
- `get_daemon_health` returns `lastHeartbeatTime: null` for all daemons on
  Dagster Cloud — only useful as a binary healthy/unhealthy check
- `get_run_compute_logs` returns null for GKE runs (ephemeral pods)
- `get_run_logs` returns `timestamp: null` for non-`MessageEvent` types

## Mutation tools

All mutation tools use a **confirm flag pattern**: `confirm=False` (default)
returns a preview of what would be sent (usually the target's current state);
`confirm=True` executes the mutation. No server-side state — preview and execute
are independent calls.

| Tool                              | Description                                                                   |
| --------------------------------- | ----------------------------------------------------------------------------- |
| `launch_run`                      | Materialize selected assets in a code location                                |
| `launch_multiple_runs`            | Batch-launch multiple asset materializations                                  |
| `reexecute_run`                   | Re-execute a previous run (`FROM_FAILURE`, `FROM_ASSET_FAILURE`, `ALL_STEPS`) |
| `terminate_runs`                  | Cancel runs (`SAFE_TERMINATE` or `MARK_AS_CANCELED_IMMEDIATELY`)              |
| `cancel_backfill`                 | Cancel an in-progress backfill                                                |
| `resume_backfill`                 | Resume a failed/canceled backfill                                             |
| `start_schedule` / `stop_schedule`| Turn a schedule on/off by name + location                                     |
| `start_sensor` / `stop_sensor`    | Turn a sensor on/off by name + location                                       |
| `set_sensor_cursor`               | Set or reset a sensor's cursor                                                |
| `reload_code_location`            | Re-import a code location's definitions                                       |
| `free_concurrency_slots`          | Free slots held by a dead run                                                 |

### Usage pattern

1. Call with `confirm=False` (or omit) to preview the execution params
2. Review the preview JSON
3. Call again with `confirm=True` to execute

### Schema gotchas (mutations)

- `ExecutionParams.selector` uses `assetSelection` (list of `AssetKeyInput`),
  not `assetKeys` — tools handle this conversion from slash-separated strings
- `ReexecutionParams.extraTags` uses `[ExecutionTag!]` format (`key`/`value`
  objects), not a flat dict — tools handle this conversion
- `launchMultipleRuns` returns a nested result: the outer union has
  `LaunchMultipleRunsResult`, whose `launchMultipleRunsResult` field is a list
  of per-run `LaunchRunResult` unions
- `startSchedule`/`startSensor` take a selector (name + location + repo), but
  `stopRunningSchedule`/`stopSensor` take the **InstigationState id** — the
  stop tools resolve it first via `scheduleOrError`/`sensorOrError`, so a stop
  is two GraphQL calls
- `freeConcurrencySlots` returns a bare `Boolean!`, not a result union
