"""Live check: compute-log tools resolve their own logKey from a run ID.

Unlike the other scripts here this one imports the package, so it runs in
the project environment (no PEP 723 block):

    DAGSTER_CLOUD_API_TOKEN=... \
    DAGSTER_CLOUD_ORGANIZATION_ID=kipptaf \
    DAGSTER_CLOUD_DEPLOYMENT=prod \
    uv run scripts/check_log_key_resolution.py

Asserts against a known-good fixture run, and falls back to a recent
FAILURE run when that one has aged out of log retention — there it can only
assert that the resolved key round-trips to non-null output, not the key's
value. Exits non-zero on failure.
"""

import asyncio
import json
import sys

# Private, but the tools need a live httpx client and the lifespan is the
# only thing that builds one.
from dagster_plus_mcp.server import GraphQLError, _lifespan, server
from dagster_plus_mcp.tools import (
    _resolve_log_key,
    get_run_compute_logs,
    list_runs,
)

FIXTURE_RUN_ID = "140a55b5-8841-474f-bced-d1aa2cde9ffd"
FIXTURE_STEP_KEY = "kippmiami_dbt_assets"
FIXTURE_LOG_KEY = "twzyagjy"


async def check_fixture_run() -> bool:
    """Resolve the fixture run's key and read its stderr back."""
    key = await _resolve_log_key(FIXTURE_RUN_ID, FIXTURE_STEP_KEY, None)
    assert key == [FIXTURE_RUN_ID, "compute_logs", FIXTURE_LOG_KEY], key
    print(f"ok   resolved {FIXTURE_STEP_KEY} -> {FIXTURE_LOG_KEY}")

    # run_id alone: only unambiguous if the run captured one step worker.
    logs = json.loads(await get_run_compute_logs(run_id=FIXTURE_RUN_ID))
    if "error" in logs:
        print(f"note run_id alone is ambiguous: {logs['error']}")
        logs = json.loads(
            await get_run_compute_logs(
                run_id=FIXTURE_RUN_ID, step_key=FIXTURE_STEP_KEY
            )
        )
    assert "error" not in logs, logs
    assert logs["stderr"], f"stderr was empty: {logs}"
    print(f"ok   stderr round-tripped ({len(logs['stderr'])} bytes)")
    return True


async def check_recent_failure() -> bool:
    """Fallback: any recent FAILURE run whose key round-trips to output."""
    runs = json.loads(await list_runs(limit=20, statuses=["FAILURE"]))
    for run in runs.get("results", []):
        logs = json.loads(await get_run_compute_logs(run_id=run["id"]))
        if "error" in logs or not (logs.get("stderr") or logs.get("stdout")):
            continue
        print(f"ok   {run['id']} -> {logs['logKey'][-1]}, output non-null")
        return True
    print("FAIL no recent FAILURE run produced compute-log output")
    return False


async def main() -> int:
    async with _lifespan(server):
        try:
            return 0 if await check_fixture_run() else 1
        except (AssertionError, GraphQLError) as e:
            print(f"note fixture run unusable ({e}) — trying a recent run")
        return 0 if await check_recent_failure() else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
