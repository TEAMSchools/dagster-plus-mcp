# /// script
# requires-python = ">=3.12"
# dependencies = ["graphql-core>=3.2", "httpx"]
# ///
"""Refresh schema.json from a live Dagster Cloud GraphQL introspection.

Usage (same env vars as the MCP server):
    DAGSTER_CLOUD_API_TOKEN=... \
    DAGSTER_CLOUD_ORGANIZATION_ID=kipptaf \
    DAGSTER_CLOUD_DEPLOYMENT=prod \
    uv run scripts/refresh_schema.py

Overwrites dagster_plus_mcp/schema.json with the current introspection
dump, then run scripts/validate_queries.py to find queries broken by
the schema change.
"""

import json
import os
import sys
from pathlib import Path

import httpx

from graphql import get_introspection_query

SCHEMA_PATH = Path(__file__).parent.parent / "dagster_plus_mcp" / "schema.json"


def main() -> int:
    token = os.environ["DAGSTER_CLOUD_API_TOKEN"]
    organization = os.environ["DAGSTER_CLOUD_ORGANIZATION_ID"]
    deployment = os.environ["DAGSTER_CLOUD_DEPLOYMENT"]
    url = f"https://{organization}.dagster.cloud/{deployment}/graphql"

    response = httpx.post(
        url,
        json={"query": get_introspection_query(descriptions=True)},
        headers={
            "Dagster-Cloud-Api-Token": token,
            "Content-Type": "application/json",
        },
        timeout=120,
    )
    response.raise_for_status()
    payload = response.json()
    if "errors" in payload:
        print(f"Introspection failed: {payload['errors']}", file=sys.stderr)
        return 1

    # The committed dump is the bare introspection result (no {"data": ...}
    # wrapper); validate_queries.py accepts either shape for compatibility.
    SCHEMA_PATH.write_text(json.dumps(payload["data"], indent=1, sort_keys=True))
    type_count = len(payload["data"]["__schema"]["types"])
    print(f"Wrote {SCHEMA_PATH} ({type_count} types)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
