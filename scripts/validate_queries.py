# /// script
# requires-python = ">=3.12"
# dependencies = ["graphql-core>=3.2"]
# ///
"""Validate every GraphQL query in queries.py against schema.json.

Usage:
    uv run scripts/validate_queries.py

Exits non-zero if any query fails to parse or validate against the
introspection dump. Run after refresh_schema.py to find queries broken
by a schema change.
"""

import json
import sys
from pathlib import Path

from graphql import build_client_schema, parse, validate

REPO_ROOT = Path(__file__).parent.parent
SCHEMA_PATH = REPO_ROOT / "dagster_plus_mcp" / "schema.json"
QUERIES_PATH = REPO_ROOT / "dagster_plus_mcp" / "queries.py"


def load_queries() -> dict[str, str]:
    """Import queries.py without the package (avoids server env-var checks)."""
    namespace: dict[str, str] = {}
    exec(  # noqa: S102 — trusted first-party file
        compile(QUERIES_PATH.read_text(), str(QUERIES_PATH), "exec"), namespace
    )
    return {
        name: value
        for name, value in namespace.items()
        if name.isupper() and isinstance(value, str)
    }


def main() -> int:
    raw = json.loads(SCHEMA_PATH.read_text())
    schema = build_client_schema(raw.get("data", raw))
    queries = load_queries()

    failures = 0
    for name, query in sorted(queries.items()):
        try:
            document = parse(query)
        except Exception as e:
            print(f"FAIL {name}: parse error: {e}")
            failures += 1
            continue
        errors = validate(schema, document)
        if errors:
            failures += 1
            print(f"FAIL {name}:")
            for error in errors:
                print(f"  - {error.message}")
        else:
            print(f"ok   {name}")

    print(f"\n{len(queries)} queries, {failures} failures")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
