"""Shared fixtures. Env vars must be set before the package imports."""

import os

# Force-set (not setdefault): tests assert URLs built from these values, so a
# real token/org in the shell must not leak into the suite.
os.environ["DAGSTER_CLOUD_API_TOKEN"] = "test-token"
os.environ["DAGSTER_CLOUD_ORGANIZATION_ID"] = "test-org"
os.environ["DAGSTER_CLOUD_DEPLOYMENT"] = "test-deployment"

import pytest  # noqa: E402

from dagster_plus_mcp import tools  # noqa: E402


class GqlRecorder:
    """Fake gql() that records calls and returns queued responses."""

    def __init__(self):
        self.calls: list[dict] = []
        self.responses: list[dict] = []

    def queue(self, *responses: dict) -> None:
        self.responses.extend(responses)

    async def __call__(self, query, variables=None, deployment=None):
        self.calls.append(
            {"query": query, "variables": variables, "deployment": deployment}
        )
        if not self.responses:
            raise AssertionError("GqlRecorder: no queued response for call")
        return self.responses.pop(0)


@pytest.fixture
def gql_recorder(monkeypatch) -> GqlRecorder:
    recorder = GqlRecorder()
    monkeypatch.setattr(tools, "gql", recorder)
    return recorder
