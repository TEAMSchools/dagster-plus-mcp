"""Unit tests for tool handlers — no API access; gql() is mocked."""

import json

import pytest
from pydantic import ValidationError

from dagster_plus_mcp import queries, tools
from dagster_plus_mcp.server import GraphQLError
from dagster_plus_mcp.tools import RunSpec, _build_execution_params, _handle_gql_errors


class TestHandleGqlErrors:
    async def test_graphql_error_returns_structured_json(self):
        @_handle_gql_errors
        async def boom():
            raise GraphQLError("query failed", details=[{"message": "bad field"}])

        result = json.loads(await boom())
        assert result == {
            "error": "query failed",
            "details": [{"message": "bad field"}],
        }

    async def test_graphql_error_without_details_omits_key(self):
        @_handle_gql_errors
        async def boom():
            raise GraphQLError("query failed")

        result = json.loads(await boom())
        assert result == {"error": "query failed"}

    async def test_unexpected_exception_returns_type_name(self):
        @_handle_gql_errors
        async def boom():
            raise ValueError("nope")

        result = json.loads(await boom())
        assert result == {"error": "ValueError", "details": "nope"}

    async def test_success_passes_through(self):
        @_handle_gql_errors
        async def ok():
            return "fine"

        assert await ok() == "fine"


class TestBuildExecutionParams:
    def test_asset_keys_split_on_slash(self):
        params = _build_execution_params(
            ["kipptaf/extracts/foo", "kippnewark/bar"],
            repository_location_name="kipptaf",
        )
        assert params["selector"]["assetSelection"] == [
            {"path": ["kipptaf", "extracts", "foo"]},
            {"path": ["kippnewark", "bar"]},
        ]
        assert params["selector"]["jobName"] == "__ASSET_JOB"
        assert params["selector"]["repositoryName"] == "__repository__"

    def test_tags_become_execution_metadata(self):
        params = _build_execution_params(
            ["a/b"],
            repository_location_name="kipptaf",
            tags={"dagster/partition": "05/11/2026"},
        )
        assert params["executionMetadata"]["tags"] == [
            {"key": "dagster/partition", "value": "05/11/2026"}
        ]

    def test_no_tags_or_config_omits_keys(self):
        params = _build_execution_params(
            ["a/b"], repository_location_name="kipptaf"
        )
        assert "executionMetadata" not in params
        assert "runConfigData" not in params

    def test_run_config_passed_through(self):
        params = _build_execution_params(
            ["a/b"],
            repository_location_name="kipptaf",
            run_config={"ops": {}},
        )
        assert params["runConfigData"] == {"ops": {}}


class TestRunSpec:
    def test_empty_asset_keys_rejected(self):
        with pytest.raises(ValidationError):
            RunSpec(asset_keys=[], repository_location_name="kipptaf")

    def test_defaults(self):
        spec = RunSpec(asset_keys=["a/b"], repository_location_name="kipptaf")
        assert spec.repository_name == "__repository__"
        assert spec.tags is None
        assert spec.run_config is None


class TestGetRunLogsFiltering:
    async def test_filter_types_client_side(self, gql_recorder):
        gql_recorder.queue(
            {
                "logsForRun": {
                    "events": [
                        {"__typename": "ExecutionStepStartEvent", "stepKey": "a"},
                        {"__typename": "ExecutionStepFailureEvent", "stepKey": "b"},
                        {"__typename": "MessageEvent", "message": "hi"},
                    ],
                    "cursor": "c1",
                    "hasMore": False,
                }
            }
        )
        result = json.loads(
            await tools.get_run_logs(
                run_id="r1", filter_types=["ExecutionStepFailureEvent"]
            )
        )
        assert [e["__typename"] for e in result["events"]] == [
            "ExecutionStepFailureEvent"
        ]
        assert result["cursor"] == "c1"

    async def test_no_filter_returns_all(self, gql_recorder):
        gql_recorder.queue(
            {
                "logsForRun": {
                    "events": [{"__typename": "MessageEvent", "message": "hi"}],
                    "cursor": None,
                    "hasMore": False,
                }
            }
        )
        result = json.loads(await tools.get_run_logs(run_id="r1"))
        assert len(result["events"]) == 1


class TestCloudAgentsFiltering:
    AGENTS = {
        "agents": [
            {
                "id": "agent-1",
                "status": "RUNNING",
                "lastHeartbeatTime": 100.0,
                "errors": [
                    {"timestamp": 50.0, "error": {"message": "old error"}},
                    {"timestamp": 150.0, "error": {"message": "new error"}},
                ],
                "codeServerStates": [],
                "runWorkerStates": [],
            },
            {
                "id": "agent-2",
                "status": "NOT_RUNNING",
                "lastHeartbeatTime": None,
                "errors": [],
                "codeServerStates": [],
                "runWorkerStates": [],
            },
        ]
    }

    async def test_status_filter(self, gql_recorder):
        gql_recorder.queue(json.loads(json.dumps(self.AGENTS)))
        result = json.loads(await tools.get_cloud_agents(status="RUNNING"))
        assert [a["id"] for a in result] == ["agent-1"]

    async def test_agent_id_substring_filter(self, gql_recorder):
        gql_recorder.queue(json.loads(json.dumps(self.AGENTS)))
        result = json.loads(await tools.get_cloud_agents(agent_id="ent-2"))
        assert [a["id"] for a in result] == ["agent-2"]

    async def test_errors_after_drops_old_errors(self, gql_recorder):
        gql_recorder.queue(json.loads(json.dumps(self.AGENTS)))
        result = json.loads(await tools.get_cloud_agents(errors_after=100.0))
        agent_1 = next(a for a in result if a["id"] == "agent-1")
        assert [e["message"] for e in agent_1["errors"]] == ["new error"]


class TestDeploymentOverride:
    async def test_default_deployment_is_none(self, gql_recorder):
        gql_recorder.queue({"runOrError": {"id": "r1"}})
        await tools.get_run(run_id="r1")
        assert gql_recorder.calls[0]["deployment"] is None

    async def test_deployment_threaded_to_gql(self, gql_recorder):
        gql_recorder.queue({"runOrError": {"id": "r1"}})
        await tools.get_run(run_id="r1", deployment="my-branch")
        assert gql_recorder.calls[0]["deployment"] == "my-branch"

    async def test_list_deployments_parses_result(self, gql_recorder):
        gql_recorder.queue(
            {"fullDeployments": [{"deploymentName": "prod"}]}
        )
        result = json.loads(await tools.list_deployments())
        assert result == [{"deploymentName": "prod"}]


class TestGqlUrlSelection:
    async def test_deployment_override_builds_absolute_url(self, monkeypatch):
        from dagster_plus_mcp import server

        posted = {}

        class FakeClient:
            async def post(self, url, json):
                posted["url"] = url
                posted["json"] = json

                class R:
                    is_success = True

                    @staticmethod
                    def json():
                        return {"data": {"ok": True}}

                return R()

        monkeypatch.setattr(server, "_client", FakeClient())
        data = await server.gql("query { ok }", deployment="my-branch")
        assert data == {"ok": True}
        assert posted["url"] == (
            "https://test-org.dagster.cloud/my-branch/graphql"
        )

    async def test_no_deployment_uses_base_url(self, monkeypatch):
        from dagster_plus_mcp import server

        posted = {}

        class FakeClient:
            async def post(self, url, json):
                posted["url"] = url

                class R:
                    is_success = True

                    @staticmethod
                    def json():
                        return {"data": {}}

                return R()

        monkeypatch.setattr(server, "_client", FakeClient())
        await server.gql("query { ok }")
        assert posted["url"] == ""

    async def test_invalid_deployment_name_rejected(self, monkeypatch):
        from dagster_plus_mcp import server

        class FakeClient:
            async def post(self, url, json):
                raise AssertionError("request must not be sent")

        monkeypatch.setattr(server, "_client", FakeClient())
        for bad in ["prod/graphql?x=", "../org-settings", "", "a b"]:
            with pytest.raises(ValueError):
                await server.gql("query { ok }", deployment=bad)

    async def test_http_error_raises_graphql_error(self, monkeypatch):
        from dagster_plus_mcp import server

        class FakeClient:
            async def post(self, url, json):
                class R:
                    is_success = False
                    status_code = 502
                    text = "bad gateway" * 100

                return R()

        monkeypatch.setattr(server, "_client", FakeClient())
        with pytest.raises(GraphQLError) as exc_info:
            await server.gql("query { ok }")
        assert "502" in exc_info.value.message
        assert len(exc_info.value.details) <= 500

    async def test_graphql_errors_payload_raises(self, monkeypatch):
        from dagster_plus_mcp import server

        class FakeClient:
            async def post(self, url, json):
                class R:
                    is_success = True

                    @staticmethod
                    def json():
                        return {"errors": [{"message": "bad field"}]}

                return R()

        monkeypatch.setattr(server, "_client", FakeClient())
        with pytest.raises(GraphQLError) as exc_info:
            await server.gql("query { ok }")
        assert exc_info.value.details == [{"message": "bad field"}]


class TestMutationConfirmPattern:
    async def test_terminate_runs_preview_fetches_current_status(self, gql_recorder):
        gql_recorder.queue({"runsOrError": {"results": [{"id": "r1"}]}})
        result = json.loads(await tools.terminate_runs(run_ids=["r1"]))
        assert result["mode"] == "preview"
        assert gql_recorder.calls[0]["query"] == queries.LIST_RUNS_QUERY
        assert gql_recorder.calls[0]["variables"]["filter"] == {"runIds": ["r1"]}

    async def test_terminate_runs_confirm_executes_mutation(self, gql_recorder):
        gql_recorder.queue(
            {"terminateRuns": {"terminateRunResults": []}}
        )
        result = json.loads(
            await tools.terminate_runs(
                run_ids=["r1", "r2"],
                terminate_policy="MARK_AS_CANCELED_IMMEDIATELY",
                confirm=True,
            )
        )
        call = gql_recorder.calls[0]
        assert call["query"] == queries.TERMINATE_RUNS_MUTATION
        assert call["variables"] == {
            "runIds": ["r1", "r2"],
            "terminatePolicy": "MARK_AS_CANCELED_IMMEDIATELY",
        }
        assert result == {"terminateRunResults": []}

    async def test_cancel_backfill_preview_then_confirm(self, gql_recorder):
        gql_recorder.queue({"partitionBackfillOrError": {"id": "b1"}})
        preview = json.loads(await tools.cancel_backfill(backfill_id="b1"))
        assert preview["mode"] == "preview"
        assert preview["backfill"] == {"id": "b1"}

        gql_recorder.queue(
            {"cancelPartitionBackfill": {"__typename": "CancelBackfillSuccess"}}
        )
        result = json.loads(
            await tools.cancel_backfill(backfill_id="b1", confirm=True)
        )
        assert result["__typename"] == "CancelBackfillSuccess"
        assert gql_recorder.calls[-1]["query"] == queries.CANCEL_BACKFILL_MUTATION

    async def test_resume_backfill_confirm(self, gql_recorder):
        gql_recorder.queue(
            {"resumePartitionBackfill": {"__typename": "ResumeBackfillSuccess"}}
        )
        result = json.loads(
            await tools.resume_backfill(backfill_id="b1", confirm=True)
        )
        assert result["__typename"] == "ResumeBackfillSuccess"

    WORKSPACE = {
        "workspaceOrError": {
            "locationEntries": [
                {"name": "kipptaf", "loadStatus": "LOADED"},
                {"name": "kippnewark", "loadStatus": "LOADED"},
            ]
        }
    }

    async def test_reload_code_location_preview_shows_current_state(
        self, gql_recorder
    ):
        gql_recorder.queue(json.loads(json.dumps(self.WORKSPACE)))
        result = json.loads(await tools.reload_code_location(location_name="kipptaf"))
        assert result["mode"] == "preview"
        assert result["location"] == {"name": "kipptaf", "loadStatus": "LOADED"}

    async def test_reload_code_location_preview_unknown_location_errors(
        self, gql_recorder
    ):
        gql_recorder.queue(json.loads(json.dumps(self.WORKSPACE)))
        result = json.loads(await tools.reload_code_location(location_name="typo"))
        assert "error" in result
        assert result["known_locations"] == ["kipptaf", "kippnewark"]

    async def test_reload_code_location_confirm_executes(self, gql_recorder):
        gql_recorder.queue(
            {"reloadRepositoryLocation": {"__typename": "WorkspaceLocationEntry"}}
        )
        result = json.loads(
            await tools.reload_code_location(location_name="kipptaf", confirm=True)
        )
        assert result["__typename"] == "WorkspaceLocationEntry"
        assert gql_recorder.calls[0]["query"] == (
            queries.RELOAD_REPOSITORY_LOCATION_MUTATION
        )

    async def test_free_concurrency_slots_preview_shows_run(self, gql_recorder):
        gql_recorder.queue({"runOrError": {"id": "r1", "status": "FAILURE"}})
        result = json.loads(await tools.free_concurrency_slots(run_id="r1"))
        assert result["mode"] == "preview"
        assert result["run"]["status"] == "FAILURE"

    async def test_free_concurrency_slots_confirm(self, gql_recorder):
        gql_recorder.queue({"freeConcurrencySlots": True})
        result = json.loads(
            await tools.free_concurrency_slots(run_id="r1", confirm=True)
        )
        assert result == {"freed": True}
        assert gql_recorder.calls[0]["variables"] == {
            "runId": "r1",
            "stepKey": None,
        }

    async def test_launch_run_empty_asset_keys_rejected(self, gql_recorder):
        result = json.loads(
            await tools.launch_run(asset_keys=[], repository_location_name="kipptaf")
        )
        assert "error" in result
        assert gql_recorder.calls == []

    async def test_launch_run_preview_makes_no_call(self, gql_recorder):
        result = json.loads(
            await tools.launch_run(
                asset_keys=["a/b"], repository_location_name="kipptaf"
            )
        )
        assert result["mode"] == "preview"
        assert gql_recorder.calls == []


class TestScheduleSensorControls:
    SCHEDULE = {
        "scheduleOrError": {
            "__typename": "Schedule",
            "id": "sched-1",
            "name": "my_schedule",
            "scheduleState": {"id": "state-1", "status": "RUNNING"},
        }
    }
    SENSOR = {
        "sensorOrError": {
            "__typename": "Sensor",
            "id": "sensor-1",
            "name": "my_sensor",
            "sensorState": {"id": "state-2", "status": "RUNNING"},
        }
    }

    async def test_stop_schedule_resolves_state_id(self, gql_recorder):
        gql_recorder.queue(
            json.loads(json.dumps(self.SCHEDULE)),
            {"stopRunningSchedule": {"__typename": "ScheduleStateResult"}},
        )
        result = json.loads(
            await tools.stop_schedule(
                schedule_name="my_schedule",
                repository_location_name="kipptaf",
                confirm=True,
            )
        )
        assert result["__typename"] == "ScheduleStateResult"
        stop_call = gql_recorder.calls[1]
        assert stop_call["query"] == queries.STOP_SCHEDULE_MUTATION
        assert stop_call["variables"] == {"id": "state-1"}

    async def test_stop_schedule_not_found_short_circuits(self, gql_recorder):
        gql_recorder.queue(
            {
                "scheduleOrError": {
                    "__typename": "ScheduleNotFoundError",
                    "message": "not found",
                }
            }
        )
        result = json.loads(
            await tools.stop_schedule(
                schedule_name="missing",
                repository_location_name="kipptaf",
                confirm=True,
            )
        )
        assert result["__typename"] == "ScheduleNotFoundError"
        assert len(gql_recorder.calls) == 1

    async def test_start_schedule_confirm_uses_selector(self, gql_recorder):
        gql_recorder.queue(
            {"startSchedule": {"__typename": "ScheduleStateResult"}}
        )
        await tools.start_schedule(
            schedule_name="my_schedule",
            repository_location_name="kipptaf",
            confirm=True,
        )
        call = gql_recorder.calls[0]
        assert call["query"] == queries.START_SCHEDULE_MUTATION
        assert call["variables"]["scheduleSelector"] == {
            "scheduleName": "my_schedule",
            "repositoryLocationName": "kipptaf",
            "repositoryName": "__repository__",
        }

    async def test_stop_sensor_resolves_state_id(self, gql_recorder):
        gql_recorder.queue(
            json.loads(json.dumps(self.SENSOR)),
            {"stopSensor": {"__typename": "StopSensorMutationResult"}},
        )
        result = json.loads(
            await tools.stop_sensor(
                sensor_name="my_sensor",
                repository_location_name="kipptaf",
                confirm=True,
            )
        )
        assert result["__typename"] == "StopSensorMutationResult"
        assert gql_recorder.calls[1]["variables"] == {"id": "state-2"}

    async def test_set_sensor_cursor_confirm(self, gql_recorder):
        gql_recorder.queue({"setSensorCursor": {"__typename": "Sensor"}})
        await tools.set_sensor_cursor(
            sensor_name="my_sensor",
            repository_location_name="kipptaf",
            cursor="12345",
            confirm=True,
        )
        call = gql_recorder.calls[0]
        assert call["query"] == queries.SET_SENSOR_CURSOR_MUTATION
        assert call["variables"]["cursor"] == "12345"

    async def test_set_sensor_cursor_requires_cursor_or_reset(self, gql_recorder):
        result = json.loads(
            await tools.set_sensor_cursor(
                sensor_name="my_sensor",
                repository_location_name="kipptaf",
                confirm=True,
            )
        )
        assert "error" in result
        assert gql_recorder.calls == []

    async def test_set_sensor_cursor_rejects_cursor_and_reset(self, gql_recorder):
        result = json.loads(
            await tools.set_sensor_cursor(
                sensor_name="my_sensor",
                repository_location_name="kipptaf",
                cursor="12345",
                reset=True,
                confirm=True,
            )
        )
        assert "error" in result
        assert gql_recorder.calls == []

    async def test_set_sensor_cursor_reset_sends_null(self, gql_recorder):
        gql_recorder.queue({"setSensorCursor": {"__typename": "Sensor"}})
        await tools.set_sensor_cursor(
            sensor_name="my_sensor",
            repository_location_name="kipptaf",
            reset=True,
            confirm=True,
        )
        assert gql_recorder.calls[0]["variables"]["cursor"] is None

    async def test_stop_schedule_preview_shows_state(self, gql_recorder):
        gql_recorder.queue(json.loads(json.dumps(self.SCHEDULE)))
        result = json.loads(
            await tools.stop_schedule(
                schedule_name="my_schedule",
                repository_location_name="kipptaf",
            )
        )
        assert result["mode"] == "preview"
        assert result["schedule"]["scheduleState"]["id"] == "state-1"
        assert len(gql_recorder.calls) == 1

    async def test_start_sensor_preview_shows_state(self, gql_recorder):
        gql_recorder.queue(json.loads(json.dumps(self.SENSOR)))
        result = json.loads(
            await tools.start_sensor(
                sensor_name="my_sensor",
                repository_location_name="kipptaf",
            )
        )
        assert result["mode"] == "preview"
        assert gql_recorder.calls[0]["query"] == queries.SENSOR_STATE_QUERY

    async def test_mutation_threads_deployment(self, gql_recorder):
        gql_recorder.queue(
            {"startSensor": {"__typename": "Sensor"}}
        )
        await tools.start_sensor(
            sensor_name="my_sensor",
            repository_location_name="kipptaf",
            confirm=True,
            deployment="my-branch",
        )
        assert gql_recorder.calls[0]["deployment"] == "my-branch"
