"""Acceptance tests for finite OpenLineage events from ``streamt test``."""

from __future__ import annotations

import importlib
import json
from pathlib import Path
from typing import ClassVar
from uuid import UUID

import pytest
import yaml
from click.testing import CliRunner, Result

from streamt.cli import main
from streamt.core.models import DataTest, StreamtProject
from streamt.testing.runner import TestRunner as StreamtTestRunner
from streamt.testing.runner import resolve_sample_test_topic

test_command = importlib.import_module("streamt.cli.commands.test")

_NAMESPACE_ENVIRONMENT = (
    "OPENLINEAGE_NAMESPACE",
    "STREAMT_OPENLINEAGE_KAFKA_NAMESPACE",
    "STREAMT_OPENLINEAGE_GATEWAY_NAMESPACE",
)
_TRANSPORT_ENVIRONMENT = (
    "OPENLINEAGE_CONFIG",
    "OPENLINEAGE_DISABLED",
    "OPENLINEAGE_URL",
    "OPENLINEAGE_API_KEY",
    "OPENLINEAGE__TRANSPORT__TYPE",
    "OPENLINEAGE__TRANSPORT__LOG_FILE_PATH",
    "OPENLINEAGE__TRANSPORT__URL",
    "OPENLINEAGE__TRANSPORT__ENDPOINT",
)


class _FakeTransport:
    """Record every delivery attempt and optionally fail at safe test boundaries."""

    def __init__(self, *, fail_attempts: set[int] | None = None, fail_close: bool = False):
        self.attempts: list[dict[str, object]] = []
        self.fail_attempts = fail_attempts or set()
        self.fail_close = fail_close
        self.close_calls = 0

    def emit(self, event: dict[str, object]) -> None:
        self.attempts.append(event)
        if len(self.attempts) in self.fail_attempts:
            raise RuntimeError("transport secret=https://user:key@example.invalid/private")

    def close(self) -> None:
        self.close_calls += 1
        if self.fail_close:
            raise RuntimeError("close secret=/private/openlineage.jsonl")


@pytest.fixture(autouse=True)
def _clear_openlineage_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (*_NAMESPACE_ENVIRONMENT, *_TRANSPORT_ENVIRONMENT):
        # Record absent variables in MonkeyPatch's undo stack too. Project
        # dotenv loading writes directly to os.environ, so a bare delenv() on
        # an already absent key would not clean up the value after the test.
        monkeypatch.setenv(name, "streamt-test-unset-sentinel")
        monkeypatch.delenv(name, raising=False)


def _project_config(*, tests: list[dict[str, object]] | None = None) -> dict[str, object]:
    return {
        "project": {"name": "telemetry project"},
        "runtime": {
            "kafka": {"bootstrap_servers": "broker.example:9092"},
            "flink": {
                "default": "local",
                "clusters": {"local": {"rest_url": "http://localhost:8081"}},
            },
            "conduktor": {"gateway": {"proxy_bootstrap": "gateway.example:6969"}},
        },
        "sources": [{"name": "raw", "topic": "raw.events.v1"}],
        "models": [
            {
                "name": "clean",
                "sql": 'SELECT * FROM {{ source("raw") }}',
                "topic": {"name": "clean.physical.v2"},
            }
        ],
        "tests": tests
        if tests is not None
        else [
            {"name": "clean_schema", "model": "clean", "type": "schema"},
            {"name": "clean_sample", "model": "clean", "type": "sample"},
            {"name": "raw_sample", "model": "raw", "type": "sample"},
            {"name": "clean_continuous", "model": "clean", "type": "continuous"},
        ],
    }


def _write_project(
    path: Path,
    *,
    tests: list[dict[str, object]] | None = None,
) -> Path:
    path.joinpath("stream_project.yml").write_text(
        yaml.safe_dump(_project_config(tests=tests), sort_keys=False),
        encoding="utf-8",
    )
    return path


def _passing_results(_self: StreamtTestRunner, tests: list[DataTest]) -> list[dict[str, object]]:
    return [{"name": selected.name, "status": "passed"} for selected in tests]


def _install_transport(
    monkeypatch: pytest.MonkeyPatch,
    *transports: _FakeTransport,
) -> None:
    queue = list(transports)
    sentinel_config = object()

    def load_config(_environment: object, *, emission_requested: bool) -> object:
        assert emission_requested is True
        return sentinel_config

    def create_transport(config: object) -> _FakeTransport:
        assert config is sentinel_config
        return queue.pop(0)

    monkeypatch.setattr(test_command, "load_openlineage_transport_config", load_config)
    monkeypatch.setattr(test_command, "create_openlineage_transport", create_transport)


def _invoke(
    project: Path,
    *arguments: str,
    output: str = "text",
) -> Result:
    prefix = ["-o", output] if output != "text" else []
    return CliRunner().invoke(
        main,
        [
            *prefix,
            "test",
            "-p",
            str(project),
            *arguments,
        ],
    )


def _emission_arguments(*extra: str) -> tuple[str, ...]:
    return (
        "--emit-openlineage",
        "--openlineage-job-namespace",
        "test-jobs",
        *extra,
    )


def _event_types(transport: _FakeTransport) -> list[object]:
    return [event["eventType"] for event in transport.attempts]


def _json_envelope(result: Result) -> dict[str, object]:
    return json.loads(result.stdout)


def test_help_exposes_only_the_explicit_runtime_options() -> None:
    result = CliRunner().invoke(main, ["test", "--help"])

    assert result.exit_code == 0
    for option in (
        "--emit-openlineage",
        "--openlineage-job-namespace",
        "--openlineage-kafka-namespace",
        "--openlineage-gateway-namespace",
    ):
        assert option in result.stdout


def test_transport_environment_alone_does_not_enable_emission(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project = _write_project(
        tmp_path, tests=[{"name": "schema", "model": "clean", "type": "schema"}]
    )
    monkeypatch.setenv("OPENLINEAGE_CONFIG", "/private/missing/config.yml")
    monkeypatch.setenv("OPENLINEAGE_DISABLED", "not-a-boolean")
    monkeypatch.setattr(
        test_command,
        "load_openlineage_transport_config",
        lambda *_args, **_kwargs: pytest.fail("transport config was loaded without the flag"),
    )

    result = _invoke(project)

    assert result.exit_code == 0
    assert "PASS" in result.stdout


@pytest.mark.parametrize(
    ("arguments", "tests"),
    [
        (("--coverage",), None),
        (("--model", "missing"), None),
        (("--deploy",), [{"name": "schema", "model": "clean", "type": "schema"}]),
    ],
)
def test_non_run_paths_never_load_or_open_a_transport(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    arguments: tuple[str, ...],
    tests: list[dict[str, object]] | None,
) -> None:
    project = _write_project(tmp_path, tests=tests)
    monkeypatch.setattr(StreamtTestRunner, "run", _passing_results)
    monkeypatch.setattr(
        test_command,
        "load_openlineage_transport_config",
        lambda *_args, **_kwargs: pytest.fail("non-run path loaded a transport"),
    )

    result = _invoke(project, "--emit-openlineage", *arguments)

    assert result.exit_code == 0


def test_preflight_requires_job_namespace_before_runner_or_transport(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project = _write_project(
        tmp_path, tests=[{"name": "schema", "model": "clean", "type": "schema"}]
    )
    constructed = False

    def fail_constructor(_self: StreamtTestRunner, _project: StreamtProject) -> None:
        nonlocal constructed
        constructed = True

    monkeypatch.setattr(StreamtTestRunner, "__init__", fail_constructor)
    monkeypatch.setattr(
        test_command,
        "load_openlineage_transport_config",
        lambda *_args, **_kwargs: pytest.fail("namespace failure reached transport config"),
    )

    result = _invoke(project, "--emit-openlineage", output="json")

    assert result.exit_code == 1
    envelope = _json_envelope(result)
    assert envelope["errors"] == [
        {
            "code": "E506_OPENLINEAGE_INVALID",
            "message": "OpenLineage job namespace must contain a non-whitespace character",
            "location": "job_namespace",
        }
    ]
    assert constructed is False


def test_transport_configuration_failure_is_safe_and_precedes_runner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project = _write_project(
        tmp_path, tests=[{"name": "schema", "model": "clean", "type": "schema"}]
    )
    secret_path = "/private/a-secret/openlineage.yml"
    monkeypatch.setenv("OPENLINEAGE_CONFIG", secret_path)
    monkeypatch.setattr(
        StreamtTestRunner,
        "__init__",
        lambda *_args: pytest.fail("runner was constructed before transport preflight"),
    )

    result = _invoke(project, *_emission_arguments(), output="json")

    assert result.exit_code == 1
    assert secret_path not in result.output
    error = _json_envelope(result)["errors"][0]  # type: ignore[index]
    assert error["code"] == "E506_OPENLINEAGE_INVALID"
    assert error["location"] == "openlineage.config"
    assert len(error["message"]) <= 1024


def test_options_win_over_environment_and_use_kafka_not_gateway_namespace(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project = _write_project(tmp_path, tests=[{"name": "sample", "model": "raw", "type": "sample"}])
    transport = _FakeTransport()
    _install_transport(monkeypatch, transport)
    monkeypatch.setattr(StreamtTestRunner, "run", _passing_results)
    monkeypatch.setenv("OPENLINEAGE_NAMESPACE", "environment-jobs")
    monkeypatch.setenv("STREAMT_OPENLINEAGE_KAFKA_NAMESPACE", "kafka://environment.example:19092")
    monkeypatch.setenv("STREAMT_OPENLINEAGE_GATEWAY_NAMESPACE", "kafka://gateway.example:16969")

    result = _invoke(
        project,
        "--emit-openlineage",
        "--openlineage-job-namespace",
        "option-jobs",
        "--openlineage-kafka-namespace",
        "kafka://option.example:29092",
        "--openlineage-gateway-namespace",
        "kafka://unused-gateway.example:26969",
    )

    assert result.exit_code == 0
    start = transport.attempts[0]
    assert start["job"] == {
        "namespace": "option-jobs",
        "name": "streamt/telemetry%20project/commands/test",
        "facets": start["job"]["facets"],  # type: ignore[index]
    }
    assert start["inputs"] == [
        {"namespace": "kafka://option.example:29092", "name": "raw.events.v1"}
    ]


def test_dotenv_namespaces_are_available_after_project_parse(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project = _write_project(tmp_path, tests=[{"name": "sample", "model": "raw", "type": "sample"}])
    project.joinpath(".env").write_text(
        "OPENLINEAGE_NAMESPACE=dotenv-jobs\n"
        "STREAMT_OPENLINEAGE_KAFKA_NAMESPACE=kafka://dotenv.example:39092\n",
        encoding="utf-8",
    )
    transport = _FakeTransport()
    _install_transport(monkeypatch, transport)
    monkeypatch.setattr(StreamtTestRunner, "run", _passing_results)

    result = _invoke(project, "--emit-openlineage")

    assert result.exit_code == 0
    assert transport.attempts[0]["job"]["namespace"] == "dotenv-jobs"  # type: ignore[index]
    assert transport.attempts[0]["inputs"] == [
        {"namespace": "kafka://dotenv.example:39092", "name": "raw.events.v1"}
    ]


@pytest.mark.parametrize(
    ("option", "value", "location"),
    [
        ("--openlineage-kafka-namespace", "https://not-kafka.invalid", "kafka_namespace"),
        ("--openlineage-gateway-namespace", "kafka://many:9092,other:9092", "gateway_namespace"),
    ],
)
def test_explicit_unused_dataset_namespaces_are_still_validated(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    option: str,
    value: str,
    location: str,
) -> None:
    project = _write_project(
        tmp_path, tests=[{"name": "schema", "model": "clean", "type": "schema"}]
    )
    monkeypatch.setattr(
        test_command,
        "load_openlineage_transport_config",
        lambda *_args, **_kwargs: pytest.fail("invalid namespace reached transport config"),
    )

    result = _invoke(
        project,
        *_emission_arguments(option, value),
        output="json",
    )

    assert result.exit_code == 1
    assert _json_envelope(result)["errors"][0]["location"] == location  # type: ignore[index]


@pytest.mark.parametrize("selected_type", ["schema", "continuous"])
def test_non_sample_runs_emit_an_aggregate_run_without_inputs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    selected_type: str,
) -> None:
    project = _write_project(tmp_path)
    transport = _FakeTransport()
    _install_transport(monkeypatch, transport)
    monkeypatch.setattr(StreamtTestRunner, "run", _passing_results)

    result = _invoke(
        project,
        *_emission_arguments(),
        "--type",
        selected_type,
    )

    assert result.exit_code == 0
    assert _event_types(transport) == ["START", "COMPLETE"]
    assert all("inputs" not in event for event in transport.attempts)


def test_mixed_selection_uses_only_sorted_unique_topics_consumed_by_sample_tests(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project = _write_project(tmp_path)
    transport = _FakeTransport()
    _install_transport(monkeypatch, transport)
    monkeypatch.setattr(StreamtTestRunner, "run", _passing_results)

    result = _invoke(project, *_emission_arguments())

    assert result.exit_code == 0
    expected_inputs = [
        {"namespace": "kafka://broker.example:9092", "name": "clean.physical.v2"},
        {"namespace": "kafka://broker.example:9092", "name": "raw.events.v1"},
    ]
    assert transport.attempts[0]["inputs"] == expected_inputs
    assert transport.attempts[1]["inputs"] == expected_inputs
    job_type = transport.attempts[0]["job"]["facets"]["jobType"]  # type: ignore[index]
    assert {
        "processingType": job_type["processingType"],
        "integration": job_type["integration"],
        "jobType": job_type["jobType"],
    } == {"processingType": "BATCH", "integration": "STREAMT", "jobType": "TEST"}


def test_shared_sample_topic_resolver_matches_runner_behavior() -> None:
    project = StreamtProject.model_validate(_project_config())
    runner = StreamtTestRunner(project)

    for target, expected in (
        ("clean", "clean.physical.v2"),
        ("raw", "raw.events.v1"),
        ("missing", None),
    ):
        assert resolve_sample_test_topic(project, target) == expected
        assert runner._get_topic_for_test(target) == expected


def test_run_uuid_is_v4_stable_across_pair_and_new_per_invocation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project = _write_project(
        tmp_path, tests=[{"name": "schema", "model": "clean", "type": "schema"}]
    )
    first = _FakeTransport()
    second = _FakeTransport()
    _install_transport(monkeypatch, first, second)
    monkeypatch.setattr(StreamtTestRunner, "run", _passing_results)

    first_result = _invoke(project, *_emission_arguments())
    second_result = _invoke(project, *_emission_arguments())

    assert first_result.exit_code == second_result.exit_code == 0
    first_ids = [event["run"]["runId"] for event in first.attempts]  # type: ignore[index]
    second_ids = [event["run"]["runId"] for event in second.attempts]  # type: ignore[index]
    assert first_ids[0] == first_ids[1]
    assert second_ids[0] == second_ids[1]
    assert first_ids[0] != second_ids[0]
    assert UUID(first_ids[0]).version == UUID(second_ids[0]).version == 4
    assert first.attempts[0]["job"] == first.attempts[1]["job"]


@pytest.mark.parametrize(
    ("results", "expected_terminal", "expected_exit"),
    [
        ([{"name": "schema", "status": "passed"}], "COMPLETE", 0),
        ([{"name": "schema", "status": "failed", "errors": ["assertion secret"]}], "FAIL", 1),
        ([{"name": "schema", "status": "skipped", "errors": []}], "FAIL", 1),
        ([], "FAIL", 0),
    ],
)
def test_result_truth_selects_complete_or_fail_without_copying_details(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    results: list[dict[str, object]],
    expected_terminal: str,
    expected_exit: int,
) -> None:
    project = _write_project(
        tmp_path, tests=[{"name": "schema", "model": "clean", "type": "schema"}]
    )
    transport = _FakeTransport()
    _install_transport(monkeypatch, transport)
    monkeypatch.setattr(StreamtTestRunner, "run", lambda *_args: results)

    result = _invoke(project, *_emission_arguments())

    assert result.exit_code == expected_exit
    assert _event_types(transport) == ["START", expected_terminal]
    terminal = transport.attempts[1]
    if expected_terminal == "FAIL":
        facet = terminal["run"]["facets"]["errorMessage"]  # type: ignore[index]
        assert facet["message"] == "streamt test command did not complete successfully"
        assert "assertion secret" not in json.dumps(terminal)
        assert "stackTrace" not in facet
    else:
        assert "facets" not in terminal["run"]  # type: ignore[operator]


class TestExecutionTerminalEvents:
    """Execution exceptions retain their identity while producing one terminal attempt."""

    runner_error: ClassVar[RuntimeError] = RuntimeError("original runner exception")

    @pytest.mark.parametrize(
        ("raised", "expected_terminal"),
        [(runner_error, "FAIL"), (KeyboardInterrupt(), "ABORT")],
    )
    def test_uncaught_and_interrupted_execution_choose_terminal(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        raised: BaseException,
        expected_terminal: str,
    ) -> None:
        project = _write_project(
            tmp_path, tests=[{"name": "schema", "model": "clean", "type": "schema"}]
        )
        transport = _FakeTransport()
        _install_transport(monkeypatch, transport)

        def raise_from_runner(*_args: object) -> list[dict[str, object]]:
            raise raised

        monkeypatch.setattr(StreamtTestRunner, "run", raise_from_runner)

        result = _invoke(project, *_emission_arguments())

        assert result.exit_code == 1
        assert _event_types(transport) == ["START", expected_terminal]
        assert transport.close_calls == 1
        if isinstance(raised, RuntimeError):
            assert result.exception is raised


@pytest.mark.parametrize(
    ("fail_attempts", "fail_close", "warning_location"),
    [
        ({1}, False, "openlineage.start"),
        ({2}, False, "openlineage.terminal"),
        (set(), True, "openlineage.transport"),
    ],
)
def test_delivery_and_close_failures_are_structured_w112_and_keep_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fail_attempts: set[int],
    fail_close: bool,
    warning_location: str,
) -> None:
    project = _write_project(
        tmp_path, tests=[{"name": "schema", "model": "clean", "type": "schema"}]
    )
    transport = _FakeTransport(fail_attempts=fail_attempts, fail_close=fail_close)
    _install_transport(monkeypatch, transport)
    monkeypatch.setattr(StreamtTestRunner, "run", _passing_results)

    result = _invoke(project, *_emission_arguments(), output="json")

    assert result.exit_code == 0
    assert result.stderr == ""
    envelope = _json_envelope(result)
    assert envelope["status"] == "ok"
    assert envelope["data"]["passed"] == 1  # type: ignore[index]
    warning = envelope["warnings"][0]  # type: ignore[index]
    assert warning["code"] == "W112_OPENLINEAGE_EMIT_FAILED"
    assert warning["location"] == warning_location
    rendered = json.dumps(envelope)
    assert "user:key" not in rendered
    assert "/private" not in rendered
    assert transport.close_calls == 1


def test_terminal_and_close_failures_never_replace_original_execution_exception(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project = _write_project(
        tmp_path, tests=[{"name": "schema", "model": "clean", "type": "schema"}]
    )
    transport = _FakeTransport(fail_attempts={2}, fail_close=True)
    _install_transport(monkeypatch, transport)
    original = RuntimeError("original test execution error")

    def fail_run(*_args: object) -> list[dict[str, object]]:
        raise original

    monkeypatch.setattr(StreamtTestRunner, "run", fail_run)

    result = _invoke(project, *_emission_arguments())

    assert result.exception is original
    assert _event_types(transport) == ["START", "FAIL"]
    assert transport.close_calls == 1


def test_delivery_failure_does_not_hide_a_structured_test_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project = _write_project(
        tmp_path, tests=[{"name": "schema", "model": "clean", "type": "schema"}]
    )
    transport = _FakeTransport(fail_attempts={2})
    _install_transport(monkeypatch, transport)
    monkeypatch.setattr(
        StreamtTestRunner,
        "run",
        lambda *_args: [{"name": "schema", "status": "failed", "errors": ["real failure"]}],
    )

    result = _invoke(project, *_emission_arguments(), output="json")

    assert result.exit_code == 1
    envelope = _json_envelope(result)
    assert envelope["status"] == "error"
    assert envelope["data"]["failed"] == 1  # type: ignore[index]
    assert envelope["data"]["results"][0]["errors"] == ["real failure"]  # type: ignore[index]
    assert envelope["warnings"][0]["code"] == "W112_OPENLINEAGE_EMIT_FAILED"  # type: ignore[index]
