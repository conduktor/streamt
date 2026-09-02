"""Reliability tests for the read-only status command."""

from __future__ import annotations

import json
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest
from click.testing import CliRunner, Result

from streamt.cli import main
from streamt.compiler.gateway_artifact import parse_compiled_gateway_rule_artifact
from streamt.compiler.manifest import Manifest
from streamt.deployer.flink import FlinkJobState
from streamt.deployer.gateway import (
    GatewayBackendBinding,
    GatewayDeployer,
    GatewayManagedObservationError,
    ManagedGatewayInterceptor,
    ManagedGatewayRuleObservation,
    ManagedGatewaySnapshot,
    build_desired_gateway_rule,
)
from streamt.deployer.kafka import (
    ConsumerGroupLag,
    ConsumerGroupObservationError,
    PartitionLag,
    TopicState,
)


def _project() -> MagicMock:
    project = MagicMock()
    project.project.name = "status-test"
    project.sources = []
    project.runtime.schema_registry = None
    project.runtime.flink = None
    project.runtime.connect = None
    project.runtime.conduktor = None
    return project


def _invoke_status(
    tmp_path: Path,
    artifacts: dict[str, list[dict[str, object]]],
    *,
    status_args: list[str] | None = None,
    json_output: bool = True,
    project: MagicMock | None = None,
    kafka: object | None = None,
    schema_registry: object | None = None,
    flink: object | None = None,
    connect: object | None = None,
    gateway: object | None = None,
) -> Result:
    manifest = Manifest(version="1", project_name="status-test", artifacts=artifacts)
    with ExitStack() as stack:
        parser = stack.enter_context(patch("streamt.core.parser.ProjectParser"))
        compiler = stack.enter_context(patch("streamt.compiler.Compiler"))
        stack.enter_context(
            patch("streamt.cli.commands.status.make_kafka_deployer", return_value=kafka)
        )
        stack.enter_context(
            patch(
                "streamt.cli.commands.status.make_sr_deployer",
                return_value=schema_registry,
            )
        )
        stack.enter_context(
            patch("streamt.cli.commands.status.make_flink_deployer", return_value=flink)
        )
        stack.enter_context(
            patch("streamt.cli.commands.status.make_connect_deployer", return_value=connect)
        )
        stack.enter_context(
            patch("streamt.cli.commands.status.make_gateway_deployer", return_value=gateway)
        )
        parser.return_value.parse.return_value = project or _project()
        compiler.return_value.compile.return_value = manifest
        return CliRunner().invoke(
            main,
            [
                *(["-o", "json"] if json_output else []),
                "status",
                *(status_args or []),
                "-p",
                str(tmp_path),
            ],
        )


_GATEWAY_ENDPOINT = "https://gateway.status.example.test/admin"
_GATEWAY_FILTER_SECRET = "status-filter-secret-6017"


def _gateway_binding() -> GatewayBackendBinding:
    return GatewayBackendBinding.from_endpoint(
        _GATEWAY_ENDPOINT,
        virtual_cluster="status-vcluster",
    )


def _gateway_artifact(
    *,
    name: str = "orders_rule",
    virtual_topic: str = "orders.public",
    physical_topic: str = "orders.v1",
    where: str | None = None,
) -> dict[str, object]:
    interceptors: list[dict[str, object]] = []
    if where is not None:
        interceptors.append({"type": "filter", "config": {"where": where}})
    return {
        "name": name,
        "virtualTopic": virtual_topic,
        "physicalTopic": physical_topic,
        "interceptors": interceptors,
    }


def _gateway_desired(
    raw_artifact: dict[str, object],
    binding: GatewayBackendBinding,
) -> ManagedGatewayRuleObservation:
    return build_desired_gateway_rule(
        parse_compiled_gateway_rule_artifact(raw_artifact),
        binding,
    )


def _gateway_present(
    desired: ManagedGatewayRuleObservation,
    *,
    physical_name: str | None = None,
    interceptors: tuple[ManagedGatewayInterceptor, ...] | None = None,
) -> ManagedGatewayRuleObservation:
    return ManagedGatewayRuleObservation(
        binding=desired.binding,
        logical_name=desired.logical_name,
        alias_name=desired.alias_name,
        exists=True,
        physical_name=physical_name or desired.physical_name,
        physical_cluster="main",
        interceptors=(desired.interceptors if interceptors is None else interceptors),
    )


def _gateway_absent(
    desired: ManagedGatewayRuleObservation,
) -> ManagedGatewayRuleObservation:
    return ManagedGatewayRuleObservation(
        binding=desired.binding,
        logical_name=desired.logical_name,
        alias_name=desired.alias_name,
        exists=False,
    )


def _gateway_deployer(
    binding: GatewayBackendBinding,
    snapshot: ManagedGatewaySnapshot | MagicMock,
) -> MagicMock:
    gateway = MagicMock(spec=GatewayDeployer)
    gateway.cluster_binding = binding
    gateway.observe_managed_gateway_snapshot.return_value = snapshot
    return gateway


def test_health_treats_missing_job_with_no_status_as_unhealthy(tmp_path: Path) -> None:
    flink = MagicMock()
    flink.get_job_state.return_value = FlinkJobState(name="orders", exists=False)

    health_result = _invoke_status(
        tmp_path,
        {"flink_jobs": [{"name": "orders"}]},
        status_args=["--health"],
        flink=flink,
    )
    assert health_result.exit_code == 1
    assert json.loads(health_result.output)["data"]["flink_jobs"] == [
        {"name": "orders", "exists": False, "job_id": None, "status": None}
    ]


def test_health_treats_missing_schema_as_unhealthy(tmp_path: Path) -> None:
    schema_registry = MagicMock()
    schema_registry.get_schema_state.return_value.exists = False

    result = _invoke_status(
        tmp_path,
        {"schemas": [{"subject": "orders-value"}]},
        status_args=["--health"],
        schema_registry=schema_registry,
    )

    assert result.exit_code == 1
    assert json.loads(result.output)["data"]["schemas"][0]["exists"] is False


def test_health_fails_when_declared_resource_cannot_be_observed(tmp_path: Path) -> None:
    result = _invoke_status(
        tmp_path,
        {"flink_jobs": [{"name": "orders"}]},
        status_args=["--health"],
        flink=None,
    )

    assert result.exit_code == 1
    assert json.loads(result.output)["data"]["flink_jobs"] == []


def test_health_fails_on_redacted_backend_error(tmp_path: Path) -> None:
    kafka = MagicMock()
    kafka.get_topic_state.side_effect = RuntimeError(
        "connection refused, password=supersecret"
    )

    result = _invoke_status(
        tmp_path,
        {"topics": [{"name": "orders", "partitions": 1, "replication_factor": 1}]},
        status_args=["--health"],
        kafka=kafka,
    )

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["status"] == "error"
    assert payload["errors"][0]["code"] == "E406_CONNECTION_REFUSED"
    assert "<redacted>" in payload["errors"][0]["message"]
    assert "supersecret" not in result.output


def test_gateway_status_reuses_one_snapshot_for_exact_multiple_rules(
    tmp_path: Path,
) -> None:
    binding = _gateway_binding()
    orders = _gateway_artifact(where=f"customer_token = '{_GATEWAY_FILTER_SECRET}'")
    payments = _gateway_artifact(
        name="payments_rule",
        virtual_topic="payments.public",
        physical_topic="payments.v1",
    )
    desired_orders = _gateway_desired(orders, binding)
    desired_payments = _gateway_desired(payments, binding)
    snapshot = MagicMock(spec=ManagedGatewaySnapshot)
    snapshot.rule.side_effect = [desired_orders, desired_payments]
    gateway = _gateway_deployer(binding, snapshot)

    result = _invoke_status(
        tmp_path,
        {"gateway_rules": [orders, payments]},
        gateway=gateway,
    )

    assert result.exit_code == 0, result.output
    rules = json.loads(result.output)["data"]["gateway_rules"]
    assert [rule["status"] for rule in rules] == ["OK", "OK"]
    assert [rule["interceptors_desired"] for rule in rules] == [1, 0]
    assert [rule["interceptors_found"] for rule in rules] == [1, 0]
    assert all(rule["scope"] == "status-vcluster" for rule in rules)
    assert all(rule["backend_fingerprint"] == binding.endpoint_fingerprint for rule in rules)
    assert rules[0]["observed_physical_topic"] == "orders.v1"
    assert rules[0]["observed_physical_cluster"] == "main"
    assert "drift_categories" not in rules[0]
    gateway.observe_managed_gateway_snapshot.assert_called_once_with()
    snapshot.rule.assert_has_calls(
        [
            call("orders_rule", "orders.public"),
            call("payments_rule", "payments.public"),
        ]
    )
    gateway.get_alias_topic.assert_not_called()
    gateway.get_interceptor.assert_not_called()
    assert _GATEWAY_ENDPOINT not in result.output
    assert _GATEWAY_FILTER_SECRET not in result.output


@pytest.mark.parametrize(
    ("collision", "message"),
    [
        ("owner", "Gateway manifest maps one logical owner to multiple rules"),
        ("alias", "Gateway manifest contains a duplicate canonical alias locator"),
        ("interceptor", "Gateway manifest contains a duplicate interceptor locator"),
        (
            "generated_namespace",
            "Gateway generated interceptor identity maps to multiple rules",
        ),
    ],
)
def test_gateway_status_rejects_the_same_manifest_collisions_before_snapshot(
    tmp_path: Path,
    collision: str,
    message: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = _gateway_artifact(
        name="first_rule",
        virtual_topic="first.public",
        where=(
            "region = 'us'"
            if collision in {"interceptor", "generated_namespace"}
            else None
        ),
    )
    second = _gateway_artifact(
        name="second_rule",
        virtual_topic="second.public",
        where=(
            "region = 'eu'"
            if collision in {"interceptor", "generated_namespace"}
            else None
        ),
    )
    first["ownership"] = {
        "mode": "managed",
        "project": "status-test",
        "type": "model",
        "name": "first_owner",
    }
    second["ownership"] = {
        "mode": "managed",
        "project": "status-test",
        "type": "model",
        "name": "second_owner",
    }
    if collision == "owner":
        second["ownership"] = dict(first["ownership"])  # type: ignore[arg-type]
    elif collision == "alias":
        second["virtualTopic"] = first["virtualTopic"]
    elif collision == "interceptor":
        second["name"] = first["name"]
    else:
        monkeypatch.setattr(
            "streamt.deployer.gateway.classify_gateway_interceptor_name",
            lambda _logical_name, _candidate: object(),
        )

    binding = _gateway_binding()
    snapshot = MagicMock(spec=ManagedGatewaySnapshot)
    gateway = _gateway_deployer(binding, snapshot)
    result = _invoke_status(
        tmp_path,
        {"gateway_rules": [first, second]},
        status_args=["--health"],
        gateway=gateway,
    )

    assert result.exit_code == 1
    assert message in result.output
    gateway.observe_managed_gateway_snapshot.assert_not_called()


def test_gateway_health_fails_when_managed_rule_is_missing(tmp_path: Path) -> None:
    binding = _gateway_binding()
    artifact = _gateway_artifact(where=f"customer_token = '{_GATEWAY_FILTER_SECRET}'")
    desired = _gateway_desired(artifact, binding)
    snapshot = MagicMock(spec=ManagedGatewaySnapshot)
    snapshot.rule.return_value = _gateway_absent(desired)
    gateway = _gateway_deployer(binding, snapshot)

    result = _invoke_status(
        tmp_path,
        {"gateway_rules": [artifact]},
        status_args=["--health"],
        gateway=gateway,
    )

    assert result.exit_code == 1
    rule = json.loads(result.output)["data"]["gateway_rules"][0]
    assert rule["status"] == "MISSING"
    assert rule["exists"] is False
    assert rule["drift_categories"] == ["presence"]
    assert rule["interceptors_found"] == 0
    assert rule["interceptors_desired"] == 1
    gateway.get_alias_topic.assert_not_called()
    gateway.get_interceptor.assert_not_called()


def test_gateway_health_fails_on_mapping_and_scope_drift(tmp_path: Path) -> None:
    binding = _gateway_binding()
    artifact = _gateway_artifact(where="region = 'US'")
    desired = _gateway_desired(artifact, binding)
    current = _gateway_present(
        desired,
        physical_name="orders.previous",
        # An expected interceptor observed under another scope is absent from
        # the exact target-scope aggregate returned by snapshot.rule().
        interceptors=(),
    )
    snapshot = MagicMock(spec=ManagedGatewaySnapshot)
    snapshot.rule.return_value = current
    gateway = _gateway_deployer(binding, snapshot)

    result = _invoke_status(
        tmp_path,
        {"gateway_rules": [artifact]},
        status_args=["--health"],
        gateway=gateway,
    )

    assert result.exit_code == 1, result.output
    rule = json.loads(result.output)["data"]["gateway_rules"][0]
    assert rule["status"] == "DRIFT"
    assert rule["exists"] is True
    assert rule["observed_physical_topic"] == "orders.previous"
    assert rule["drift_categories"] == [
        "alias_mapping",
        "interceptor_identities",
    ]
    assert rule["current_fingerprint"] != rule["desired_fingerprint"]


def test_gateway_health_fails_on_config_fingerprint_drift(tmp_path: Path) -> None:
    binding = _gateway_binding()
    artifact = _gateway_artifact(where="region = 'US'")
    desired = _gateway_desired(artifact, binding)
    desired_interceptor = desired.interceptors[0]
    drifted_interceptor = ManagedGatewayInterceptor(
        name=desired_interceptor.name,
        scope=desired_interceptor.scope,
        plugin_class=desired_interceptor.plugin_class,
        priority=desired_interceptor.priority,
        config_json="{}",
    )
    snapshot = MagicMock(spec=ManagedGatewaySnapshot)
    snapshot.rule.return_value = _gateway_present(
        desired,
        interceptors=(drifted_interceptor,),
    )
    gateway = _gateway_deployer(binding, snapshot)

    result = _invoke_status(
        tmp_path,
        {"gateway_rules": [artifact]},
        status_args=["--health"],
        gateway=gateway,
    )

    assert result.exit_code == 1, result.output
    rule = json.loads(result.output)["data"]["gateway_rules"][0]
    assert rule["status"] == "DRIFT"
    assert rule["drift_categories"] == ["configuration"]
    assert rule["current_fingerprint"] != rule["desired_fingerprint"]
    assert "config" not in rule


def test_gateway_health_fails_on_stale_managed_interceptor(tmp_path: Path) -> None:
    binding = _gateway_binding()
    artifact = _gateway_artifact(where="region = 'US'")
    desired = _gateway_desired(artifact, binding)
    desired_interceptor = desired.interceptors[0]
    stale_interceptor = ManagedGatewayInterceptor(
        name="orders_rule_filter_1",
        scope=desired_interceptor.scope,
        plugin_class=desired_interceptor.plugin_class,
        priority=desired_interceptor.priority,
        config_json=desired_interceptor.config_json,
    )
    snapshot = MagicMock(spec=ManagedGatewaySnapshot)
    snapshot.rule.return_value = _gateway_present(
        desired,
        interceptors=(desired_interceptor, stale_interceptor),
    )
    gateway = _gateway_deployer(binding, snapshot)

    result = _invoke_status(
        tmp_path,
        {"gateway_rules": [artifact]},
        status_args=["--health"],
        gateway=gateway,
    )

    assert result.exit_code == 1, result.output
    rule = json.loads(result.output)["data"]["gateway_rules"][0]
    assert rule["status"] == "DRIFT"
    assert rule["interceptors_found"] == 2
    assert rule["interceptors_desired"] == 1
    assert rule["drift_categories"] == ["interceptor_identities"]


def test_gateway_health_fails_closed_when_orphan_exists_without_alias(
    tmp_path: Path,
) -> None:
    binding = _gateway_binding()
    artifact = _gateway_artifact(where="region = 'US'")
    snapshot = MagicMock(spec=ManagedGatewaySnapshot)
    snapshot.rule.side_effect = GatewayManagedObservationError(
        "Gateway managed observation is partial: alias is absent with interceptors"
    )
    gateway = _gateway_deployer(binding, snapshot)

    result = _invoke_status(
        tmp_path,
        {"gateway_rules": [artifact]},
        status_args=["--health"],
        gateway=gateway,
    )

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["status"] == "error"
    assert payload["data"]["gateway_rules"] == []
    gateway.observe_managed_gateway_snapshot.assert_called_once_with()
    snapshot.rule.assert_called_once_with("orders_rule", "orders.public")
    gateway.get_alias_topic.assert_not_called()
    gateway.get_interceptor.assert_not_called()
    assert _GATEWAY_ENDPOINT not in result.output
    assert _GATEWAY_FILTER_SECRET not in result.output


def test_gateway_health_fails_closed_when_snapshot_rejects_physical_cluster(
    tmp_path: Path,
) -> None:
    binding = _gateway_binding()
    gateway = MagicMock(spec=GatewayDeployer)
    gateway.cluster_binding = binding
    # The strict snapshot parser rejects an AliasTopic outside the supported
    # physical cluster before it can become a managed rule value object.
    gateway.observe_managed_gateway_snapshot.side_effect = (
        GatewayManagedObservationError(
            "Gateway managed snapshot contains an invalid alias"
        )
    )

    result = _invoke_status(
        tmp_path,
        {"gateway_rules": [_gateway_artifact()]},
        status_args=["--health"],
        gateway=gateway,
    )

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["status"] == "error"
    assert payload["data"]["gateway_rules"] == []
    gateway.observe_managed_gateway_snapshot.assert_called_once_with()
    gateway.get_alias_topic.assert_not_called()
    gateway.get_interceptor.assert_not_called()
    assert _GATEWAY_ENDPOINT not in result.output


def test_gateway_health_fails_closed_before_observation_on_legacy_artifact(
    tmp_path: Path,
) -> None:
    binding = _gateway_binding()
    snapshot = MagicMock(spec=ManagedGatewaySnapshot)
    gateway = _gateway_deployer(binding, snapshot)
    artifact = _gateway_artifact()
    artifact["interceptors"] = [{"name": "legacy-point-read"}]

    result = _invoke_status(
        tmp_path,
        {"gateway_rules": [artifact]},
        status_args=["--health"],
        gateway=gateway,
    )

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["status"] == "error"
    assert payload["data"]["gateway_rules"] == []
    gateway.observe_managed_gateway_snapshot.assert_not_called()
    gateway.get_alias_topic.assert_not_called()
    gateway.get_interceptor.assert_not_called()


def test_gateway_text_status_does_not_render_config_or_admin_endpoint(
    tmp_path: Path,
) -> None:
    binding = _gateway_binding()
    artifact = _gateway_artifact(where=f"customer_token = '{_GATEWAY_FILTER_SECRET}'")
    desired = _gateway_desired(artifact, binding)
    snapshot = MagicMock(spec=ManagedGatewaySnapshot)
    snapshot.rule.return_value = desired
    gateway = _gateway_deployer(binding, snapshot)

    result = _invoke_status(
        tmp_path,
        {"gateway_rules": [artifact]},
        json_output=False,
        gateway=gateway,
    )

    assert result.exit_code == 0, result.output
    assert "OK orders_rule" in result.output
    assert _GATEWAY_ENDPOINT not in result.output
    assert _GATEWAY_FILTER_SECRET not in result.output
    assert "VirtualSqlTopicPlugin" not in result.output
    assert "statement" not in result.output
    gateway.observe_managed_gateway_snapshot.assert_called_once_with()
    gateway.get_alias_topic.assert_not_called()
    gateway.get_interceptor.assert_not_called()


def test_consumer_group_partition_lag_is_json_serializable(tmp_path: Path) -> None:
    kafka = MagicMock()
    kafka.get_topic_state.return_value = TopicState(
        name="orders", exists=True, partitions=1, replication_factor=1
    )
    kafka.get_consumer_groups.return_value = ["analytics"]
    kafka.get_consumer_group_lag.return_value = ConsumerGroupLag(
        group_id="analytics",
        topic="orders",
        total_lag=7,
        partitions=[PartitionLag(partition=0, current_offset=3, end_offset=10, lag=7)],
    )

    result = _invoke_status(
        tmp_path,
        {"topics": [{"name": "orders", "partitions": 1, "replication_factor": 1}]},
        status_args=["--consumer-groups"],
        kafka=kafka,
    )

    assert result.exit_code == 0, result.output
    partitions = json.loads(result.output)["data"]["consumer_groups"][0]["topics"][0][
        "partitions"
    ]
    assert partitions == [
        {"partition": 0, "current_offset": 3, "end_offset": 10, "lag": 7}
    ]


def test_health_fails_closed_on_redacted_consumer_lag_error(tmp_path: Path) -> None:
    kafka = MagicMock()
    kafka.get_topic_state.return_value = TopicState(
        name="orders", exists=True, partitions=1, replication_factor=1
    )
    kafka.get_consumer_groups.return_value = ["analytics"]
    kafka.get_consumer_group_lag.side_effect = ConsumerGroupObservationError(
        "committed-offset query",
        group_id="analytics",
        topic="orders",
        detail="SASL denied, password=supersecret",
    )

    result = _invoke_status(
        tmp_path,
        {"topics": [{"name": "orders", "partitions": 1, "replication_factor": 1}]},
        status_args=["--consumer-groups", "--health"],
        kafka=kafka,
    )

    assert result.exit_code == 1
    assert "supersecret" not in result.output
    payload = json.loads(result.output)
    assert payload["status"] == "error"
    assert payload["errors"][0]["code"] == "E406_CONNECTION_REFUSED"
    assert "<redacted>" in payload["errors"][0]["message"]


def test_summary_does_not_count_drift_as_ok_or_missing_job_as_running(
    tmp_path: Path,
) -> None:
    kafka = MagicMock()
    kafka.get_topic_state.return_value = TopicState(
        name="orders", exists=True, partitions=1, replication_factor=1
    )
    flink = MagicMock()
    flink.get_job_state.return_value = FlinkJobState(name="orders-job", exists=False)

    result = _invoke_status(
        tmp_path,
        {
            "topics": [{"name": "orders", "partitions": 3, "replication_factor": 1}],
            "flink_jobs": [{"name": "orders-job"}],
        },
        json_output=False,
        kafka=kafka,
        flink=flink,
    )

    assert result.exit_code == 0
    assert (
        "Summary: Topics: 0 OK, 0 missing, 1 drift | Jobs: 0 running, 1 other"
        in result.output
    )
