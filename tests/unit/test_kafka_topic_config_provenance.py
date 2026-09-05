"""Only explicit topic overrides participate in topic configuration reconciliation."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from confluent_kafka.admin import AlterConfigOpType, ConfigSource

from streamt.compiler.manifest import TopicArtifact
from streamt.deployer.kafka import KafkaDeployer


def deployer_with_entries(*entries):
    deployer = KafkaDeployer.__new__(KafkaDeployer)
    deployer._closed = False
    deployer.admin = MagicMock()
    deployer.admin.list_topics.return_value = SimpleNamespace(topics={
        "events": SimpleNamespace(partitions={0: SimpleNamespace(replicas=[1])}),
    })
    future = MagicMock()
    future.result.return_value = {entry.name: entry for entry in entries}
    deployer.admin.describe_configs.return_value = {MagicMock(): future}
    return deployer


def entry(source, *, name="min.insync.replicas", value="1", is_default=False):
    # A lightweight response double retains the SDK's real enum boundary.
    return SimpleNamespace(name=name, value=value, source=source, is_default=is_default)


def artifact(config=None):
    return TopicArtifact(name="events", partitions=1, replication_factor=1, config=config or {})


@pytest.mark.parametrize("source", [
    ConfigSource.DYNAMIC_BROKER_CONFIG,
    ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG,
    ConfigSource.STATIC_BROKER_CONFIG,
    ConfigSource.DEFAULT_CONFIG,
    ConfigSource.UNKNOWN_CONFIG,
    ConfigSource.GROUP_CONFIG,
    0, 2, 3, 4, 5, 8, 999,
    None,
    True,
    1.0,
    "DYNAMIC_TOPIC_CONFIG",
])
@pytest.mark.parametrize("is_default", [False, True])
def test_non_topic_configuration_is_not_a_managed_override(source, is_default):
    deployer = deployer_with_entries(entry(source, is_default=is_default))
    state = deployer.get_topic_state("events", strict_config=True)
    assert state.config == {}
    assert deployer.plan_topic(artifact()).action == "none"
    deployer.admin.incremental_alter_configs.assert_not_called()


@pytest.mark.parametrize("is_default", [False, True])
@pytest.mark.parametrize("source", [ConfigSource.DYNAMIC_TOPIC_CONFIG, 1])
def test_explicit_topic_source_controls_override_even_if_default_flag_disagrees(is_default, source):
    deployer = deployer_with_entries(entry(source, is_default=is_default))
    assert deployer.get_topic_state("events").config == {"min.insync.replicas": "1"}
    assert deployer.plan_topic(artifact({"min.insync.replicas": "1"})).action == "none"
    assert deployer.plan_topic(artifact()).changes == {
        "config.min.insync.replicas": {"from": "1", "to": None},
    }


def test_explicit_desired_value_is_set_even_when_equal_to_inherited_value():
    deployer = deployer_with_entries(entry(ConfigSource.STATIC_BROKER_CONFIG))
    change = deployer.plan_topic(artifact({"min.insync.replicas": "1"}))
    assert change.action == "update"
    assert change.changes == {"config.min.insync.replicas": {"from": None, "to": "1"}}


def test_mixed_config_removes_only_topic_override_and_preserves_broker_inheritance():
    deployer = deployer_with_entries(
        entry(ConfigSource.STATIC_BROKER_CONFIG),
        entry(ConfigSource.DYNAMIC_TOPIC_CONFIG, name="retention.ms", value="600000"),
    )
    desired = artifact()
    change = deployer.plan_topic(desired)
    assert change.changes == {"config.retention.ms": {"from": "600000", "to": None}}
    deployer.update_topic(desired, change.changes)
    resource = deployer.admin.incremental_alter_configs.call_args.args[0][0]
    operations = resource.incremental_configs
    assert len(operations) == 1
    assert operations[0].name == "retention.ms"
    assert operations[0].value is None
    assert operations[0].incremental_operation == AlterConfigOpType.DELETE


def test_non_removed_override_still_uses_set():
    deployer = deployer_with_entries()
    desired = artifact({"retention.ms": "600000"})
    deployer.update_topic(desired, {"config.retention.ms": {"from": None, "to": "600000"}})
    operation = deployer.admin.incremental_alter_configs.call_args.args[0][0].incremental_configs[0]
    assert operation.name == "retention.ms"
    assert operation.value == "600000"
    assert operation.incremental_operation == AlterConfigOpType.SET
