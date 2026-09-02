"""Remote-state command presentation without enabling the PostgreSQL factory."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast
from unittest.mock import MagicMock, patch

import pytest
import yaml
from click.testing import CliRunner, Result

from streamt.cli import main
from streamt.compiler.manifest import (
    ArtifactOwnership,
    Manifest,
    SchemaArtifact,
    TopicArtifact,
)
from streamt.deployer.kafka import TopicChange, TopicState
from streamt.deployer.schema_registry import SchemaState
from streamt.deployer.state import local_state_path, resource_id
from streamt.deployer.state_backend import (
    LOCAL_STATE_NAMESPACE,
    DeploymentStateService,
    LocalDeploymentStateBackend,
    StateAddress,
)


def _write_remote_project(path: Path) -> None:
    (path / "stream_project.yml").write_text(
        yaml.safe_dump(
            {
                "apiVersion": "streamt.dev/v1alpha1",
                "project": {"name": "remote-presentation"},
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                "deployment_state": {
                    "backend": "postgres",
                    "namespace": "platform",
                    "postgres": {"dsn_env": "REMOTE_PRESENTATION_DSN"},
                },
            }
        ),
        encoding="utf-8",
    )


def _state_service(path: Path) -> DeploymentStateService:
    """Exercise the provider-neutral lifecycle without selecting PostgreSQL."""
    return DeploymentStateService(
        backend=LocalDeploymentStateBackend(path),
        address=StateAddress(
            namespace=LOCAL_STATE_NAMESPACE,
            project="remote-presentation",
            environment="default",
        ),
    )


def _topic(*, mode: str = "managed") -> TopicArtifact:
    return TopicArtifact(
        name="orders.v1",
        partitions=3,
        replication_factor=1,
        ownership=ArtifactOwnership(
            project="remote-presentation",
            owner_type="model",
            owner_name="orders",
            mode=mode,
        ),
    )


def _manifest(topic: TopicArtifact) -> Manifest:
    return Manifest(
        version="1.0",
        project_name="remote-presentation",
        artifacts={"topics": [topic.to_dict()]},
    )


def _schema() -> SchemaArtifact:
    return SchemaArtifact(
        subject="orders.v1-value",
        schema={
            "type": "record",
            "name": "Order",
            "fields": [{"name": "id", "type": "string"}],
        },
        schema_type="AVRO",
        compatibility="BACKWARD",
        ownership=ArtifactOwnership(
            project="remote-presentation",
            owner_type="source",
            owner_name="orders",
            mode="adopted",
        ),
    )


def _kafka(topic: TopicArtifact, *, exists: bool) -> MagicMock:
    deployer = MagicMock()
    current = TopicState(
        name=topic.name,
        exists=exists,
        partitions=topic.partitions if exists else None,
        replication_factor=topic.replication_factor if exists else None,
        config=dict(topic.config) if exists else {},
    )
    deployer.plan_topic.return_value = TopicChange(
        topic=topic.name,
        action="none" if exists else "create",
        current=current,
        desired=topic,
    )
    deployer.apply_topic.return_value = "created"
    deployer.get_consumer_groups.return_value = []
    deployer.get_topic_state.return_value = TopicState(
        name=topic.name,
        exists=True,
        partitions=topic.partitions,
        replication_factor=topic.replication_factor,
        config=dict(topic.config),
    )
    return deployer


def _payload(result: Result) -> dict[str, Any]:
    return cast(dict[str, Any], json.loads(result.stdout))


def _assert_no_local_state_presentation(result: Result) -> None:
    payload = _payload(result)
    assert all(
        warning.get("code") != "W106_LOCAL_STATE_ONLY" for warning in payload.get("warnings", [])
    )
    assert "state_file" not in payload.get("data", {})


def _run_plan(
    path: Path,
    *,
    service: DeploymentStateService,
    topic: TopicArtifact,
    kafka: MagicMock,
    output_path: Path | None = None,
) -> Result:
    args = ["-o", "json", "plan", "-p", str(path)]
    if output_path is not None:
        args.extend(["--out", str(output_path)])
    with (
        patch("streamt.compiler.Compiler.compile", return_value=_manifest(topic)),
        patch(
            "streamt.cli.commands.plan.make_deployment_state_service",
            return_value=service,
        ),
        patch(
            "streamt.cli.commands.plan.make_kafka_deployer",
            return_value=kafka,
        ),
    ):
        return CliRunner().invoke(main, args)


def _run_apply(
    path: Path,
    *,
    service: DeploymentStateService,
    topic: TopicArtifact,
    kafka: MagicMock,
    reviewed_plan: Path | None = None,
) -> Result:
    args = ["-o", "json", "apply", "-p", str(path)]
    if reviewed_plan is not None:
        args.extend(["--plan", str(reviewed_plan)])
    with (
        patch("streamt.compiler.Compiler.compile", return_value=_manifest(topic)),
        patch(
            "streamt.cli.commands.apply.make_deployment_state_service",
            return_value=service,
        ),
        patch(
            "streamt.cli.commands.apply.make_kafka_deployer",
            return_value=kafka,
        ),
    ):
        return CliRunner().invoke(main, args)


def test_remote_online_plan_omits_local_state_warning(
    tmp_path: Path,
) -> None:
    _write_remote_project(tmp_path)
    topic = _topic()
    result = _run_plan(
        tmp_path,
        service=_state_service(tmp_path),
        topic=topic,
        kafka=_kafka(topic, exists=False),
    )

    assert result.exit_code == 0, result.output
    _assert_no_local_state_presentation(result)
    assert str(local_state_path(tmp_path, environment="default")) not in result.output


@pytest.mark.parametrize("reviewed", [False, True], ids=["direct", "reviewed"])
def test_remote_apply_omits_local_state_warning_and_file(
    tmp_path: Path,
    reviewed: bool,
) -> None:
    _write_remote_project(tmp_path)
    service = _state_service(tmp_path)
    topic = _topic()
    reviewed_plan = tmp_path / "reviewed-plan.json" if reviewed else None
    if reviewed_plan is not None:
        planned = _run_plan(
            tmp_path,
            service=service,
            topic=topic,
            kafka=_kafka(topic, exists=False),
            output_path=reviewed_plan,
        )
        assert planned.exit_code == 0, planned.output
        _assert_no_local_state_presentation(planned)

    result = _run_apply(
        tmp_path,
        service=service,
        topic=topic,
        kafka=_kafka(topic, exists=False),
        reviewed_plan=reviewed_plan,
    )

    assert result.exit_code == 0, result.output
    assert _payload(result)["data"]["state_serial"] == 1
    _assert_no_local_state_presentation(result)
    assert str(local_state_path(tmp_path, environment="default")) not in result.output


def _run_adopt(
    path: Path,
    *,
    service: DeploymentStateService,
    topic: TopicArtifact,
    kafka: MagicMock,
    confirmed: bool,
) -> Result:
    args = [
        "-o",
        "json",
        "adopt",
        "-p",
        str(path),
        "-e",
        "default",
        "--kind",
        "topic",
        "--name",
        "orders",
    ]
    if confirmed:
        args.extend(
            [
                "--confirm-resource",
                resource_id(
                    "remote-presentation",
                    "default",
                    "topic",
                    "orders",
                ),
                "--confirm-env",
                "default",
            ]
        )
    with (
        patch("streamt.compiler.Compiler.compile", return_value=_manifest(topic)),
        patch(
            "streamt.cli.commands.adopt.make_deployment_state_service",
            return_value=service,
        ),
        patch(
            "streamt.cli.commands.adopt.make_kafka_deployer",
            return_value=kafka,
        ),
    ):
        return CliRunner().invoke(main, args)


def test_remote_adoption_success_and_idempotency_omit_local_state_file(
    tmp_path: Path,
) -> None:
    _write_remote_project(tmp_path)
    service = _state_service(tmp_path)
    topic = _topic(mode="adopted")
    kafka = _kafka(topic, exists=True)

    adopted = _run_adopt(
        tmp_path,
        service=service,
        topic=topic,
        kafka=kafka,
        confirmed=True,
    )
    repeated = _run_adopt(
        tmp_path,
        service=service,
        topic=topic,
        kafka=kafka,
        confirmed=True,
    )

    assert adopted.exit_code == 0, adopted.output
    assert _payload(adopted)["data"]["adopted"] is True
    _assert_no_local_state_presentation(adopted)
    assert repeated.exit_code == 0, repeated.output
    assert _payload(repeated)["data"]["already_owned"] is True
    _assert_no_local_state_presentation(repeated)


def test_remote_adoption_error_data_omits_local_state_file(
    tmp_path: Path,
) -> None:
    _write_remote_project(tmp_path)
    service = _state_service(tmp_path)
    topic = _topic(mode="adopted")
    result = _run_adopt(
        tmp_path,
        service=service,
        topic=topic,
        kafka=_kafka(topic, exists=True),
        confirmed=False,
    )

    assert result.exit_code == 1, result.output
    assert _payload(result)["errors"][0]["code"] == ("E414_ADOPTION_CONFIRMATION_REQUIRED")
    _assert_no_local_state_presentation(result)


def test_remote_schema_adoption_omits_local_state_file(
    tmp_path: Path,
) -> None:
    _write_remote_project(tmp_path)
    service = _state_service(tmp_path)
    schema = _schema()
    manifest = Manifest(
        version="1.0",
        project_name="remote-presentation",
        artifacts={"schemas": [schema.to_dict()]},
    )
    registry = MagicMock()
    registry.get_schema_state.return_value = SchemaState(
        subject=schema.subject,
        exists=True,
        version=3,
        schema_id=17,
        schema=schema.schema,
        schema_type=schema.schema_type,
        compatibility=schema.compatibility,
    )
    args = [
        "-o",
        "json",
        "adopt",
        "-p",
        str(tmp_path),
        "-e",
        "default",
        "--kind",
        "schema",
        "--name",
        "orders",
        "--confirm-resource",
        resource_id(
            "remote-presentation",
            "default",
            "schema",
            "orders",
        ),
        "--confirm-env",
        "default",
    ]
    with (
        patch("streamt.compiler.Compiler.compile", return_value=manifest),
        patch(
            "streamt.cli.commands.adopt.make_deployment_state_service",
            return_value=service,
        ),
        patch(
            "streamt.cli.commands.adopt.make_sr_deployer",
            return_value=registry,
        ),
    ):
        result = CliRunner().invoke(main, args)

    assert result.exit_code == 0, result.output
    assert _payload(result)["data"]["kind"] == "schema"
    _assert_no_local_state_presentation(result)
