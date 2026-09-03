"""Provider-free identity and prior-state checks for Connector removals."""

from __future__ import annotations

from typing import cast
from unittest.mock import MagicMock

import pytest

from streamt.compiler.connector_artifact import (
    CONNECTOR_REMOVAL_PLANNING_UNAVAILABLE_MESSAGE,
    ConnectorRemovalClusterReferenceError,
    ConnectorRemovalPreflightError,
    ConnectorRemovalRuntimeRequiredError,
    parse_compiled_connector_removal_artifact,
)
from streamt.compiler.manifest import ArtifactOwnership, ConnectorArtifact, Manifest
from streamt.core.deployment_state import (
    PostgresConnectionConfig,
    PostgresDeploymentStateConfig,
    RemoteStateRequiredError,
)
from streamt.core.models import ProjectInfo, StreamtProject
from streamt.core.runtime import ConnectClusterConfig, ConnectConfig, KafkaConfig, RuntimeConfig
from streamt.deployer.connect import ConnectClusterBinding, ConnectClusterBindingError
from streamt.deployer.planner import (
    ConnectorPlanningTargets,
    DeploymentPlanner,
    ResolvedConnectorRemoval,
    require_connector_removal_postgres_state,
    resolve_connector_planning_targets,
)
from streamt.deployer.state import LocalState, ManagedResourceRecord, resource_id

_ENDPOINT = "https://connect.example.test:8443/api/"
_CHECKSUM = "sha256:" + "1" * 64


def _postgres() -> PostgresDeploymentStateConfig:
    return PostgresDeploymentStateConfig(
        backend="postgres",
        namespace="test",
        postgres=PostgresConnectionConfig(
            dsn_env="STREAMT_TEST_ADMIN_DSN",
            writer_dsn_env="STREAMT_TEST_WRITER_DSN",
        ),
    )


def _project(
    *,
    default: str | None = "primary",
    endpoint: str = _ENDPOINT,
    include_alias: bool = False,
    postgres: bool = True,
    name: str = "payments",
) -> StreamtProject:
    clusters = {"primary": ConnectClusterConfig(rest_url=endpoint)}
    if include_alias:
        clusters["same-endpoint"] = ConnectClusterConfig(
            rest_url="https://CONNECT.EXAMPLE.TEST:8443/api"
        )
    return StreamtProject(
        project=ProjectInfo(name=name),
        runtime=RuntimeConfig(
            kafka=KafkaConfig(bootstrap_servers="broker:9092"),
            connect=ConnectConfig(default=default, clusters=clusters),
        ),
        deployment_state=_postgres() if postgres else {"backend": "local"},
    )


def _removal(
    *,
    owner: str = "archive_orders",
    name: str = "archive-orders-sink",
    cluster: str = "primary",
) -> dict[str, object]:
    return {"logicalOwner": owner, "name": name, "cluster": cluster}


def _desired(
    *,
    owner: str = "current_orders",
    name: str = "current-orders-sink",
) -> dict[str, object]:
    return ConnectorArtifact(
        name=name,
        connector_class="com.example.Sink",
        topics=["orders.v1"],
        cluster="primary",
        config={"tasks.max": 1},
        ownership=ArtifactOwnership(
            project="payments",
            owner_type="model",
            owner_name=owner,
            mode="managed",
        ),
    ).to_dict()


def _manifest(
    *removals: object,
    desired: tuple[object, ...] = (),
    project: str = "payments",
) -> Manifest:
    return Manifest(
        version="1.0.0",
        project_name=project,
        artifacts=cast(
            dict[str, list[dict[str, object]]],
            {
                "connectors": list(desired),
                "connector_removals": list(removals),
            },
        ),
    )


def _record(
    *,
    name: str = "archive-orders-sink",
    ownership: str = "managed",
    binding: ConnectClusterBinding | None = None,
) -> ManagedResourceRecord:
    selected = binding or ConnectClusterBinding.from_endpoint("primary", _ENDPOINT)
    return ManagedResourceRecord(
        physical_name=name,
        ownership=ownership,  # type: ignore[arg-type]
        artifact_checksum=_CHECKSUM,
        backend=selected.backend_identity,
    )


def _state(
    resources: dict[str, ManagedResourceRecord] | None = None,
) -> LocalState:
    return LocalState(
        project="payments",
        environment="prod",
        resources=resources or {},
    )


def _resolve(
    manifest: Manifest,
    *,
    project: StreamtProject | None = None,
    state: LocalState | None = None,
) -> ConnectorPlanningTargets:
    return resolve_connector_planning_targets(
        manifest,
        project or _project(),
        environment="prod",
        prior_state=state if state is not None else _state(),
        require_authoritative_state=True,
    )


def test_compiled_removal_parser_accepts_only_exact_artifact_shape() -> None:
    parsed = parse_compiled_connector_removal_artifact(_removal())
    assert parsed.to_dict() == _removal()

    for malformed in (
        ["not-an-object"],
        {"logicalOwner": "owner", "name": "connector"},
        {**_removal(), "endpoint": "https://secret.invalid"},
        {**_removal(), "logicalOwner": "bad/owner"},
    ):
        with pytest.raises(ValueError):
            parse_compiled_connector_removal_artifact(malformed)


@pytest.mark.parametrize("raw_connectors", [None, 1, {"name": "not-a-list"}])
def test_desired_connector_collection_must_be_exact_list(raw_connectors: object) -> None:
    manifest = _manifest(_removal())
    manifest.artifacts["connectors"] = raw_connectors  # type: ignore[assignment]
    with pytest.raises(ConnectorRemovalPreflightError, match="exact list"):
        _resolve(manifest)


def test_resolves_immutable_targets_in_declaration_order_and_defers_checksum() -> None:
    first_id = resource_id("payments", "prod", "connector", "archive_orders")
    prior = _record()
    targets = _resolve(
        _manifest(
            _removal(),
            _removal(owner="archive_customers", name="archive-customers-sink"),
            desired=(_desired(),),
        ),
        state=_state({first_id: prior}),
    )

    assert type(targets) is ConnectorPlanningTargets
    assert [removal.logical_owner for removal in targets.removals] == [
        "archive_orders",
        "archive_customers",
    ]
    assert all(type(removal) is ResolvedConnectorRemoval for removal in targets.removals)
    assert targets.removals[0].prior_record is prior
    assert targets.removals[1].prior_record is None
    assert targets.desired_connectors[0].cluster == "primary"
    assert targets.binding.endpoint_fingerprint.startswith("sha256:")


def test_requires_exact_postgres_config_independent_of_environment_policy() -> None:
    with pytest.raises(RemoteStateRequiredError, match="PostgreSQL"):
        require_connector_removal_postgres_state([_removal()], {"backend": "local"})

    require_connector_removal_postgres_state([], {"backend": "local"})


def test_missing_connect_default_is_runtime_required() -> None:
    with pytest.raises(ConnectorRemovalRuntimeRequiredError):
        _resolve(_manifest(_removal()), project=_project(default=None))


def test_configured_unknown_default_is_invalid_cluster_reference() -> None:
    with pytest.raises(ConnectorRemovalClusterReferenceError):
        _resolve(_manifest(_removal()), project=_project(default="missing"))


@pytest.mark.parametrize("cluster", ["missing", "same-endpoint"])
def test_unknown_or_nondefault_removal_alias_is_typed(cluster: str) -> None:
    with pytest.raises(ConnectorRemovalClusterReferenceError):
        _resolve(
            _manifest(_removal(cluster=cluster)),
            project=_project(include_alias=True),
        )


def test_backend_identity_parser_round_trips_without_endpoint_text() -> None:
    binding = ConnectClusterBinding.from_endpoint("primary", _ENDPOINT)
    parsed = ConnectClusterBinding.from_backend_identity(binding.backend_identity)
    assert parsed == binding
    assert "connect.example.test" not in binding.backend_identity

    with pytest.raises(ConnectClusterBindingError, match="backend identity") as error:
        ConnectClusterBinding.from_backend_identity(
            "https://user:backend-secret@connect.example.test"
        )
    assert "backend-secret" not in str(error.value)


@pytest.mark.parametrize("collision", ["logical", "provider"])
def test_desired_and_removal_collisions_fail_provider_free(collision: str) -> None:
    desired = _desired(
        owner="archive_orders" if collision == "logical" else "other_owner",
        name="different-sink" if collision == "logical" else "archive-orders-sink",
    )
    with pytest.raises(ConnectorRemovalPreflightError, match="collide"):
        _resolve(_manifest(_removal(), desired=(desired,)))


@pytest.mark.parametrize(
    ("second", "message"),
    [
        (_removal(name="other-sink"), "canonical resource identity"),
        (_removal(owner="other_owner"), "provider locator"),
    ],
    ids=["logical", "provider"],
)
def test_synthetic_removal_collisions_bypassing_dsl_fail_provider_free(
    second: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(ConnectorRemovalPreflightError, match=message):
        _resolve(_manifest(_removal(), second))


def test_alias_independent_prior_provider_collision_rejects_different_alias() -> None:
    old_alias_binding = ConnectClusterBinding.from_endpoint(
        "retired-default",
        "https://CONNECT.EXAMPLE.TEST:8443/api",
    )
    conflicting_id = resource_id("payments", "prod", "connector", "other_owner")
    assert (
        old_alias_binding.backend_identity
        != ConnectClusterBinding.from_endpoint("primary", _ENDPOINT).backend_identity
    )

    with pytest.raises(ConnectorRemovalPreflightError, match="provider claim"):
        _resolve(
            _manifest(_removal()),
            state=_state({conflicting_id: _record(binding=old_alias_binding)}),
        )


@pytest.mark.parametrize(
    "record",
    [
        _record(ownership="adopted"),
        ManagedResourceRecord("archive-orders-sink", "managed", _CHECKSUM, "kafka-connect"),
        ManagedResourceRecord("archive-orders-sink", "managed", _CHECKSUM, "future-connect"),
        _record(name="different-sink"),
        _record(
            binding=ConnectClusterBinding.from_endpoint(
                "primary", "https://other-connect.example.test"
            )
        ),
        _record(binding=ConnectClusterBinding.from_endpoint("retired-default", _ENDPOINT)),
    ],
    ids=["adopted", "legacy", "other-legacy", "wrong-name", "wrong-endpoint", "wrong-alias"],
)
def test_present_prior_record_requires_exact_managed_canonical_identity(
    record: ManagedResourceRecord,
) -> None:
    connector_id = resource_id("payments", "prod", "connector", "archive_orders")
    with pytest.raises(ConnectorRemovalPreflightError):
        _resolve(_manifest(_removal()), state=_state({connector_id: record}))


def test_duplicate_alias_independent_prior_claims_fail() -> None:
    target_id = resource_id("payments", "prod", "connector", "archive_orders")
    other_id = resource_id("payments", "prod", "connector", "other_owner")
    old_alias = ConnectClusterBinding.from_endpoint("old", _ENDPOINT)
    with pytest.raises(ConnectorRemovalPreflightError, match="provider claim"):
        _resolve(
            _manifest(_removal()),
            state=_state(
                {
                    target_id: _record(),
                    other_id: _record(binding=old_alias),
                }
            ),
        )


@pytest.mark.parametrize(
    "backend",
    [
        "kafka-connect",
        "future-connect",
        "kafka-connect:v0:primary:sha256:" + "2" * 64,
    ],
)
def test_ambiguous_noncanonical_backend_name_claim_fails(backend: str) -> None:
    other_id = resource_id("payments", "prod", "connector", "other_owner")
    ambiguous = ManagedResourceRecord(
        "archive-orders-sink",
        "managed",
        _CHECKSUM,
        backend,
    )
    with pytest.raises(ConnectorRemovalPreflightError, match="legacy provider claim"):
        _resolve(_manifest(_removal()), state=_state({other_id: ambiguous}))


@pytest.mark.parametrize(
    "backend",
    [
        "kafka-connect",
        "future-connect",
        "kafka-connect:v0:primary:sha256:" + "2" * 64,
    ],
)
def test_unrelated_noncanonical_backend_name_claim_is_ignored(backend: str) -> None:
    other_id = resource_id("payments", "prod", "connector", "other_owner")
    unrelated = ManagedResourceRecord(
        "unrelated-sink",
        "managed",
        _CHECKSUM,
        backend,
    )
    assert (
        _resolve(
            _manifest(_removal()),
            state=_state({other_id: unrelated}),
        )
        .removals[0]
        .prior_record
        is None
    )


def test_rejects_cross_project_state_and_manifest_identity() -> None:
    with pytest.raises(ConnectorRemovalPreflightError, match="project"):
        _resolve(_manifest(_removal(), project="other"))

    with pytest.raises(ConnectorRemovalPreflightError, match="project environment"):
        resolve_connector_planning_targets(
            _manifest(_removal()),
            _project(),
            environment="prod",
            prior_state=LocalState(project="payments", environment="other"),
            require_authoritative_state=True,
        )


def test_rejects_resource_identity_over_durable_action_boundary() -> None:
    long_project = "p" * 480
    with pytest.raises(ConnectorRemovalPreflightError, match="durable action"):
        resolve_connector_planning_targets(
            _manifest(_removal(), project=long_project),
            _project(name=long_project),
            environment="prod",
            prior_state=LocalState(project=long_project, environment="prod"),
            require_authoritative_state=True,
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("logical_owner", "owner\x00"),
        ("logical_owner", "o" * 129),
        ("connector_name", "connector\ud800"),
        ("connector_name", "c" * 257),
    ],
)
def test_resolved_target_defensively_revalidates_declaration_bounds(
    field: str,
    value: str,
) -> None:
    binding = ConnectClusterBinding.from_endpoint("primary", _ENDPOINT)
    values = {
        "resource_id": resource_id("payments", "prod", "connector", "owner"),
        "logical_owner": "owner",
        "connector_name": "connector",
        "binding": binding,
    }
    values[field] = value
    with pytest.raises(ConnectorRemovalPreflightError):
        ResolvedConnectorRemoval(**values)  # type: ignore[arg-type]


def test_preflight_errors_do_not_expose_runtime_endpoint() -> None:
    secret_endpoint = "https://connect-secret.example.test/private"
    direct_id = resource_id("payments", "prod", "connector", "archive_orders")
    with pytest.raises(ConnectorRemovalPreflightError) as error:
        _resolve(
            _manifest(_removal()),
            project=_project(endpoint=secret_endpoint),
            state=_state({direct_id: _record()}),
        )
    assert secret_endpoint not in str(error.value)
    assert "connect-secret.example.test" not in str(error.value)


def test_library_planner_fails_closed_after_preflight_before_provider_reads() -> None:
    providers = {
        "schema_registry_deployer": MagicMock(),
        "kafka_deployer": MagicMock(),
        "flink_deployer": MagicMock(),
        "connect_deployer": MagicMock(),
        "gateway_deployer": MagicMock(),
    }
    planner = DeploymentPlanner(
        _manifest(_removal()),
        **providers,
        project=_project(),
        prior_state=_state(),
        project_name="payments",
        environment="prod",
    )

    with pytest.raises(
        ConnectorRemovalPreflightError,
        match=f"^{CONNECTOR_REMOVAL_PLANNING_UNAVAILABLE_MESSAGE}$",
    ):
        planner.plan()

    for provider in providers.values():
        assert provider.mock_calls == []
