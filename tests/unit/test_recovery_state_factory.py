"""Recovery-only deployment-state factory boundaries."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

import streamt.deployer.postgres_state_backend as postgres_backend
from streamt.core.deployment_state import (
    DeploymentStateConfig,
    local_deployment_state_config,
    validate_deployment_state_config,
)
from streamt.deployer.state_backend import (
    LOCAL_STATE_NAMESPACE,
    LocalDeploymentStateBackend,
    StateAddress,
    StateBackendUnavailableError,
    make_deployment_state_service,
    make_recovery_state_service,
)


def _postgres_config(
    *,
    writer_dsn_env: str | None = "STREAMT_RECOVERY_WRITER_DSN",
    writer_role_env: str | None = "STREAMT_RECOVERY_WRITER_ROLE",
) -> DeploymentStateConfig:
    postgres: dict[str, object] = {
        "dsn_env": "STREAMT_ADMIN_DSN",
        "schema": "state_catalog",
    }
    if writer_dsn_env is not None:
        postgres["writer_dsn_env"] = writer_dsn_env
    if writer_role_env is not None:
        postgres["writer_role_env"] = writer_role_env
    return validate_deployment_state_config(
        {
            "backend": "postgres",
            "namespace": "production-platform",
            "lock_timeout_seconds": 47,
            "postgres": postgres,
        }
    )


def test_local_recovery_factory_preserves_the_existing_local_authority(
    tmp_path: Path,
) -> None:
    service = make_recovery_state_service(
        tmp_path,
        project="payments",
        environment="prod",
        config=local_deployment_state_config(),
    )

    assert isinstance(service.backend, LocalDeploymentStateBackend)
    assert service.store.backend == "local"
    assert service.address == StateAddress(
        namespace=LOCAL_STATE_NAMESPACE,
        project="payments",
        environment="prod",
    )


def test_postgres_recovery_factory_uses_only_the_writer_binding_and_exact_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    writer_dsn = "postgresql://writer:writer-secret@writer.internal/state"
    monkeypatch.setenv("STREAMT_RECOVERY_WRITER_DSN", writer_dsn)
    monkeypatch.setenv(
        "STREAMT_ADMIN_DSN",
        "postgresql://owner:owner-secret@admin.internal/state",
    )
    monkeypatch.setenv("STREAMT_RECOVERY_WRITER_ROLE", "configured-role-is-not-read")
    constructed: list[dict[str, object]] = []

    class Backend:
        def __init__(self, **kwargs: object) -> None:
            constructed.append(kwargs)

    monkeypatch.setattr(postgres_backend, "PrivatePostgresStateReadBackend", Backend)

    service = make_recovery_state_service(
        tmp_path,
        project="payments",
        environment="prod-us",
        config=_postgres_config(),
    )

    assert constructed == [
        {
            "dsn": writer_dsn,
            "schema": "state_catalog",
            "lock_timeout_seconds": 47,
        }
    ]
    assert isinstance(service.backend, Backend)
    assert service.address == StateAddress(
        namespace="production-platform",
        project="payments",
        environment="prod-us",
    )


def test_postgres_recovery_factory_requires_a_writer_credential_binding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    admin_dsn = "postgresql://owner:owner-secret@admin.internal/state"
    monkeypatch.setenv("STREAMT_ADMIN_DSN", admin_dsn)
    monkeypatch.setenv("STREAMT_RECOVERY_WRITER_ROLE", "state_writer")

    with pytest.raises(StateBackendUnavailableError) as raised:
        make_recovery_state_service(
            tmp_path,
            project="payments",
            environment="prod",
            config=_postgres_config(writer_dsn_env=None),
        )

    rendered = f"{raised.value!s} {raised.value!r}"
    assert rendered == (
        "PostgreSQL recovery state credentials are not configured "
        "StateBackendUnavailableError('PostgreSQL recovery state credentials are not "
        "configured')"
    )
    assert "STREAMT_ADMIN_DSN" not in rendered
    assert admin_dsn not in rendered


@pytest.mark.parametrize("writer_dsn", [None, "", "   "])
def test_postgres_recovery_factory_rejects_unavailable_writer_credentials(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    writer_dsn: str | None,
) -> None:
    secret = "postgresql://owner:owner-secret@admin.internal/state"
    monkeypatch.setenv("STREAMT_ADMIN_DSN", secret)
    if writer_dsn is None:
        monkeypatch.delenv("STREAMT_RECOVERY_WRITER_DSN", raising=False)
    else:
        monkeypatch.setenv("STREAMT_RECOVERY_WRITER_DSN", writer_dsn)

    with pytest.raises(StateBackendUnavailableError) as raised:
        make_recovery_state_service(
            tmp_path,
            project="payments",
            environment="prod",
            config=_postgres_config(),
        )

    rendered = f"{raised.value!s} {raised.value!r}"
    assert "STREAMT_RECOVERY_WRITER_DSN" not in rendered
    assert "STREAMT_ADMIN_DSN" not in rendered
    assert secret not in rendered
    assert "postgresql://" not in rendered


def test_postgres_recovery_factory_does_not_fallback_to_admin_credentials(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "STREAMT_ADMIN_DSN",
        "postgresql://owner:owner-secret@admin.internal/state",
    )
    monkeypatch.delenv("STREAMT_RECOVERY_WRITER_DSN", raising=False)

    def fail_if_constructed(**_kwargs: object) -> object:
        raise AssertionError("backend construction would imply credential fallback")

    monkeypatch.setattr(
        postgres_backend,
        "PrivatePostgresStateReadBackend",
        fail_if_constructed,
    )

    with pytest.raises(
        StateBackendUnavailableError,
        match=r"^PostgreSQL recovery state credentials are unavailable$",
    ):
        make_recovery_state_service(
            tmp_path,
            project="payments",
            environment="prod",
            config=_postgres_config(),
        )


def test_postgres_recovery_factory_keeps_driver_lazy_and_secrets_unserialized(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    writer_dsn = "postgresql://writer:writer-secret@writer.internal/state"
    monkeypatch.setenv("STREAMT_RECOVERY_WRITER_DSN", writer_dsn)
    calls = 0

    def fail_if_loaded() -> object:
        nonlocal calls
        calls += 1
        raise AssertionError("factory construction must not load psycopg")

    monkeypatch.setattr(postgres_backend, "_load_psycopg", fail_if_loaded)
    config = _postgres_config()

    service = make_recovery_state_service(
        tmp_path,
        project="payments",
        environment="prod",
        config=config,
    )

    assert isinstance(service.backend, postgres_backend.PrivatePostgresStateReadBackend)
    assert calls == 0
    rendered = f"{service!r} {json.dumps(config.model_dump(mode='json'))}"
    assert writer_dsn not in rendered
    assert "writer-secret" not in rendered


@pytest.mark.parametrize("dsn", [None, "", "   ", "postgresql://u:p@db/state"])
def test_ordinary_postgres_factory_remains_disabled(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    dsn: str | None,
) -> None:
    if dsn is None:
        monkeypatch.delenv("STREAMT_ADMIN_DSN", raising=False)
    else:
        monkeypatch.setenv("STREAMT_ADMIN_DSN", dsn)
    monkeypatch.setenv(
        "STREAMT_RECOVERY_WRITER_DSN",
        "postgresql://writer:writer-secret@writer.internal/state",
    )

    with pytest.raises(StateBackendUnavailableError) as raised:
        make_deployment_state_service(
            tmp_path,
            project="payments",
            environment="prod",
            config=_postgres_config(),
        )

    expected = (
        "PostgreSQL deployment state credentials are unavailable"
        if dsn is None or not dsn.strip()
        else "PostgreSQL deployment state is unavailable in this release"
    )
    assert str(raised.value) == expected
