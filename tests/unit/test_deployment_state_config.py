"""Strict deployment-state configuration and merge-contract tests."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from streamt.core.deployment_state import (
    LocalDeploymentStateConfig,
    PostgresDeploymentStateConfig,
    validate_deployment_state_config,
)
from streamt.core.environment import EnvironmentError, EnvironmentManager
from streamt.core.models import StreamtProject
from streamt.core.parser import ParseError, ProjectParser
from streamt.deployer.state_backend import (
    StateBackendUnavailableError,
    make_deployment_state_service,
)


def _runtime() -> dict[str, object]:
    return {"kafka": {"bootstrap_servers": "localhost:9092"}}


def _write_project(
    path: Path,
    *,
    deployment_state: object = None,
    include_state: bool = False,
) -> None:
    data: dict[str, object] = {
        "apiVersion": "streamt.dev/v1alpha1",
        "project": {"name": "state-config-test"},
        "runtime": _runtime(),
    }
    if include_state:
        data["deployment_state"] = deployment_state
    (path / "stream_project.yml").write_text(
        yaml.safe_dump(data),
        encoding="utf-8",
    )


def _postgres(**overrides: object) -> dict[str, object]:
    value: dict[str, object] = {
        "backend": "postgres",
        "namespace": "platform team",
        "postgres": {"dsn_env": "STREAMT_STATE_DSN"},
    }
    value.update(overrides)
    return value


def test_omitted_project_config_defaults_to_local(tmp_path: Path) -> None:
    _write_project(tmp_path)

    project = ProjectParser(tmp_path).parse()

    assert project.deployment_state == LocalDeploymentStateConfig(backend="local")


def test_postgres_config_is_strict_and_keeps_only_the_dsn_name(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("STREAMT_STATE_DSN", raising=False)
    _write_project(
        tmp_path,
        deployment_state=_postgres(
            postgres={
                "dsn_env": "STREAMT_STATE_DSN",
                "writer_role_env": "STREAMT_STATE_WRITER_ROLE",
            }
        ),
        include_state=True,
    )

    project = ProjectParser(tmp_path).parse()

    assert isinstance(project.deployment_state, PostgresDeploymentStateConfig)
    assert project.deployment_state.namespace == "platform team"
    assert project.deployment_state.lock_timeout_seconds == 30
    assert project.deployment_state.postgres.dsn_env == "STREAMT_STATE_DSN"
    assert (
        project.deployment_state.postgres.writer_role_env
        == "STREAMT_STATE_WRITER_ROLE"
    )
    assert project.deployment_state.postgres.schema_name == "streamt"
    assert "dsn" not in project.deployment_state.model_dump(mode="json")


def test_writer_role_environment_name_is_optional_for_v1_administration(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path, deployment_state=_postgres(), include_state=True)

    project = ProjectParser(tmp_path).parse()

    assert isinstance(project.deployment_state, PostgresDeploymentStateConfig)
    assert project.deployment_state.postgres.writer_role_env is None


@pytest.mark.parametrize(
    "value",
    [
        {},
        {"backend": "local", "namespace": "forbidden"},
        {"backend": "local", "postgres": {"dsn_env": "STATE_DSN"}},
        _postgres(extra=True),
        _postgres(namespace=""),
        _postgres(namespace="team/platform"),
        _postgres(lock_timeout_seconds=True),
        _postgres(lock_timeout_seconds="30"),
        _postgres(lock_timeout_seconds=0),
        _postgres(lock_timeout_seconds=301),
        _postgres(postgres={"dsn_env": "${STATE_DSN}"}),
        _postgres(postgres={"dsn_env": "postgresql://db"}),
        _postgres(postgres={"dsn_env": "STATE-DSN"}),
        _postgres(
            postgres={"dsn_env": "STATE_DSN", "writer_role_env": "${WRITER_ROLE}"}
        ),
        _postgres(
            postgres={"dsn_env": "STATE_DSN", "writer_role_env": "writer-role"}
        ),
        _postgres(postgres={"dsn_env": "STATE_DSN", "writer_role_env": ""}),
        _postgres(postgres={"dsn_env": "STATE_DSN", "writer_role_env": False}),
        _postgres(postgres={"dsn_env": "STATE_DSN", "schema": "bad-name"}),
        _postgres(postgres={"dsn_env": "STATE_DSN", "unknown": True}),
    ],
)
def test_invalid_or_mixed_provider_shapes_fail(value: object) -> None:
    with pytest.raises(ValidationError):
        validate_deployment_state_config(value)


def test_explicit_null_project_config_is_not_treated_as_omitted(tmp_path: Path) -> None:
    _write_project(tmp_path, deployment_state=None, include_state=True)

    with pytest.raises(ParseError, match="Invalid deployment_state"):
        ProjectParser(tmp_path).parse()


def test_environment_omission_inherits_root_config(tmp_path: Path) -> None:
    _write_project(
        tmp_path,
        deployment_state=_postgres(
            postgres={
                "dsn_env": "STREAMT_STATE_DSN",
                "writer_role_env": "STREAMT_STATE_WRITER_ROLE",
            }
        ),
        include_state=True,
    )
    environments = tmp_path / "environments"
    environments.mkdir()
    (environments / "dev.yml").write_text(
        yaml.safe_dump({"environment": {"name": "dev"}, "runtime": _runtime()}),
        encoding="utf-8",
    )

    project = ProjectParser(tmp_path, environment="dev").parse()

    assert project.deployment_state.backend == "postgres"
    assert project.deployment_state.namespace == "platform team"
    assert project.deployment_state.postgres.writer_role_env == (
        "STREAMT_STATE_WRITER_ROLE"
    )


def test_environment_provider_block_replaces_root_whole(tmp_path: Path) -> None:
    _write_project(tmp_path, deployment_state=_postgres(), include_state=True)
    environments = tmp_path / "environments"
    environments.mkdir()
    (environments / "dev.yml").write_text(
        yaml.safe_dump(
            {
                "environment": {"name": "dev"},
                "runtime": _runtime(),
                "deployment_state": {"backend": "local"},
            }
        ),
        encoding="utf-8",
    )

    project = ProjectParser(tmp_path, environment="dev").parse()

    assert project.deployment_state.backend == "local"


def test_environment_postgres_block_replaces_writer_environment_name(
    tmp_path: Path,
) -> None:
    _write_project(
        tmp_path,
        deployment_state=_postgres(
            postgres={
                "dsn_env": "BASE_STATE_DSN",
                "writer_role_env": "BASE_STATE_WRITER_ROLE",
            }
        ),
        include_state=True,
    )
    environments = tmp_path / "environments"
    environments.mkdir()
    (environments / "dev.yml").write_text(
        yaml.safe_dump(
            {
                "environment": {"name": "dev"},
                "runtime": _runtime(),
                "deployment_state": _postgres(
                    namespace="dev",
                    postgres={
                        "dsn_env": "DEV_STATE_DSN",
                        "writer_role_env": "DEV_STATE_WRITER_ROLE",
                    },
                ),
            }
        ),
        encoding="utf-8",
    )

    project = ProjectParser(tmp_path, environment="dev").parse()

    assert isinstance(project.deployment_state, PostgresDeploymentStateConfig)
    assert project.deployment_state.namespace == "dev"
    assert project.deployment_state.postgres.dsn_env == "DEV_STATE_DSN"
    assert project.deployment_state.postgres.writer_role_env == (
        "DEV_STATE_WRITER_ROLE"
    )


def test_partial_environment_provider_does_not_deep_merge(tmp_path: Path) -> None:
    _write_project(tmp_path, deployment_state=_postgres(), include_state=True)
    environments = tmp_path / "environments"
    environments.mkdir()
    (environments / "dev.yml").write_text(
        yaml.safe_dump(
            {
                "environment": {"name": "dev"},
                "runtime": _runtime(),
                "deployment_state": {
                    "backend": "postgres",
                    "namespace": "override",
                },
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(EnvironmentError, match=r"deployment_state.*postgres"):
        EnvironmentManager(tmp_path).load_environment("dev")


def test_require_remote_state_is_strict_environment_only_safety(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path)
    environments = tmp_path / "environments"
    environments.mkdir()
    (environments / "dev.yml").write_text(
        yaml.safe_dump(
            {
                "environment": {"name": "dev"},
                "runtime": _runtime(),
                "safety": {"require_remote_state": True},
            }
        ),
        encoding="utf-8",
    )

    config = EnvironmentManager(tmp_path).load_environment("dev")

    assert config.requires_remote_state is True


def test_non_boolean_require_remote_state_is_rejected(tmp_path: Path) -> None:
    _write_project(tmp_path)
    environments = tmp_path / "environments"
    environments.mkdir()
    (environments / "dev.yml").write_text(
        yaml.safe_dump(
            {
                "environment": {"name": "dev"},
                "runtime": _runtime(),
                "safety": {"require_remote_state": "true"},
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(EnvironmentError, match="must be a boolean"):
        EnvironmentManager(tmp_path).load_environment("dev")


def test_generated_project_schema_exposes_strict_tagged_provider_contract() -> None:
    schema = StreamtProject.model_json_schema(by_alias=True)
    deployment_state = schema["properties"]["deployment_state"]
    assert deployment_state["discriminator"] == {
        "mapping": {
            "local": "#/$defs/LocalDeploymentStateConfig",
            "postgres": "#/$defs/PostgresDeploymentStateConfig",
        },
        "propertyName": "backend",
    }
    local = schema["$defs"]["LocalDeploymentStateConfig"]
    assert local["additionalProperties"] is False
    assert local["required"] == ["backend"]
    postgres = schema["$defs"]["PostgresDeploymentStateConfig"]
    assert postgres["additionalProperties"] is False
    assert postgres["properties"]["namespace"] == {
        "minLength": 1,
        "pattern": "^[^/]+$",
        "title": "Namespace",
        "type": "string",
    }
    assert postgres["properties"]["lock_timeout_seconds"] == {
        "default": 30,
        "maximum": 300,
        "minimum": 1,
        "title": "Lock Timeout Seconds",
        "type": "integer",
    }
    connection = schema["$defs"]["PostgresConnectionConfig"]
    assert connection["additionalProperties"] is False
    assert connection["properties"]["dsn_env"]["pattern"] == (
        "^[A-Za-z_][A-Za-z0-9_]*$"
    )
    assert connection["properties"]["writer_role_env"] == {
        "anyOf": [
            {
                "pattern": "^[A-Za-z_][A-Za-z0-9_]*$",
                "type": "string",
            },
            {"type": "null"},
        ],
        "default": None,
        "title": "Writer Role Env",
    }
    assert connection["properties"]["schema"]["default"] == "streamt"


def test_writer_role_value_is_not_resolved_or_serialized_during_parsing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("STREAMT_STATE_WRITER_ROLE", "runtime-secret-role")
    _write_project(
        tmp_path,
        deployment_state=_postgres(
            postgres={
                "dsn_env": "STREAMT_STATE_DSN",
                "writer_role_env": "STREAMT_STATE_WRITER_ROLE",
            }
        ),
        include_state=True,
    )
    (tmp_path / ".env").write_text(
        "STREAMT_STATE_WRITER_ROLE=base-secret-role\n",
        encoding="utf-8",
    )
    environments = tmp_path / "environments"
    environments.mkdir()
    (environments / "dev.yml").write_text(
        yaml.safe_dump({"environment": {"name": "dev"}, "runtime": _runtime()}),
        encoding="utf-8",
    )
    (tmp_path / ".env.dev").write_text(
        "STREAMT_STATE_WRITER_ROLE=environment-secret-role\n",
        encoding="utf-8",
    )

    project = ProjectParser(tmp_path, environment="dev").parse()
    serialized = project.deployment_state.model_dump_json()

    assert isinstance(project.deployment_state, PostgresDeploymentStateConfig)
    assert project.deployment_state.postgres.writer_role_env == (
        "STREAMT_STATE_WRITER_ROLE"
    )
    assert "runtime-secret-role" not in serialized
    assert "base-secret-role" not in serialized
    assert "environment-secret-role" not in serialized


def test_dsn_value_uses_dotenv_precedence_only_when_factory_is_constructed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("STREAMT_STATE_DSN", raising=False)
    _write_project(tmp_path, deployment_state=_postgres(), include_state=True)
    environments = tmp_path / "environments"
    environments.mkdir()
    (environments / "dev.yml").write_text(
        yaml.safe_dump({"environment": {"name": "dev"}, "runtime": _runtime()}),
        encoding="utf-8",
    )
    (tmp_path / ".env").write_text("STREAMT_STATE_DSN=base-secret\n")
    (tmp_path / ".env.dev").write_text("STREAMT_STATE_DSN=environment-secret\n")

    project = ProjectParser(tmp_path, environment="dev").parse()

    assert project.deployment_state.postgres.dsn_env == "STREAMT_STATE_DSN"
    assert "environment-secret" not in project.deployment_state.model_dump_json()
    assert "base-secret" not in project.deployment_state.model_dump_json()
    with pytest.raises(
        StateBackendUnavailableError,
        match="unavailable in this release",
    ):
        make_deployment_state_service(
            tmp_path,
            project=project.project.name,
            environment="dev",
            config=project.deployment_state,
        )


def test_real_environment_dsn_has_precedence_over_dotenv(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("STREAMT_STATE_DSN", "real-environment-secret")
    _write_project(tmp_path, deployment_state=_postgres(), include_state=True)
    environments = tmp_path / "environments"
    environments.mkdir()
    (environments / "dev.yml").write_text(
        yaml.safe_dump({"environment": {"name": "dev"}, "runtime": _runtime()}),
        encoding="utf-8",
    )
    (tmp_path / ".env").write_text("STREAMT_STATE_DSN=base-secret\n")
    (tmp_path / ".env.dev").write_text("STREAMT_STATE_DSN=environment-secret\n")

    project = ProjectParser(tmp_path, environment="dev").parse()

    assert project.deployment_state.postgres.dsn_env == "STREAMT_STATE_DSN"
    with pytest.raises(
        StateBackendUnavailableError,
        match="unavailable in this release",
    ):
        make_deployment_state_service(
            tmp_path,
            project=project.project.name,
            environment="dev",
            config=project.deployment_state,
        )
