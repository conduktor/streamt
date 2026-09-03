"""Strict DSL and pure compilation for explicit Connector removals."""

from __future__ import annotations

import copy
from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest
import yaml
from jsonschema import Draft202012Validator
from pydantic import ValidationError

from streamt.compiler import Compiler
from streamt.compiler.manifest import ConnectorRemovalArtifact, Manifest
from streamt.compiler.model_resolution import CompileError
from streamt.core.errors import ErrorCode
from streamt.core.models import ConnectorRemovalDeclaration, StreamtProject
from streamt.core.parser import ParseError, ProjectParser
from streamt.deployer.plan_file import manifest_checksum


def _removal(
    *,
    logical_owner: str = "archive_orders",
    name: str = "archive-orders-sink",
    cluster: str = "primary-connect",
) -> dict[str, str]:
    return {
        "logical_owner": logical_owner,
        "name": name,
        "cluster": cluster,
    }


def _config(removals: object = ()) -> dict[str, object]:
    config: dict[str, object] = {
        "apiVersion": "streamt.dev/v1alpha1",
        "project": {"name": "payments", "version": "1.0.0"},
        "runtime": {
            "kafka": {"bootstrap_servers": "broker.invalid:9092"},
            "connect": {
                "default": "primary-connect",
                "clusters": {"primary-connect": {"rest_url": "https://connect.invalid"}},
            },
        },
    }
    if removals != ():
        config["lifecycle"] = {"connector_removals": removals}
    return config


def _parse(tmp_path: Path, config: dict[str, object]) -> StreamtProject:
    (tmp_path / "stream_project.yml").write_text(
        yaml.safe_dump(config, sort_keys=False),
        encoding="utf-8",
    )
    return ProjectParser(tmp_path).parse()


def _with_desired_sink(config: dict[str, object]) -> dict[str, object]:
    config["sources"] = [{"name": "orders", "topic": "raw.orders"}]
    config["models"] = [
        {
            "name": "archive_orders",
            "from": [{"source": "orders"}],
            "materialized": "sink",
            "sink": {"connector": "jdbc-sink", "config": {}},
        }
    ]
    return config


def test_connector_removal_error_and_warning_codes_are_reserved() -> None:
    assert ErrorCode.CONNECTOR_REMOVAL_INVALID == "E427_CONNECTOR_REMOVAL_INVALID"
    assert ErrorCode.CONNECTOR_REMOVAL_DRIFT == "E428_CONNECTOR_REMOVAL_DRIFT"
    assert (
        ErrorCode.CONNECTOR_REMOVAL_DESTRUCTIVE
        == "W119_CONNECTOR_REMOVAL_DESTRUCTIVE"
    )


def test_connector_removals_compile_separately_in_declaration_order(
    tmp_path: Path,
) -> None:
    project = _parse(
        tmp_path,
        _config(
            [
                _removal(),
                _removal(
                    logical_owner="archive_customers",
                    name="archive-customers-sink",
                ),
            ]
        ),
    )
    compiler = Compiler(project)

    first = compiler.compile(dry_run=True)
    second = compiler.compile(dry_run=True)

    expected = [
        {
            "logicalOwner": "archive_orders",
            "name": "archive-orders-sink",
            "cluster": "primary-connect",
        },
        {
            "logicalOwner": "archive_customers",
            "name": "archive-customers-sink",
            "cluster": "primary-connect",
        },
    ]
    assert first.artifacts["connector_removals"] == expected
    assert second.artifacts["connector_removals"] == expected
    assert first.artifacts["connectors"] == []
    assert first.models == []
    assert first.dag.get("nodes", []) == []


def test_empty_connector_removals_preserve_prior_manifest_shape_and_checksum(
    tmp_path: Path,
) -> None:
    omitted = Compiler(_parse(tmp_path, _config())).compile(dry_run=True)
    explicit = Compiler(_parse(tmp_path, _config([]))).compile(dry_run=True)

    assert "connector_removals" not in omitted.artifacts
    assert "connector_removals" not in explicit.artifacts
    assert manifest_checksum(omitted) == manifest_checksum(explicit)


def test_connector_removal_content_participates_in_manifest_checksum(
    tmp_path: Path,
) -> None:
    first = Compiler(_parse(tmp_path, _config([_removal()]))).compile(dry_run=True)
    second = Compiler(_parse(tmp_path, _config([_removal(name="archive-orders-v2")]))).compile(
        dry_run=True
    )

    assert manifest_checksum(first) != manifest_checksum(second)


def test_connector_removal_artifact_is_immutable_and_exact() -> None:
    removal = ConnectorRemovalArtifact(
        logical_owner="archive_orders",
        connector_name="archive-orders-sink",
        cluster_alias="primary-connect",
    )

    assert removal.to_dict() == {
        "logicalOwner": "archive_orders",
        "name": "archive-orders-sink",
        "cluster": "primary-connect",
    }
    with pytest.raises(FrozenInstanceError):
        removal.connector_name = "changed"  # type: ignore[misc]


@pytest.mark.parametrize(
    "overrides",
    [
        {"logical_owner": "owner/child"},
        {"logical_owner": "x" * 129},
        {"connector_name": "\x00"},
        {"connector_name": "x" * 257},
        {"cluster_alias": " \t"},
        {"cluster_alias": "\ud800"},
        {"cluster_alias": "x" * 129},
    ],
)
def test_connector_removal_artifact_defensively_rejects_invalid_text(
    overrides: dict[str, str],
) -> None:
    values = {
        "logical_owner": "archive_orders",
        "connector_name": "archive-orders-sink",
        "cluster_alias": "primary-connect",
        **overrides,
    }

    with pytest.raises(ValueError, match="Connector removal"):
        ConnectorRemovalArtifact(**values)


@pytest.mark.parametrize(
    ("field", "maximum"),
    [("logical_owner", 128), ("name", 256), ("cluster", 128)],
)
def test_connector_removal_text_bounds_are_exact(field: str, maximum: int) -> None:
    valid = _removal()
    valid[field] = "x" * maximum
    parsed = ConnectorRemovalDeclaration.model_validate(valid)
    assert getattr(parsed, field) == "x" * maximum

    invalid = _removal()
    invalid[field] = "x" * (maximum + 1)
    with pytest.raises(ValidationError):
        ConnectorRemovalDeclaration.model_validate(invalid)


@pytest.mark.parametrize("field", ["logical_owner", "name", "cluster"])
@pytest.mark.parametrize("value", ["", " \t ", "valid\x00invalid", "valid\ud800invalid"])
def test_connector_removal_rejects_empty_control_and_surrogate_text(
    field: str,
    value: str,
) -> None:
    removal = _removal()
    removal[field] = value

    with pytest.raises(ValidationError):
        ConnectorRemovalDeclaration.model_validate(removal)


def test_connector_removal_logical_owner_rejects_slash() -> None:
    with pytest.raises(ValidationError):
        ConnectorRemovalDeclaration.model_validate(_removal(logical_owner="archive/orders"))


@pytest.mark.parametrize(
    "field",
    [
        "config",
        "connector_class",
        "topics",
        "ownership",
        "backend",
        "endpoint",
        "fingerprint",
    ],
)
def test_connector_removal_rejects_unknown_and_secret_bearing_fields(
    tmp_path: Path,
    field: str,
) -> None:
    removal: dict[str, object] = dict(_removal())
    removal[field] = {"password": "must-not-enter-manifest"}

    with pytest.raises(ParseError, match="Invalid lifecycle") as error:
        _parse(tmp_path, _config([removal]))

    assert "must-not-enter-manifest" not in str(error.value)


@pytest.mark.parametrize(
    "removals",
    [
        None,
        _removal(),
        [{"name": "connector", "cluster": "primary-connect"}],
        [{"logical_owner": "owner", "cluster": "primary-connect"}],
        [{"logical_owner": "owner", "name": "connector"}],
        [{"logical_owner": None, "name": "connector", "cluster": "primary-connect"}],
        [{"logical_owner": "owner", "name": None, "cluster": "primary-connect"}],
        [{"logical_owner": "owner", "name": "connector", "cluster": None}],
    ],
)
def test_connector_removal_rejects_non_exact_shapes(
    tmp_path: Path,
    removals: object,
) -> None:
    with pytest.raises(ParseError, match="Invalid lifecycle"):
        _parse(tmp_path, _config(removals))


def test_connector_removal_collection_is_capped_at_256(tmp_path: Path) -> None:
    valid = [
        _removal(logical_owner=f"owner-{index}", name=f"connector-{index}") for index in range(256)
    ]
    assert len(_parse(tmp_path, _config(valid)).lifecycle.connector_removals) == 256

    with pytest.raises(ParseError, match="Invalid lifecycle"):
        _parse(
            tmp_path,
            _config(
                [
                    *valid,
                    _removal(logical_owner="owner-256", name="connector-256"),
                ]
            ),
        )


@pytest.mark.parametrize("duplicate", ["logical_owner", "provider"])
def test_connector_removal_rejects_duplicate_identities(
    tmp_path: Path,
    duplicate: str,
) -> None:
    first = _removal()
    second = _removal(
        logical_owner="archive_customers",
        name="archive-customers-sink",
    )
    if duplicate == "logical_owner":
        second["logical_owner"] = first["logical_owner"]
    else:
        second["name"] = first["name"]
        second["cluster"] = first["cluster"]

    with pytest.raises(ParseError, match="duplicate Connector removal"):
        _parse(tmp_path, _config([first, second]))


def test_compiler_defensively_rejects_mutated_duplicate_removal(
    tmp_path: Path,
) -> None:
    project = _parse(tmp_path, _config([_removal()]))
    project.lifecycle.connector_removals.append(
        copy.deepcopy(project.lifecycle.connector_removals[0])
    )
    compiler = Compiler(project)

    with pytest.raises(CompileError, match="Duplicate Connector removal"):
        compiler.compile(dry_run=True)

    assert compiler.connector_removals == []


def test_compiler_rejects_desired_and_removal_logical_identity_conflict(
    tmp_path: Path,
) -> None:
    config = _with_desired_sink(_config([_removal(name="different-provider-name")]))

    with pytest.raises(CompileError, match="same logical resource"):
        Compiler(_parse(tmp_path, config)).compile(dry_run=True)


def test_compiler_rejects_desired_and_removal_provider_target_conflict(
    tmp_path: Path,
) -> None:
    config = _with_desired_sink(
        _config(
            [
                _removal(
                    logical_owner="retired_owner",
                    name="archive_orders",
                )
            ]
        )
    )

    with pytest.raises(CompileError, match="same provider target"):
        Compiler(_parse(tmp_path, config)).compile(dry_run=True)


def test_removed_desired_model_without_tombstone_creates_no_delete_artifact(
    tmp_path: Path,
) -> None:
    manifest = Compiler(_parse(tmp_path, _config())).compile(dry_run=True)

    assert manifest.artifacts["connectors"] == []
    assert "connector_removals" not in manifest.artifacts


def test_manifest_round_trip_preserves_connector_removal_collection(
    tmp_path: Path,
) -> None:
    manifest = Compiler(_parse(tmp_path, _config([_removal()]))).compile(dry_run=True)
    path = tmp_path / "manifest.json"
    manifest.save(path)

    loaded = Manifest.load(path)

    assert loaded.artifacts["connector_removals"] == manifest.artifacts["connector_removals"]


def test_generated_schema_exposes_strict_bounded_connector_removals() -> None:
    schema = StreamtProject.model_json_schema(
        mode="serialization",
        ref_template="#/$defs/{model}",
    )
    Draft202012Validator.check_schema(schema)

    declaration = schema["$defs"]["ConnectorRemovalDeclaration"]
    assert declaration["additionalProperties"] is False
    assert declaration["required"] == ["logical_owner", "name", "cluster"]
    assert declaration["properties"]["logical_owner"] == {
        "maxLength": 128,
        "minLength": 1,
        "title": "Logical Owner",
        "type": "string",
    }
    assert declaration["properties"]["name"]["maxLength"] == 256
    assert declaration["properties"]["cluster"]["maxLength"] == 128
    removals = schema["$defs"]["LifecycleConfig"]["properties"]["connector_removals"]
    assert removals["maxItems"] == 256
    assert removals["items"] == {"$ref": "#/$defs/ConnectorRemovalDeclaration"}
