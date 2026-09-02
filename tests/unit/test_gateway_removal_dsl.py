"""Strict DSL and pure compilation for explicit Gateway rule removals."""

from __future__ import annotations

import copy
import json
from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest
import yaml
from jsonschema import Draft202012Validator

from streamt.compiler import Compiler
from streamt.compiler.manifest import (
    ArtifactOwnership,
    GatewayRuleArtifact,
    GatewayRuleRemovalArtifact,
    Manifest,
)
from streamt.compiler.model_resolution import CompileError
from streamt.core.models import StreamtProject
from streamt.core.parser import ParseError, ProjectParser
from streamt.deployer.plan_file import manifest_checksum
from streamt.deployer.state import artifact_checksum


def _removal(
    *,
    logical_owner: str = "orders_view",
    rule_name: str = "orders_rule",
    alias_name: str = "orders.public",
) -> dict[str, object]:
    return {
        "logical_owner": logical_owner,
        "prior_artifact": {
            "name": rule_name,
            "virtualTopic": alias_name,
            "physicalTopic": "raw.orders",
            "interceptors": [
                {
                    "type": "filter",
                    "config": {"where": "region = 'us'"},
                },
                {
                    "type": "mask",
                    "config": {
                        "field": "customer.email",
                        "method": "MASK_ALL",
                        "forRoles": ["support"],
                    },
                },
            ],
        },
    }


def _config(removals: object = ()) -> dict[str, object]:
    config: dict[str, object] = {
        "apiVersion": "streamt.dev/v1alpha1",
        "project": {"name": "payments", "version": "1.0.0"},
        "runtime": {"kafka": {"bootstrap_servers": "broker.invalid:9092"}},
    }
    if removals != ():
        config["lifecycle"] = {"gateway_rule_removals": removals}
    return config


def _parse(tmp_path: Path, config: dict[str, object]):
    (tmp_path / "stream_project.yml").write_text(
        yaml.safe_dump(config, sort_keys=False),
        encoding="utf-8",
    )
    return ProjectParser(tmp_path).parse()


def test_removal_compiles_separately_with_injected_managed_ownership_and_checksum(
    tmp_path: Path,
) -> None:
    project = _parse(tmp_path, _config([_removal()]))

    manifest = Compiler(project).compile(dry_run=True)

    assert manifest.artifacts["gateway_rules"] == []
    assert manifest.models == []
    assert manifest.dag.get("nodes", []) == []
    assert manifest.artifacts["gateway_rule_removals"] == [
        {
            "logicalOwner": "orders_view",
            "priorArtifact": {
                "name": "orders_rule",
                "virtualTopic": "orders.public",
                "physicalTopic": "raw.orders",
                "interceptors": [
                    {
                        "type": "filter",
                        "config": {"where": "region = 'us'"},
                    },
                    {
                        "type": "mask",
                        "config": {
                            "field": "customer.email",
                            "method": "MASK_ALL",
                            "forRoles": ["support"],
                        },
                    },
                ],
                "ownership": {
                    "mode": "managed",
                    "project": "payments",
                    "type": "model",
                    "name": "orders_view",
                },
            },
        }
    ]
    expected_prior = GatewayRuleArtifact(
        name="orders_rule",
        virtual_topic="orders.public",
        physical_topic="raw.orders",
        interceptors=manifest.artifacts["gateway_rule_removals"][0][
            "priorArtifact"
        ]["interceptors"],
        ownership=ArtifactOwnership(
            project="payments",
            owner_type="model",
            owner_name="orders_view",
            mode="managed",
        ),
    )
    compiled_prior = manifest.artifacts["gateway_rule_removals"][0]["priorArtifact"]
    assert artifact_checksum(compiled_prior) == artifact_checksum(
        expected_prior.to_dict()
    )
    assert json.loads(manifest.to_json())["artifacts"][
        "gateway_rule_removals"
    ] == manifest.artifacts["gateway_rule_removals"]


def test_empty_lifecycle_is_additive_and_changes_no_desired_collection(
    tmp_path: Path,
) -> None:
    project = _parse(tmp_path, _config())
    manifest = Compiler(project).compile(dry_run=True)

    assert project.lifecycle.gateway_rule_removals == []
    assert manifest.artifacts["gateway_rules"] == []
    assert "gateway_rule_removals" not in manifest.artifacts


@pytest.mark.parametrize(
    ("where", "mask"),
    [
        ("region = 'us'", None),
        (
            None,
            {
                "column": "customer.email",
                "method": "redact",
                "for_roles": ["support"],
            },
        ),
        (
            None,
            {
                "column": "customer.email",
                "method": "redact",
            },
        ),
    ],
    ids=["filter-only", "mask-explicit-roles", "mask-omitted-roles"],
)
def test_tombstone_prior_exactly_matches_normally_compiled_gateway_artifact(
    tmp_path: Path,
    where: str | None,
    mask: dict[str, object] | None,
) -> None:
    model: dict[str, object] = {
        "name": "orders_view",
        "materialized": "virtual_topic",
        "gateway": {"virtual_topic": {"name": "orders.public"}},
        "sql": 'SELECT * FROM {{ source("orders") }}',
    }
    if where is not None:
        model["sql"] = f"{model['sql']} WHERE {where}"
    if mask is not None:
        model["security"] = {"policies": [{"mask": mask}]}
    desired_config = _config()
    desired_config["sources"] = [{"name": "orders", "topic": "raw.orders"}]
    desired_config["models"] = [model]

    desired_manifest = Compiler(_parse(tmp_path, desired_config)).compile(dry_run=True)
    desired_prior = desired_manifest.artifacts["gateway_rules"][0]
    declared_prior = {
        key: copy.deepcopy(desired_prior[key])
        for key in ("name", "virtualTopic", "physicalTopic", "interceptors")
    }
    removal = {
        "logical_owner": "orders_view",
        "prior_artifact": declared_prior,
    }

    manifest = Compiler(_parse(tmp_path, _config([removal]))).compile(dry_run=True)
    compiled_prior = manifest.artifacts["gateway_rule_removals"][0]["priorArtifact"]

    assert compiled_prior == desired_prior
    assert artifact_checksum(compiled_prior) == artifact_checksum(desired_prior)


def test_removal_artifact_is_immutable_and_returns_defensive_prior_copies() -> None:
    removal = GatewayRuleRemovalArtifact(
        logical_owner="orders_view",
        prior_artifact=GatewayRuleArtifact(
            name="orders_rule",
            virtual_topic="orders.public",
            physical_topic="raw.orders",
            interceptors=[
                {"type": "filter", "config": {"where": "region = 'us'"}},
                {
                    "type": "mask",
                    "config": {
                        "field": "customer.email",
                        "method": "MASK_ALL",
                        "forRoles": ["support"],
                    },
                },
            ],
            ownership=ArtifactOwnership(
                project="payments",
                owner_type="model",
                owner_name="orders_view",
                mode="managed",
            ),
        ),
    )

    with pytest.raises(FrozenInstanceError):
        removal.logical_owner = "changed"  # type: ignore[misc]

    first = removal.prior_artifact
    first.name = "changed"
    first_filter_config = first.interceptors[0]["config"]
    assert isinstance(first_filter_config, dict)
    first_filter_config["where"] = "changed"
    first_mask_config = first.interceptors[1]["config"]
    assert isinstance(first_mask_config, dict)
    first_roles = first_mask_config["forRoles"]
    assert isinstance(first_roles, list)
    first_roles.append("changed")

    assert removal.prior_artifact.name == "orders_rule"
    assert removal.prior_artifact.interceptors[0]["config"] == {
        "where": "region = 'us'"
    }
    assert removal.prior_artifact.interceptors[1]["config"] == {
        "field": "customer.email",
        "method": "MASK_ALL",
        "forRoles": ["support"],
    }

    serialized = removal.to_dict()
    serialized_prior = serialized["priorArtifact"]
    assert isinstance(serialized_prior, dict)
    serialized_interceptors = serialized_prior["interceptors"]
    assert isinstance(serialized_interceptors, list)
    serialized_interceptors.clear()
    assert len(removal.to_dict()["priorArtifact"]["interceptors"]) == 2


def test_removal_content_participates_in_manifest_checksum(tmp_path: Path) -> None:
    first = Compiler(_parse(tmp_path, _config([_removal()]))).compile(dry_run=True)
    changed = _removal()
    changed_prior = changed["prior_artifact"]
    assert isinstance(changed_prior, dict)
    changed_prior["physicalTopic"] = "raw.orders.v2"
    second = Compiler(_parse(tmp_path, _config([changed]))).compile(dry_run=True)

    assert manifest_checksum(first) != manifest_checksum(second)


@pytest.mark.parametrize(
    "lifecycle",
    [
        {"future": []},
        {"gateway_rule_removals": None},
        {"gateway_rule_removals": [{**_removal(), "backend": "gateway"}]},
        {
            "gateway_rule_removals": [
                {
                    "logical_owner": "orders_view",
                    "prior_artifact": {
                        "name": "orders_rule",
                        "virtualTopic": "orders.public",
                        "physicalTopic": "raw.orders",
                    },
                }
            ]
        },
        {
            "gateway_rule_removals": [
                {
                    "logical_owner": "orders_view",
                    "prior_artifact": {
                        **_removal()["prior_artifact"],
                        "interceptors": [
                            {
                                "type": "filter",
                                "config": {
                                    "field": "email",
                                    "method": "MASK_ALL",
                                },
                            }
                        ],
                    },
                }
            ]
        },
        {
            "gateway_rule_removals": [
                {
                    "logical_owner": "orders_view",
                    "prior_artifact": {
                        **_removal()["prior_artifact"],
                        "interceptors": [{"type": "mask", "config": {"where": "true"}}],
                    },
                }
            ]
        },
        {
            "gateway_rule_removals": [
                {
                    "logical_owner": "orders_view",
                    "prior_artifact": {
                        **_removal()["prior_artifact"],
                        "ownership": {"mode": "managed"},
                    },
                }
            ]
        },
        {
            "gateway_rule_removals": [
                {
                    "logical_owner": "orders_view",
                    "prior_artifact": {
                        **_removal()["prior_artifact"],
                        "endpoint": "https://gateway.invalid",
                    },
                }
            ]
        },
        {
            "gateway_rule_removals": [
                {
                    "logical_owner": "orders/view",
                    "prior_artifact": _removal()["prior_artifact"],
                }
            ]
        },
        {
            "gateway_rule_removals": [
                {
                    "logical_owner": "orders_view",
                    "prior_artifact": {
                        **_removal()["prior_artifact"],
                        "virtualTopic": "orders/public",
                    },
                }
            ]
        },
        {
            "gateway_rule_removals": [
                {
                    "logical_owner": "orders_view",
                    "prior_artifact": {
                        **_removal()["prior_artifact"],
                        "interceptors": None,
                    },
                }
            ]
        },
        {
            "gateway_rule_removals": [
                {
                    "logical_owner": "orders_view",
                    "prior_artifact": {
                        **_removal()["prior_artifact"],
                        "interceptors": {
                            "type": "filter",
                            "config": {"where": "true"},
                        },
                    },
                }
            ]
        },
        {
            "gateway_rule_removals": [
                {
                    "logical_owner": "orders_view",
                    "prior_artifact": {
                        **_removal()["prior_artifact"],
                        "interceptors": [
                            {"type": "encrypt", "config": {"field": "email"}}
                        ],
                    },
                }
            ]
        },
        {
            "gateway_rule_removals": [
                {
                    "logical_owner": "orders_view",
                    "prior_artifact": {
                        **_removal()["prior_artifact"],
                        "interceptors": [
                            {
                                "type": "filter",
                                "config": {"where": "true", "future": True},
                            }
                        ],
                    },
                }
            ]
        },
        {
            "gateway_rule_removals": [
                {
                    "logical_owner": "orders_view",
                    "prior_artifact": {
                        **_removal()["prior_artifact"],
                        "interceptors": [
                            {
                                "type": "mask",
                                "config": {
                                    "field": "email",
                                    "method": "MASK_ALL",
                                    "forRoles": None,
                                },
                            }
                        ],
                    },
                }
            ]
        },
    ],
)
def test_lifecycle_removal_dsl_rejects_non_exact_shapes(
    tmp_path: Path,
    lifecycle: dict[str, object],
) -> None:
    config = _config()
    config["lifecycle"] = lifecycle

    with pytest.raises(ParseError, match="Invalid lifecycle"):
        _parse(tmp_path, config)


def test_lifecycle_null_is_rejected_and_omission_uses_empty_config(
    tmp_path: Path,
) -> None:
    null_config = _config()
    null_config["lifecycle"] = None

    with pytest.raises(ParseError, match="field 'lifecycle' must be a mapping"):
        _parse(tmp_path, null_config)

    project = _parse(tmp_path, _config())
    assert project.lifecycle.gateway_rule_removals == []


def test_generated_schema_discriminates_removal_interceptors_and_rejects_null() -> None:
    schema = StreamtProject.model_json_schema(
        mode="serialization",
        ref_template="#/$defs/{model}",
    )
    Draft202012Validator.check_schema(schema)
    validator = Draft202012Validator(schema)

    prior_artifact_schema = schema["$defs"]["GatewayRulePriorArtifact"]
    interceptor_schema = prior_artifact_schema["properties"]["interceptors"]["items"]
    assert interceptor_schema["discriminator"] == {
        "mapping": {
            "filter": "#/$defs/GatewayRulePriorFilterInterceptor",
            "mask": "#/$defs/GatewayRulePriorMaskInterceptor",
        },
        "propertyName": "type",
    }
    assert "oneOf" in interceptor_schema
    mask_schema = schema["$defs"]["GatewayRulePriorMaskConfig"]
    assert mask_schema["properties"]["forRoles"]["uniqueItems"] is True
    assert schema["properties"]["lifecycle"] == {"$ref": "#/$defs/LifecycleConfig"}

    assert validator.is_valid(_config())

    null_lifecycle = _config()
    null_lifecycle["lifecycle"] = None
    assert not validator.is_valid(null_lifecycle)

    mismatched = _config([_removal()])
    mismatched_lifecycle = mismatched["lifecycle"]
    assert isinstance(mismatched_lifecycle, dict)
    mismatched_removals = mismatched_lifecycle["gateway_rule_removals"]
    assert isinstance(mismatched_removals, list)
    mismatched_prior = mismatched_removals[0]["prior_artifact"]
    assert isinstance(mismatched_prior, dict)
    mismatched_prior["interceptors"] = [
        {
            "type": "filter",
            "config": {"field": "email", "method": "MASK_ALL"},
        }
    ]
    assert not validator.is_valid(mismatched)

    duplicate_roles = _config([_removal()])
    duplicate_lifecycle = duplicate_roles["lifecycle"]
    assert isinstance(duplicate_lifecycle, dict)
    duplicate_removals = duplicate_lifecycle["gateway_rule_removals"]
    assert isinstance(duplicate_removals, list)
    duplicate_prior = duplicate_removals[0]["prior_artifact"]
    assert isinstance(duplicate_prior, dict)
    duplicate_interceptors = duplicate_prior["interceptors"]
    assert isinstance(duplicate_interceptors, list)
    duplicate_mask = duplicate_interceptors[1]
    assert isinstance(duplicate_mask, dict)
    duplicate_config = duplicate_mask["config"]
    assert isinstance(duplicate_config, dict)
    duplicate_config["forRoles"] = ["support", "support"]
    assert not validator.is_valid(duplicate_roles)


@pytest.mark.parametrize("duplicate", ["owner", "rule", "alias"])
def test_lifecycle_removal_dsl_rejects_duplicate_identities(
    tmp_path: Path,
    duplicate: str,
) -> None:
    first = _removal()
    second = _removal(
        logical_owner="customers_view",
        rule_name="customers_rule",
        alias_name="customers.public",
    )
    if duplicate == "owner":
        second["logical_owner"] = first["logical_owner"]
    else:
        first_prior = first["prior_artifact"]
        second_prior = second["prior_artifact"]
        assert isinstance(first_prior, dict)
        assert isinstance(second_prior, dict)
        key = "name" if duplicate == "rule" else "virtualTopic"
        second_prior[key] = first_prior[key]

    with pytest.raises(ParseError, match="duplicate Gateway rule removal"):
        _parse(tmp_path, _config([first, second]))


def test_compiler_defensively_rejects_duplicates_added_after_parsing(
    tmp_path: Path,
) -> None:
    project = _parse(tmp_path, _config([_removal()]))
    assert project.lifecycle is not None
    project.lifecycle.gateway_rule_removals.append(
        copy.deepcopy(project.lifecycle.gateway_rule_removals[0])
    )

    with pytest.raises(CompileError, match="Duplicate Gateway rule removal"):
        Compiler(project).compile(dry_run=True)


def test_manifest_load_preserves_separate_removal_collection(tmp_path: Path) -> None:
    manifest = Compiler(_parse(tmp_path, _config([_removal()]))).compile(dry_run=True)
    path = tmp_path / "manifest.json"
    manifest.save(path)

    loaded = Manifest.load(path)

    assert loaded.artifacts["gateway_rule_removals"] == manifest.artifacts[
        "gateway_rule_removals"
    ]
