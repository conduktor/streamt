"""Strict DSL and pure compilation for explicit Gateway rule removals."""

from __future__ import annotations

import copy
import json
from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest
import yaml

from streamt.compiler import Compiler
from streamt.compiler.manifest import (
    ArtifactOwnership,
    GatewayRuleArtifact,
    GatewayRuleRemovalArtifact,
    Manifest,
)
from streamt.compiler.model_resolution import CompileError
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
    manifest = Compiler(_parse(tmp_path, _config())).compile(dry_run=True)

    assert manifest.artifacts["gateway_rules"] == []
    assert manifest.artifacts["gateway_rule_removals"] == []


def test_omitted_optional_mask_roles_remain_omitted_for_checksum_exactness(
    tmp_path: Path,
) -> None:
    removal = _removal()
    prior = removal["prior_artifact"]
    assert isinstance(prior, dict)
    prior["interceptors"] = [
        {
            "type": "mask",
            "config": {"field": "customer.email", "method": "MASK_ALL"},
        }
    ]

    manifest = Compiler(_parse(tmp_path, _config([removal]))).compile(dry_run=True)
    compiled_prior = manifest.artifacts["gateway_rule_removals"][0]["priorArtifact"]

    assert "forRoles" not in compiled_prior["interceptors"][0]["config"]


def test_removal_artifact_is_immutable_and_returns_defensive_prior_copies() -> None:
    removal = GatewayRuleRemovalArtifact(
        logical_owner="orders_view",
        prior_artifact=GatewayRuleArtifact(
            name="orders_rule",
            virtual_topic="orders.public",
            physical_topic="raw.orders",
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
    assert removal.prior_artifact.name == "orders_rule"


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
