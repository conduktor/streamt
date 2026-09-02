"""Pure complete-manifest resolution for managed Gateway rules."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import FrozenInstanceError

import pytest

from streamt.deployer.gateway import (
    GatewayBackendBinding,
    GatewayManifestResolutionError,
    ResolvedManagedGatewayRule,
    resolve_managed_gateway_rules,
)


def _binding() -> GatewayBackendBinding:
    return GatewayBackendBinding.from_endpoint(
        "https://gateway.resolver.example.test/secret-admin-path",
        virtual_cluster="payments-prod",
    )


def _rule(
    name: str,
    alias: str,
    owner: str,
    *,
    where: str | None = None,
) -> dict[str, object]:
    interceptors: list[dict[str, object]] = []
    if where is not None:
        interceptors.append({"type": "filter", "config": {"where": where}})
    return {
        "name": name,
        "virtualTopic": alias,
        "physicalTopic": f"{name}.physical",
        "interceptors": interceptors,
        "ownership": {
            "mode": "managed",
            "project": "payments",
            "type": "model",
            "name": owner,
        },
    }


def test_resolver_preserves_compiler_order_and_detaches_caller_inputs() -> None:
    secret = "resolver-config-secret-91c7"
    raw_rules = [
        _rule("second_rule", "second.public", "second_owner"),
        _rule(
            "first_rule",
            "first.public",
            "first_owner",
            where=f"customer_token = '{secret}'",
        ),
    ]
    original = deepcopy(raw_rules)

    resolved = resolve_managed_gateway_rules(raw_rules, _binding())

    assert isinstance(resolved, tuple)
    assert all(isinstance(rule, ResolvedManagedGatewayRule) for rule in resolved)
    assert [rule.artifact.name for rule in resolved] == ["second_rule", "first_rule"]
    assert [rule.logical_owner for rule in resolved] == ["second_owner", "first_owner"]
    assert raw_rules == original

    raw_rules[0]["name"] = "caller-mutated"
    second_interceptors = raw_rules[1]["interceptors"]
    assert isinstance(second_interceptors, list)
    second_declaration = second_interceptors[0]
    assert isinstance(second_declaration, dict)
    second_config = second_declaration["config"]
    assert isinstance(second_config, dict)
    second_config["where"] = "caller-mutated"
    assert resolved[0].artifact.name == "second_rule"
    assert resolved[1].artifact.interceptors[0]["config"] == {
        "where": f"customer_token = '{secret}'"
    }
    with pytest.raises(FrozenInstanceError):
        resolved[0].logical_owner = "mutated"  # type: ignore[misc]


def test_resolved_rule_repr_hides_artifact_config_and_gateway_endpoint() -> None:
    secret = "resolver-config-secret-d418"
    endpoint = "gateway.resolver.example.test"
    resolved = resolve_managed_gateway_rules(
        [
            _rule(
                "secret_rule",
                "secret.public",
                "secret_owner",
                where=f"customer_token = '{secret}'",
            )
        ],
        _binding(),
    )[0]

    rendered = repr(resolved)
    assert rendered == "ResolvedManagedGatewayRule(logical_owner='secret_owner')"
    assert secret not in rendered
    assert endpoint not in rendered
    assert "artifact" not in rendered
    assert "desired" not in rendered


@pytest.mark.parametrize(
    ("collision", "message"),
    [
        ("rule_name", "Gateway manifest contains a duplicate rule name"),
        ("owner", "Gateway manifest maps one logical owner to multiple rules"),
        ("alias", "Gateway manifest contains a duplicate canonical alias locator"),
        ("interceptor", "Gateway manifest contains a duplicate interceptor locator"),
    ],
)
def test_resolver_rejects_each_exact_manifest_collision(
    collision: str,
    message: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = _rule(
        "first_rule",
        "first.public",
        "first_owner",
        where="region = 'us'" if collision == "interceptor" else None,
    )
    second = _rule(
        "second_rule",
        "second.public",
        "second_owner",
        where="region = 'eu'" if collision == "interceptor" else None,
    )
    if collision == "rule_name":
        second["name"] = first["name"]
    elif collision == "owner":
        second["ownership"] = deepcopy(first["ownership"])
    elif collision == "alias":
        second["virtualTopic"] = first["virtualTopic"]
    else:
        monkeypatch.setattr(
            "streamt.deployer.gateway.generate_gateway_interceptor_name",
            lambda _logical_name, _declaration_type, _ordinal: "shared_filter_0",
        )

    with pytest.raises(GatewayManifestResolutionError, match=message):
        resolve_managed_gateway_rules([first, second], _binding())


def test_resolver_rejects_cross_rule_generated_namespace_ownership(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rules = [
        _rule("first_rule", "first.public", "first_owner", where="region = 'us'"),
        _rule("second_rule", "second.public", "second_owner", where="region = 'eu'"),
    ]
    monkeypatch.setattr(
        "streamt.deployer.gateway.classify_gateway_interceptor_name",
        lambda _logical_name, _candidate: object(),
    )

    with pytest.raises(
        GatewayManifestResolutionError,
        match="Gateway generated interceptor identity maps to multiple rules",
    ):
        resolve_managed_gateway_rules(rules, _binding())
