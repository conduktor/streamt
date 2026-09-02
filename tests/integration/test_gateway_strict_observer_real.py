"""Focused real Conduktor Gateway gate for the strict managed observer."""

from __future__ import annotations

import json
import os
import sys
import uuid
from dataclasses import FrozenInstanceError

import pytest
import requests
from confluent_kafka.admin import AdminClient, NewTopic
from requests.auth import HTTPBasicAuth

from streamt.compiler.manifest import GatewayRuleArtifact
from streamt.deployer.gateway import (
    GatewayDeployer,
    ManagedGatewayRuleObservation,
    build_desired_gateway_rule,
)

_ENABLE_ENV = "STREAMT_TEST_GATEWAY_STRICT_OBSERVER"
_ADMIN_URL_ENV = "STREAMT_TEST_GATEWAY_ADMIN_URL"
_ADMIN_USER_ENV = "STREAMT_TEST_GATEWAY_ADMIN_USER"
_ADMIN_PASSWORD_ENV = "STREAMT_TEST_GATEWAY_ADMIN_PASSWORD"
_KAFKA_BOOTSTRAP_ENV = "STREAMT_TEST_KAFKA_BOOTSTRAP_SERVERS"


def _delete_for_cleanup(
    session: requests.Session,
    endpoint: str,
    payload: dict[str, object],
) -> str | None:
    """Attempt one exact cleanup request and return a sanitized failure."""
    try:
        response = session.delete(
            endpoint,
            json=payload,
            timeout=10,
            allow_redirects=False,
            stream=True,
        )
    except requests.RequestException as error:
        return f"cleanup request failed with {type(error).__name__}"
    try:
        if response.status_code in {204, 404}:
            return None
        return f"cleanup request returned HTTP {response.status_code}"
    finally:
        response.close()


@pytest.mark.integration
@pytest.mark.gateway
def test_gateway_315_strict_snapshot_observes_default_scope_exactly_once() -> None:
    """Validate Gateway 3.15's real default-scope aggregate wire shape."""
    if os.environ.get(_ENABLE_ENV) != "1":
        pytest.skip(f"set {_ENABLE_ENV}=1 to run the real Gateway observer gate")

    admin_url = os.environ.get(_ADMIN_URL_ENV, "http://127.0.0.1:8888").rstrip("/")
    username = os.environ.get(_ADMIN_USER_ENV, "admin")
    password = os.environ.get(_ADMIN_PASSWORD_ENV, "conduktor")
    kafka_bootstrap = os.environ.get(_KAFKA_BOOTSTRAP_ENV, "127.0.0.1:9092")
    suffix = uuid.uuid4().hex[:12]
    logical_name = f"test-strict-rule-{suffix}"
    alias_name = f"test-strict-alias-{suffix}"
    absent_alias_name = f"test-strict-absent-{suffix}"
    physical_name = f"test-strict-physical-{suffix}"
    alias_endpoint = f"{admin_url}/gateway/v2/alias-topic"

    kafka = AdminClient({"bootstrap.servers": kafka_bootstrap})
    setup = requests.Session()
    setup.auth = HTTPBasicAuth(username, password)
    alias_created = False
    topic_created = False
    try:
        kafka.create_topics(
            [NewTopic(physical_name, num_partitions=1, replication_factor=1)]
        )[physical_name].result(timeout=15)
        topic_created = True
        created = setup.put(
            alias_endpoint,
            json={
                "kind": "AliasTopic",
                "apiVersion": "gateway/v2",
                "metadata": {"name": alias_name},
                "spec": {"physicalName": physical_name},
            },
            timeout=10,
        )
        assert created.status_code in {200, 201}, created.text
        alias_created = True

        requests_seen: list[tuple[str, str, dict[str, object]]] = []
        observed_bodies: dict[str, bytes] = {}
        with GatewayDeployer(
            admin_url=admin_url,
            username=username,
            password=password,
        ) as deployer:
            request = deployer._session.request

            def record_request(
                method: str,
                url: str,
                **kwargs: object,
            ) -> requests.Response:
                requests_seen.append((method, url, kwargs))
                response = request(method, url, **kwargs)
                if url == alias_endpoint:
                    observed_bodies["aliases"] = response.content
                return response

            deployer._session.request = record_request  # type: ignore[method-assign]
            snapshot = deployer.observe_managed_gateway_snapshot()

        assert [(method, url) for method, url, _kwargs in requests_seen] == [
            ("GET", alias_endpoint),
            ("GET", f"{admin_url}/gateway/v2/interceptor"),
        ]
        assert all(
            kwargs
            == {
                "timeout": 10,
                "allow_redirects": False,
                "stream": True,
            }
            for _method, _url, kwargs in requests_seen
        )

        raw_aliases = json.loads(observed_bodies["aliases"])
        matching_raw_aliases = [
            value
            for value in raw_aliases
            if value.get("metadata", {}).get("name") == alias_name
        ]
        assert matching_raw_aliases == [
            {
                "kind": "AliasTopic",
                "apiVersion": "gateway/v2",
                "metadata": {"name": alias_name},
                "spec": {
                    "physicalName": physical_name,
                    "physicalCluster": "main",
                },
            }
        ]

        assert isinstance(snapshot.aliases, tuple)
        assert isinstance(snapshot.interceptors, tuple)
        normalized_aliases = tuple(
            alias for alias in snapshot.aliases if alias.name == alias_name
        )
        assert len(normalized_aliases) == 1
        normalized_alias = normalized_aliases[0]
        assert normalized_alias.scope == "passthrough"
        assert normalized_alias.physical_name == physical_name
        assert normalized_alias.physical_cluster == "main"

        expected_present = ManagedGatewayRuleObservation(
            binding=snapshot.binding,
            logical_name=logical_name,
            alias_name=alias_name,
            exists=True,
            physical_name=physical_name,
            physical_cluster="main",
        )
        present = snapshot.rule(logical_name, alias_name)
        absent = snapshot.rule(logical_name, absent_alias_name)
        assert present == expected_present
        assert present.fingerprint == expected_present.fingerprint
        assert absent == ManagedGatewayRuleObservation(
            binding=snapshot.binding,
            logical_name=logical_name,
            alias_name=absent_alias_name,
            exists=False,
        )
        assert len(requests_seen) == 2

        with pytest.raises(FrozenInstanceError):
            snapshot.aliases = ()  # type: ignore[misc]
        with pytest.raises(FrozenInstanceError):
            normalized_alias.physical_name = "mutated"  # type: ignore[misc]
        with pytest.raises(FrozenInstanceError):
            present.exists = False  # type: ignore[misc]
    finally:
        if alias_created:
            deleted = setup.delete(
                alias_endpoint,
                json={"name": alias_name},
                timeout=10,
            )
            assert deleted.status_code in {200, 204, 404}, deleted.text
        if topic_created:
            kafka.delete_topics([physical_name])[physical_name].result(timeout=15)
        setup.close()


@pytest.mark.integration
@pytest.mark.gateway
@pytest.mark.parametrize("with_interceptor", [False, True], ids=["alias-only", "aggregate"])
def test_gateway_315_exact_managed_delete_removes_only_target_aggregate(
    with_interceptor: bool,
) -> None:
    """Delete one exact aggregate without residue or unrelated-resource loss."""
    if os.environ.get(_ENABLE_ENV) != "1":
        pytest.skip(f"set {_ENABLE_ENV}=1 to run the real Gateway deletion gate")

    admin_url = os.environ.get(_ADMIN_URL_ENV, "http://127.0.0.1:8888").rstrip("/")
    username = os.environ.get(_ADMIN_USER_ENV, "admin")
    password = os.environ.get(_ADMIN_PASSWORD_ENV, "conduktor")
    kafka_bootstrap = os.environ.get(_KAFKA_BOOTSTRAP_ENV, "127.0.0.1:9092")
    suffix = uuid.uuid4().hex[:12]
    rule_name = f"test-delete-rule-{suffix}"
    alias_name = f"test-delete-alias-{suffix}"
    physical_name = f"test-delete-physical-{suffix}"
    unrelated_rule_name = f"test-delete-other-rule-{suffix}"
    unrelated_alias_name = f"test-delete-other-alias-{suffix}"
    unrelated_physical_name = f"test-delete-other-physical-{suffix}"
    alias_endpoint = f"{admin_url}/gateway/v2/alias-topic"
    interceptor_endpoint = f"{admin_url}/gateway/v2/interceptor"
    topic_names = (physical_name, unrelated_physical_name)

    target_interceptors = (
        [{"type": "filter", "config": {"where": "amount > 0"}}] if with_interceptor else []
    )
    target_artifact = GatewayRuleArtifact(
        name=rule_name,
        virtual_topic=alias_name,
        physical_topic=physical_name,
        interceptors=target_interceptors,
    )
    unrelated_artifact = GatewayRuleArtifact(
        name=unrelated_rule_name,
        virtual_topic=unrelated_alias_name,
        physical_topic=unrelated_physical_name,
        interceptors=[
            {"type": "filter", "config": {"where": "amount >= 0"}},
        ],
    )

    kafka = AdminClient({"bootstrap.servers": kafka_bootstrap})
    cleanup = requests.Session()
    cleanup.auth = HTTPBasicAuth(username, password)
    created_topics: list[str] = []
    cleanup_interceptors: list[tuple[str, dict[str, object]]] = []
    try:
        futures = kafka.create_topics(
            [NewTopic(name, num_partitions=1, replication_factor=1) for name in topic_names]
        )
        for name in topic_names:
            futures[name].result(timeout=15)
            created_topics.append(name)

        with GatewayDeployer(
            admin_url=admin_url,
            username=username,
            password=password,
        ) as deployer:
            target_desired = build_desired_gateway_rule(
                target_artifact,
                deployer.cluster_binding,
            )
            unrelated_desired = build_desired_gateway_rule(
                unrelated_artifact,
                deployer.cluster_binding,
            )
            cleanup_interceptors = [
                (
                    interceptor.name,
                    dict(interceptor.scope),
                )
                for desired in (target_desired, unrelated_desired)
                for interceptor in desired.interceptors
            ]

            initial = deployer.observe_managed_gateway_snapshot()
            assert (
                deployer.apply_managed_gateway_rule(
                    initial.rule(rule_name, alias_name),
                    target_desired,
                )
                == "created"
            )
            assert (
                deployer.apply_managed_gateway_rule(
                    initial.rule(unrelated_rule_name, unrelated_alias_name),
                    unrelated_desired,
                )
                == "created"
            )

            before = deployer.observe_managed_gateway_snapshot()
            target_current = before.rule(rule_name, alias_name)
            unrelated_current = before.rule(
                unrelated_rule_name,
                unrelated_alias_name,
            )
            assert target_current == target_desired
            assert unrelated_current == unrelated_desired

            requests_seen: list[tuple[str, str, dict[str, object]]] = []
            request = deployer._session.request

            def record_request(
                method: str,
                url: str,
                **kwargs: object,
            ) -> requests.Response:
                requests_seen.append((method, url, kwargs))
                return request(method, url, **kwargs)

            deployer._session.request = record_request  # type: ignore[method-assign]
            assert deployer.delete_managed_gateway_rule(target_current) == "deleted"

            expected_deletes = [
                (
                    "DELETE",
                    f"{interceptor_endpoint}/{interceptor.name}",
                    {
                        "json": dict(interceptor.scope),
                        "timeout": 10,
                        "allow_redirects": False,
                        "stream": True,
                    },
                )
                for interceptor in target_current.interceptors
            ]
            expected_deletes.append(
                (
                    "DELETE",
                    alias_endpoint,
                    {
                        "json": {
                            "name": alias_name,
                            "vCluster": target_current.binding.virtual_cluster,
                        },
                        "timeout": 10,
                        "allow_redirects": False,
                        "stream": True,
                    },
                )
            )
            assert requests_seen == expected_deletes

            requests_seen.clear()
            after = deployer.observe_managed_gateway_snapshot()
            assert [(method, url) for method, url, _kwargs in requests_seen] == [
                ("GET", alias_endpoint),
                ("GET", interceptor_endpoint),
            ]
            assert all(
                kwargs
                == {
                    "timeout": 10,
                    "allow_redirects": False,
                    "stream": True,
                }
                for _method, _url, kwargs in requests_seen
            )

            assert after.rule(rule_name, alias_name) == ManagedGatewayRuleObservation(
                binding=target_current.binding,
                logical_name=rule_name,
                alias_name=alias_name,
                exists=False,
            )
            assert after.rule(unrelated_rule_name, unrelated_alias_name) == unrelated_current
            assert after.aliases == tuple(
                alias
                for alias in before.aliases
                if not (
                    alias.scope == target_current.binding.virtual_cluster
                    and alias.name == alias_name
                )
            )
            target_interceptor_names = {
                interceptor.name for interceptor in target_current.interceptors
            }
            target_interceptor_locators = {
                (interceptor.scope, interceptor.name) for interceptor in target_current.interceptors
            }
            assert after.interceptors == tuple(
                interceptor
                for interceptor in before.interceptors
                if (interceptor.scope, interceptor.name) not in target_interceptor_locators
            )
            assert not target_interceptor_names.intersection(
                interceptor.name for interceptor in after.interceptors
            )
    finally:
        active_failure = sys.exc_info()[0] is not None
        cleanup_failures: list[str] = []
        for interceptor_name, scope in reversed(cleanup_interceptors):
            failure = _delete_for_cleanup(
                cleanup,
                f"{interceptor_endpoint}/{interceptor_name}",
                scope,
            )
            if failure is not None:
                cleanup_failures.append(f"interceptor {interceptor_name}: {failure}")
        for cleanup_alias in (alias_name, unrelated_alias_name):
            failure = _delete_for_cleanup(
                cleanup,
                alias_endpoint,
                {"name": cleanup_alias, "vCluster": "passthrough"},
            )
            if failure is not None:
                cleanup_failures.append(f"alias {cleanup_alias}: {failure}")
        if created_topics:
            try:
                deleted_topics = kafka.delete_topics(created_topics)
            except Exception as error:
                cleanup_failures.append(
                    f"topic cleanup failed with {type(error).__name__}"
                )
            else:
                for topic_name in created_topics:
                    try:
                        deleted_topics[topic_name].result(timeout=15)
                    except Exception as error:
                        cleanup_failures.append(
                            f"topic {topic_name}: cleanup failed with {type(error).__name__}"
                        )
        cleanup.close()
        if cleanup_failures and not active_failure:
            pytest.fail("; ".join(cleanup_failures))
