"""Focused real Conduktor Gateway gate for the strict managed observer."""

from __future__ import annotations

import json
import os
import uuid
from dataclasses import FrozenInstanceError

import pytest
import requests
from confluent_kafka.admin import AdminClient, NewTopic
from requests.auth import HTTPBasicAuth

from streamt.deployer.gateway import (
    GatewayDeployer,
    ManagedGatewayRuleObservation,
)

_ENABLE_ENV = "STREAMT_TEST_GATEWAY_STRICT_OBSERVER"
_ADMIN_URL_ENV = "STREAMT_TEST_GATEWAY_ADMIN_URL"
_ADMIN_USER_ENV = "STREAMT_TEST_GATEWAY_ADMIN_USER"
_ADMIN_PASSWORD_ENV = "STREAMT_TEST_GATEWAY_ADMIN_PASSWORD"
_KAFKA_BOOTSTRAP_ENV = "STREAMT_TEST_KAFKA_BOOTSTRAP_SERVERS"


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
