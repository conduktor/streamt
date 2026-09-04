"""Pure secret-neutral manifest checksum contract tests."""

from __future__ import annotations

import copy
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from streamt.compiler.manifest import Manifest
from streamt.core import manifest_identity as manifest_identity_module
from streamt.core.manifest_identity import ManifestIdentityError, manifest_checksum

_FIXTURE_DIR = Path(__file__).parents[1] / "fixtures" / "strimzi" / "1.2.0"
_FAILURE_MESSAGE = "Manifest content could not be checksummed"


class _ManifestStub:
    def __init__(self, content: object) -> None:
        self.content = content

    def to_dict(self) -> object:
        return self.content


class _ExplodingManifest:
    def to_dict(self) -> dict[str, object]:
        raise RuntimeError("private-exception-sentinel")


class _PrivateUnsupportedValue:
    pass


class _CompiledAtImpostor:
    def __hash__(self) -> int:
        return hash("compiled_at")

    def __eq__(self, other: object) -> bool:
        return other == "compiled_at"


def _checksum_json(value: object) -> str:
    canonical = json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return f"sha256:{hashlib.sha256(canonical).hexdigest()}"


def test_deployer_plan_file_preserves_checksum_and_error_compatibility() -> None:
    from streamt.deployer import plan_file

    baseline = Manifest.load(_FIXTURE_DIR / "manifest.json")
    assert plan_file.manifest_checksum(baseline) == manifest_checksum(baseline) == (
        "sha256:c332e02e89962298f8bb4a1ac29b964f97e0c38b2192e9986d9b41ba21814b39"
    )
    with pytest.raises(plan_file.PlanFileError) as raised:
        plan_file.manifest_checksum(_ManifestStub({"value": object()}))  # type: ignore[arg-type]
    assert str(raised.value) == "Plan manifest content could not be checksummed"
    assert raised.value.__cause__ is None


def test_exact_canonical_projection_and_checksum_bytes_are_frozen() -> None:
    raw = {
        "version": "1.0",
        "project": "caf\N{LATIN SMALL LETTER E WITH ACUTE}",
        "compiled_at": "2026-09-04T01:02:03Z",
        "artifacts": {
            "topics": [
                {
                    "config": {
                        "credential": "private-credential-sentinel",
                        "retention.ms": 60_000,
                    },
                    "name": "orders.v1",
                }
            ]
        },
    }
    expected_projection = {
        "version": "1.0",
        "project": "caf\N{LATIN SMALL LETTER E WITH ACUTE}",
        "artifacts": {
            "topics": [
                {
                    "config": {
                        "credential": "<redacted>",
                        "retention.ms": 60_000,
                    },
                    "name": "orders.v1",
                }
            ]
        },
    }

    assert manifest_checksum(_ManifestStub(raw)) == _checksum_json(expected_projection)
    assert manifest_checksum(_ManifestStub(raw)) == (
        "sha256:ddb7efd57d66612fce7b5e7f41270e55df69d130c2fe4ff2af25228f450365d3"
    )


@pytest.mark.parametrize(
    "value",
    [
        None,
        True,
        False,
        0,
        -123456789,
        1.25,
        -0.0,
        "quoted \" text \\ and caf\N{LATIN SMALL LETTER E WITH ACUTE}",
        [None, True, 17, "value"],
        {"z": [3, 2, 1], "a": {"nested": False}},
    ],
)
def test_local_canonical_serializer_matches_json_dumps_for_ordinary_values(
    value: object,
) -> None:
    assert manifest_identity_module._canonical_json(value) == json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def test_arbitrarily_large_signed_integers_use_exact_decimal_without_global_limit() -> None:
    positive = 10**5000
    negative = -(10**5000)
    expected_json = (
        '{"negative":-1'
        + ("0" * 5000)
        + ',"positive":1'
        + ("0" * 5000)
        + "}"
    )
    expected = f"sha256:{hashlib.sha256(expected_json.encode()).hexdigest()}"

    assert manifest_identity_module._canonical_json(
        {"positive": positive, "negative": negative}
    ) == expected_json
    assert manifest_checksum(
        _ManifestStub({"positive": positive, "negative": negative})
    ) == expected


def test_compilation_time_is_excluded_only_at_the_manifest_root() -> None:
    first = _ManifestStub(
        {
            "compiled_at": "2026-01-01T00:00:00Z",
            "nested": {"compiled_at": "represented-value"},
        }
    )
    later = _ManifestStub(
        {
            "compiled_at": "2099-12-31T23:59:59Z",
            "nested": {"compiled_at": "represented-value"},
        }
    )
    nested_change = _ManifestStub(
        {
            "compiled_at": "2026-01-01T00:00:00Z",
            "nested": {"compiled_at": "changed-value"},
        }
    )

    assert manifest_checksum(first) == manifest_checksum(later)
    assert manifest_checksum(first) != manifest_checksum(nested_change)


@pytest.mark.parametrize(
    "key",
    [
        "password",
        "db.passwd",
        "client-secret",
        "access_token",
        "apikey",
        "api_key",
        "api-key",
        "Authorization",
        "user.credential",
        "user.credentials",
        "basic.auth.user.info",
        "sasl_jaas_config",
        "prefix.sasl-jaas-config.suffix",
    ],
)
def test_exact_sensitive_key_policy_uses_the_fixed_redaction_marker(key: str) -> None:
    first = _ManifestStub({"config": {key: "private-value-alpha"}})
    second = _ManifestStub({"config": {key: "private-value-beta"}})
    redacted = _ManifestStub({"config": {key: "<redacted>"}})

    assert manifest_checksum(first) == manifest_checksum(second)
    assert manifest_checksum(first) == manifest_checksum(redacted)


@pytest.mark.parametrize(
    "key",
    ["passwordless", "mytokenizer", "api.keys", "credentialed", "sasl.jaas.option"],
)
def test_sensitive_key_policy_does_not_overmatch_nonsecret_keys(key: str) -> None:
    first = _ManifestStub({"config": {key: "represented-alpha"}})
    second = _ManifestStub({"config": {key: "represented-beta"}})
    assert manifest_checksum(first) != manifest_checksum(second)


def test_represented_nonsecret_content_changes_the_checksum() -> None:
    baseline = Manifest.load(_FIXTURE_DIR / "manifest.json")
    changed = Manifest.load(_FIXTURE_DIR / "manifest-nonsecret-variant.json")
    secret_only = Manifest.load(_FIXTURE_DIR / "manifest-secret-variant.json")

    assert manifest_checksum(baseline) == manifest_checksum(secret_only)
    assert manifest_checksum(baseline) != manifest_checksum(changed)


def test_checksum_does_not_mutate_caller_owned_manifest_content() -> None:
    raw: dict[str, object] = {
        "compiled_at": "2026-09-04T01:02:03Z",
        "config": {
            "credential": "private-value",
            "ordinary": ["first", {"nested": True}],
        },
        "set_value": {"b", "a"},
    }
    before = copy.deepcopy(raw)
    original_config = raw["config"]

    manifest_checksum(_ManifestStub(raw))

    assert raw == before
    assert raw["config"] is original_config
    assert "compiled_at" in raw


@pytest.mark.parametrize(
    "manifest",
    [
        _ManifestStub([]),
        _ManifestStub({object(): "private-non-string-key"}),
        _ManifestStub({_CompiledAtImpostor(): "private-root-key-impostor"}),
        _ManifestStub({"value": float("nan")}),
        _ManifestStub({"value": "private-surrogate-\ud800"}),
        _ManifestStub({"value": _PrivateUnsupportedValue()}),
        _ExplodingManifest(),
    ],
)
def test_malformed_or_hostile_content_fails_with_one_secret_neutral_error(
    manifest: object,
) -> None:
    with pytest.raises(ManifestIdentityError) as raised:
        manifest_checksum(manifest)  # type: ignore[arg-type]

    assert str(raised.value) == _FAILURE_MESSAGE
    assert repr(raised.value) == f"ManifestIdentityError({_FAILURE_MESSAGE!r})"
    assert raised.value.__cause__ is None
    surface = f"{raised.value!s} {raised.value!r}"
    assert "private" not in surface
    assert "Unsupported" not in surface
    assert "RuntimeError" not in surface


def test_importing_core_helper_does_not_import_runtime_or_deployment_layers(
    tmp_path: Path,
) -> None:
    script = """
import sys
import streamt.core.manifest_identity

forbidden = (
    "streamt.core.deployment_state",
    "streamt.core.runtime",
    "streamt.deployer",
    "streamt.planner",
    "streamt.provider",
    "streamt.providers",
    "streamt.state",
)
loaded = sorted(
    name
    for name in sys.modules
    if any(name == prefix or name.startswith(prefix + ".") for prefix in forbidden)
)
if loaded:
    raise SystemExit("forbidden runtime layer imported")

import streamt.core as core
from streamt.core.dag import DAGBuilder
from streamt.core.models import DataTest, Exposure, Model, Project, Source, StreamtProject
from streamt.core.parser import ProjectParser
from streamt.core.validator import ProjectValidator

expected_exports = {
    "DAGBuilder": DAGBuilder,
    "DataTest": DataTest,
    "Exposure": Exposure,
    "Model": Model,
    "Project": Project,
    "ProjectParser": ProjectParser,
    "ProjectValidator": ProjectValidator,
    "Source": Source,
    "StreamtProject": StreamtProject,
}
if any(getattr(core, name) is not value for name, value in expected_exports.items()):
    raise SystemExit("legacy core export changed")
"""
    environment = dict(os.environ)
    environment.pop("PYTHONPATH", None)
    result = subprocess.run(
        [sys.executable, "-I", "-c", script],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout == ""
    assert result.stderr == ""
