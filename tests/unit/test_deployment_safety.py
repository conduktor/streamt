"""Safety invariants for ownership-aware planning and selective apply."""

from __future__ import annotations

from copy import deepcopy

import pytest

from streamt.cli.commands.apply import (
    destructive_operations_allowed,
    filter_manifest_for_selection,
)
from streamt.compiler.manifest import ArtifactOwnership, Manifest
from streamt.core.environment import EnvironmentConfig, EnvironmentInfo, SafetyConfig
from streamt.deployer.state import StateFormatError, StateIdentityError


def _ownership(owner_type: str, owner_name: str, mode: str = "managed") -> dict[str, str]:
    return ArtifactOwnership(
        project="payments",
        owner_type=owner_type,
        owner_name=owner_name,
        mode=mode,
    ).to_dict()


class TestSelectiveManifestFiltering:
    def test_keeps_every_artifact_owned_by_selected_closure(self):
        manifest = Manifest(
            version="1.0",
            project_name="payments",
            models=[{"name": "upstream"}, {"name": "target"}, {"name": "unrelated"}],
            sources=[{"name": "raw"}, {"name": "other_raw"}],
            tests=[{"name": "quality", "model": "target"}],
            artifacts={
                "schemas": [
                    {"subject": "raw-value", "ownership": _ownership("source", "raw")},
                    {
                        "subject": "other-value",
                        "ownership": _ownership("source", "other_raw"),
                    },
                ],
                "topics": [
                    {"name": "upstream", "ownership": _ownership("model", "upstream")},
                    {"name": "target", "ownership": _ownership("model", "target")},
                    {"name": "unrelated", "ownership": _ownership("model", "unrelated")},
                    {"name": "legacy-without-owner"},
                ],
                "flink_jobs": [
                    {"name": "target", "ownership": _ownership("model", "target")},
                    {"name": "unrelated", "ownership": _ownership("model", "unrelated")},
                ],
                "test_jobs": [
                    {"name": "test_quality", "ownership": _ownership("model", "target")}
                ],
                "connectors": [
                    {"name": "target_sink", "ownership": _ownership("model", "target")},
                    {
                        "name": "unrelated_sink",
                        "ownership": _ownership("model", "unrelated"),
                    },
                ],
                "gateway_rules": [
                    {"name": "upstream_rule", "ownership": _ownership("model", "upstream")},
                    {
                        "name": "unrelated_rule",
                        "ownership": _ownership("model", "unrelated"),
                    },
                ],
            },
        )

        filter_manifest_for_selection(
            manifest,
            selected_models={"upstream", "target"},
            selected_sources={"raw"},
        )

        assert [model["name"] for model in manifest.models] == ["upstream", "target"]
        assert [source["name"] for source in manifest.sources] == ["raw"]
        assert [schema["subject"] for schema in manifest.artifacts["schemas"]] == [
            "raw-value"
        ]
        assert [topic["name"] for topic in manifest.artifacts["topics"]] == [
            "upstream",
            "target",
        ]
        assert [job["name"] for job in manifest.artifacts["flink_jobs"]] == ["target"]
        assert [job["name"] for job in manifest.artifacts["test_jobs"]] == [
            "test_quality"
        ]
        assert [connector["name"] for connector in manifest.artifacts["connectors"]] == [
            "target_sink"
        ]
        assert [rule["name"] for rule in manifest.artifacts["gateway_rules"]] == [
            "upstream_rule"
        ]

    def test_adopted_model_and_source_artifacts_are_selected(self):
        manifest = Manifest(
            version="1.0",
            project_name="payments",
            artifacts={
                "schemas": [
                    {
                        "subject": "raw-value",
                        "ownership": _ownership("source", "raw", mode="adopted"),
                    },
                    {
                        "subject": "external-value",
                        "ownership": _ownership(
                            "source", "raw", mode="external"
                        ),
                    },
                ],
                "topics": [
                    {
                        "name": "target",
                        "ownership": _ownership("model", "target", mode="adopted"),
                    },
                    {
                        "name": "external",
                        "ownership": _ownership(
                            "model", "target", mode="external"
                        ),
                    },
                ],
            },
        )

        filter_manifest_for_selection(
            manifest,
            selected_models={"target"},
            selected_sources={"raw"},
        )

        assert [schema["subject"] for schema in manifest.artifacts["schemas"]] == [
            "raw-value", "external-value"
        ]
        assert [topic["name"] for topic in manifest.artifacts["topics"]] == ["target", "external"]

    def test_external_claims_are_retained_but_unowned_artifacts_are_not_selected(self):
        manifest = Manifest(
            version="1.0",
            project_name="payments",
            artifacts={
                "topics": [
                    {"name": "unowned"},
                    {
                        "name": "external",
                        "ownership": _ownership("model", "target", mode="external"),
                    },
                ]
            },
        )

        filter_manifest_for_selection(manifest, {"target"}, set())

        assert [artifact["name"] for artifact in manifest.artifacts["topics"]] == ["external"]

    @pytest.mark.parametrize("ownership", [None, {}, {"mode": "external"}, "external"])
    def test_invalid_unselected_ownership_is_not_silently_discarded(self, ownership):
        manifest = Manifest(
            version="1.0",
            project_name="payments",
            models=[{"name": "target"}, {"name": "unrelated"}],
            artifacts={"topics": [{"name": "unrelated", "ownership": ownership}]},
        )
        original = deepcopy(manifest)
        with pytest.raises(StateFormatError, match="malformed ownership"):
            filter_manifest_for_selection(manifest, {"target"}, set())
        assert manifest == original

    @pytest.mark.parametrize("mode", ["managed", "adopted", "external"])
    def test_foreign_unselected_ownership_is_not_silently_discarded(self, mode):
        manifest = Manifest(
            version="1.0",
            project_name="payments",
            artifacts={"topics": [{
                "name": "unrelated",
                "ownership": ArtifactOwnership("foreign", "model", "unrelated", mode).to_dict(),
            }]},
        )
        original = deepcopy(manifest)
        with pytest.raises(StateIdentityError, match="another project"):
            filter_manifest_for_selection(manifest, {"target"}, set())
        assert manifest == original

    def test_retaining_external_claims_does_not_include_unselected_managed_work(self):
        manifest = Manifest(
            version="1.0",
            project_name="payments",
            artifacts={"topics": [
                {"name": "target", "ownership": _ownership("model", "target")},
                {"name": "unrelated", "ownership": _ownership("model", "unrelated")},
                {"name": "external", "ownership": _ownership("model", "external", "external")},
            ]},
        )
        filter_manifest_for_selection(manifest, {"target"}, set())
        assert [artifact["name"] for artifact in manifest.artifacts["topics"]] == ["target", "external"]


class TestDestructiveDefaults:
    def test_safety_config_defaults_destructive_operations_off(self):
        assert SafetyConfig().allow_destructive is False

    def test_single_environment_mode_requires_force(self):
        assert destructive_operations_allowed(None, force=False) is False
        assert destructive_operations_allowed(None, force=True) is True

    def test_environment_must_explicitly_enable_destructive_operations(self):
        config = EnvironmentConfig(
            environment=EnvironmentInfo(name="dev"),
            runtime={},
            safety=SafetyConfig(allow_destructive=True),
        )

        assert destructive_operations_allowed(config, force=False) is True


class TestReviewedPlanPolicy:
    def test_policy_is_off_by_default_for_unprotected_environment(self):
        config = EnvironmentConfig(
            environment=EnvironmentInfo(name="dev"),
            runtime={},
        )

        assert config.requires_reviewed_plan is False

    def test_explicit_safety_policy_marks_shared_environment(self):
        config = EnvironmentConfig(
            environment=EnvironmentInfo(name="staging"),
            runtime={},
            safety=SafetyConfig(require_reviewed_plan=True),
        )

        assert config.requires_reviewed_plan is True

    def test_protected_environment_cannot_disable_reviewed_plan_policy(self):
        config = EnvironmentConfig(
            environment=EnvironmentInfo(name="prod", protected=True),
            runtime={},
            safety=SafetyConfig(require_reviewed_plan=False),
        )

        assert config.requires_reviewed_plan is True

    def test_explicit_confirmation_policy_applies_to_unprotected_environment(self):
        config = EnvironmentConfig(
            environment=EnvironmentInfo(name="staging"),
            runtime={},
            safety=SafetyConfig(confirm_apply=True),
        )

        assert config.requires_apply_confirmation is True

    def test_protected_environment_cannot_disable_confirmation(self):
        config = EnvironmentConfig(
            environment=EnvironmentInfo(name="prod", protected=True),
            runtime={},
            safety=SafetyConfig(confirm_apply=False),
        )

        assert config.requires_apply_confirmation is True
