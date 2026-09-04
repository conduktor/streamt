"""Release workflow safety-contract tests."""

from pathlib import Path

import yaml

WORKFLOW_PATH = Path(__file__).parents[2] / ".github" / "workflows" / "release.yml"
CI_WORKFLOW_PATH = Path(__file__).parents[2] / ".github" / "workflows" / "ci.yml"


def _workflow() -> dict[str, object]:
    """Load GitHub Actions YAML without YAML 1.1 coercing the `on` key."""
    loaded = yaml.load(WORKFLOW_PATH.read_text(), Loader=yaml.BaseLoader)
    assert isinstance(loaded, dict)
    return loaded


def _ci_workflow() -> dict[str, object]:
    loaded = yaml.load(CI_WORKFLOW_PATH.read_text(), Loader=yaml.BaseLoader)
    assert isinstance(loaded, dict)
    return loaded


def _step(steps: list[object], name: str) -> dict[str, object]:
    matches = [
        step for step in steps if isinstance(step, dict) and step.get("name") == name
    ]
    assert len(matches) == 1
    return matches[0]


def test_production_release_and_manual_testpypi_rehearsal_are_separate() -> None:
    workflow = _workflow()

    assert workflow["on"] == {
        "release": {"types": ["published"]},
        "workflow_dispatch": {
            "inputs": {
                "tag": {
                    "description": "Existing exact version tag to rehearse on TestPyPI",
                    "required": "true",
                    "type": "string",
                }
            }
        },
    }


def test_release_build_verifies_exact_tag_main_ci_and_distributions() -> None:
    workflow = _workflow()
    jobs = workflow["jobs"]
    assert isinstance(jobs, dict)
    build = jobs["build"]
    assert isinstance(build, dict)
    steps = build["steps"]
    assert isinstance(steps, list)
    rendered = "\n".join(str(step) for step in steps)

    assert 'merge-base --is-ancestor "${release_sha}" origin/main' in rendered
    assert "RELEASE_TAG" in rendered
    assert "RELEASE_IS_PRERELEASE" in rendered
    assert "RELEASE_NAME" in rendered
    assert "The first alpha must be published as a GitHub prerelease" in rendered
    assert "The GitHub release title must exactly match its version tag" in rendered
    assert '"${GITHUB_REF}" != "refs/tags/${RELEASE_TAG}"' in rendered
    assert 'f"v{version}"' in rendered
    assert 'show-ref --verify --quiet "refs/tags/${RELEASE_TAG}"' in rendered
    assert 'tag_sha="$(git rev-list -n 1 "refs/tags/${RELEASE_TAG}")"' in rendered
    assert "actions/workflows/ci.yml/runs" in rendered
    assert '"head_sha": release_sha' in rendered
    assert 'run.get("conclusion") == "success"' in rendered
    assert 'run.get("head_branch") == "main"' in rendered
    ci_step = _step(steps, "Require successful full CI for the exact release commit")
    assert isinstance(ci_step["run"], str)
    assert "run_id={newest['id']}" in ci_step["run"]
    download = _step(steps, "Download the exact distributions tested by CI")
    assert download["with"] == {
        "name": "python-distributions-${{ steps.ci.outputs.run_attempt }}",
        "path": "dist/",
        "run-id": "${{ steps.ci.outputs.run_id }}",
        "github-token": "${{ github.token }}",
    }
    assert "python -m twine check --strict dist/*" in rendered
    assert "sha256sum dist/*" in rendered
    assert 'artifacts=("${wheels[0]}" "${source_distributions[0]}")' in rendered
    assert 'importlib.metadata.version("streamt") == expected' in rendered
    assert "streamt.__version__ == expected" in rendered
    assert "actions/upload-artifact" not in rendered

    assert build["outputs"] == {
        "ci_run_attempt": "${{ steps.ci.outputs.run_attempt }}",
        "ci_run_id": "${{ steps.ci.outputs.run_id }}",
        "release_sha": "${{ steps.identity.outputs.release_sha }}",
        "release_tag": "${{ steps.identity.outputs.release_tag }}",
    }

    assert build["permissions"] == {"actions": "read", "contents": "read"}


def test_publish_uses_protected_oidc_environment_and_verified_artifact() -> None:
    workflow = _workflow()
    jobs = workflow["jobs"]
    assert isinstance(jobs, dict)
    publish = jobs["publish"]
    assert isinstance(publish, dict)

    assert publish["needs"] == "build"
    assert publish["if"] == (
        "github.event_name == 'release' && github.event.release.prerelease == true"
    )
    assert publish["environment"] == {
        "name": "pypi",
        "url": "https://pypi.org/project/streamt/",
    }
    assert publish["permissions"] == {
        "actions": "read",
        "contents": "read",
        "id-token": "write",
    }

    steps = publish["steps"]
    assert isinstance(steps, list)
    rendered = "\n".join(str(step) for step in steps)
    assert (
        "actions/download-artifact@3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c"
        in rendered
    )
    assert "release tag moved after distribution verification" in rendered
    download = _step(steps, "Download release distributions")
    assert isinstance(download["with"], dict)
    assert download["with"]["name"] == (
        "python-distributions-${{ needs.build.outputs.ci_run_attempt }}"
    )
    assert download["with"]["run-id"] == "${{ needs.build.outputs.ci_run_id }}"
    assert download["with"]["github-token"] == "${{ github.token }}"
    assert (
        "pypa/gh-action-pypi-publish@dc37677b2e1c63e2034f94d8a5b11f265b73ba33"
        in rendered
    )
    assert "repository-url" not in rendered


def test_manual_rehearsal_uses_separate_testpypi_environment() -> None:
    workflow = _workflow()
    jobs = workflow["jobs"]
    assert isinstance(jobs, dict)
    publish = jobs["publish-testpypi"]
    assert isinstance(publish, dict)

    assert publish["needs"] == "build"
    assert publish["if"] == "github.event_name == 'workflow_dispatch'"
    assert publish["environment"] == {
        "name": "testpypi",
        "url": "https://test.pypi.org/project/streamt/",
    }
    assert publish["permissions"] == {
        "actions": "read",
        "contents": "read",
        "id-token": "write",
    }

    steps = publish["steps"]
    assert isinstance(steps, list)
    rendered = "\n".join(str(step) for step in steps)
    assert (
        "actions/download-artifact@3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c"
        in rendered
    )
    assert "release tag moved after distribution verification" in rendered
    download = _step(steps, "Download verified rehearsal distributions")
    assert isinstance(download["with"], dict)
    assert download["with"]["name"] == (
        "python-distributions-${{ needs.build.outputs.ci_run_attempt }}"
    )
    assert download["with"]["run-id"] == "${{ needs.build.outputs.ci_run_id }}"
    assert download["with"]["github-token"] == "${{ github.token }}"
    assert (
        "pypa/gh-action-pypi-publish@dc37677b2e1c63e2034f94d8a5b11f265b73ba33"
        in rendered
    )
    assert "https://test.pypi.org/legacy/" in rendered


def test_ci_produces_one_pinned_immutable_distribution_handoff() -> None:
    workflow = _ci_workflow()
    jobs = workflow["jobs"]
    assert isinstance(jobs, dict)
    package = jobs["package"]
    assert isinstance(package, dict)
    steps = package["steps"]
    assert isinstance(steps, list)
    rendered = "\n".join(str(step) for step in steps)

    assert "actions/checkout@d23441a48e516b6c34aea4fa41551a30e30af803" in rendered
    assert "actions/setup-python@ece7cb06caefa5fff74198d8649806c4678c61a1" in rendered
    assert "'pip==26.2.1' 'build==1.6.0' 'twine==7.0.0'" in rendered
    assert "python -m twine check --strict dist/*" in rendered
    assert "actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a" in rendered
    upload = _step(steps, "Upload exact release-candidate distributions")
    assert upload["with"] == {
        "name": "python-distributions-${{ github.run_attempt }}",
        "path": "dist/",
        "if-no-files-found": "error",
        "retention-days": "14",
    }

    step_names = [step["name"] for step in steps if isinstance(step, dict)]
    build_index = step_names.index("Build distributions")
    upload_index = step_names.index("Upload exact release-candidate distributions")
    install_index = step_names.index("Install wheel in a clean environment")
    assert build_index < upload_index < install_index


def test_ci_runs_strimzi_package_parity_on_every_supported_python() -> None:
    workflow = _ci_workflow()
    jobs = workflow["jobs"]
    assert isinstance(jobs, dict)
    parity = jobs["strimzi-package-parity"]
    assert isinstance(parity, dict)
    assert parity["needs"] == "package"
    assert parity["runs-on"] == "ubuntu-latest"
    assert parity["timeout-minutes"] == "20"
    assert parity["strategy"] == {
        "fail-fast": "false",
        "matrix": {"python-version": ["3.10", "3.11", "3.12", "3.13", "3.14"]},
    }
    assert "if" not in parity
    assert "continue-on-error" not in parity

    steps = parity["steps"]
    assert isinstance(steps, list)
    for step in steps:
        assert isinstance(step, dict)
        assert "if" not in step
        assert "continue-on-error" not in step
    checkout = _step(steps, "Checkout")
    setup = _step(steps, "Setup Python")
    download = _step(steps, "Download exact release-candidate distributions")
    execute = _step(steps, "Verify source, wheel, and direct-sdist Strimzi parity")
    assert checkout["uses"] == (
        "actions/checkout@d23441a48e516b6c34aea4fa41551a30e30af803"
    )
    assert setup["uses"] == (
        "actions/setup-python@ece7cb06caefa5fff74198d8649806c4678c61a1"
    )
    assert setup["with"] == {"python-version": "${{ matrix.python-version }}"}
    assert download["uses"] == (
        "actions/download-artifact@3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c"
    )
    assert download["with"] == {
        "name": "python-distributions-${{ github.run_attempt }}",
        "path": "dist/",
    }
    assert execute["timeout-minutes"] == "15"
    command = execute["run"]
    assert isinstance(command, str)
    assert "tests/package/strimzi_package_smoke.py" in command
    assert '--wheel "${wheels[0]}"' in command
    assert '--sdist "${source_distributions[0]}"' in command
    assert '--source-root "${GITHUB_WORKSPACE}"' in command
    assert "Expected exactly one wheel" in command
    assert "Expected exactly one source distribution" in command
    assert "|| true" not in command


def test_release_consumes_the_same_attempt_named_artifact_after_full_ci() -> None:
    workflow = _workflow()
    jobs = workflow["jobs"]
    assert isinstance(jobs, dict)
    build = jobs["build"]
    assert isinstance(build, dict)
    steps = build["steps"]
    assert isinstance(steps, list)
    ci = _step(steps, "Require successful full CI for the exact release commit")
    download = _step(steps, "Download the exact distributions tested by CI")
    assert isinstance(ci["run"], str)
    assert 'run.get("conclusion") == "success"' in ci["run"]
    assert 'output.write(f"run_attempt={newest[\'run_attempt\']}\\n")' in ci["run"]
    assert download["with"] == {
        "name": "python-distributions-${{ steps.ci.outputs.run_attempt }}",
        "path": "dist/",
        "run-id": "${{ steps.ci.outputs.run_id }}",
        "github-token": "${{ github.token }}",
    }
