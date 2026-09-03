"""Release workflow safety-contract tests."""

from pathlib import Path

import yaml

WORKFLOW_PATH = Path(__file__).parents[2] / ".github" / "workflows" / "release.yml"


def _workflow() -> dict[str, object]:
    """Load GitHub Actions YAML without YAML 1.1 coercing the `on` key."""
    loaded = yaml.load(WORKFLOW_PATH.read_text(), Loader=yaml.BaseLoader)
    assert isinstance(loaded, dict)
    return loaded


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
    assert "The first alpha must be published as a GitHub prerelease" in rendered
    assert 'f"v{version}"' in rendered
    assert 'show-ref --verify --quiet "refs/tags/${RELEASE_TAG}"' in rendered
    assert 'tag_sha="$(git rev-list -n 1 "refs/tags/${RELEASE_TAG}")"' in rendered
    assert "actions/workflows/ci.yml/runs" in rendered
    assert '"head_sha": release_sha' in rendered
    assert 'run.get("conclusion") == "success"' in rendered
    assert 'run.get("head_branch") == "main"' in rendered
    assert "python -m twine check --strict dist/*" in rendered
    assert "sha256sum dist/*" in rendered
    assert 'artifacts=("${wheels[0]}" "${source_distributions[0]}")' in rendered
    assert 'importlib.metadata.version("streamt") == expected' in rendered
    assert "streamt.__version__ == expected" in rendered
    assert "actions/upload-artifact@v7" in rendered

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
    assert publish["permissions"] == {"id-token": "write"}

    steps = publish["steps"]
    assert isinstance(steps, list)
    rendered = "\n".join(str(step) for step in steps)
    assert "actions/download-artifact@v8" in rendered
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
    assert publish["permissions"] == {"id-token": "write"}

    steps = publish["steps"]
    assert isinstance(steps, list)
    rendered = "\n".join(str(step) for step in steps)
    assert "actions/download-artifact@v8" in rendered
    assert (
        "pypa/gh-action-pypi-publish@dc37677b2e1c63e2034f94d8a5b11f265b73ba33"
        in rendered
    )
    assert "https://test.pypi.org/legacy/" in rendered
