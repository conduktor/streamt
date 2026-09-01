"""Release workflow safety-contract tests."""

from pathlib import Path

import yaml

WORKFLOW_PATH = Path(__file__).parents[2] / ".github" / "workflows" / "release.yml"


def _workflow() -> dict[str, object]:
    """Load GitHub Actions YAML without YAML 1.1 coercing the `on` key."""
    loaded = yaml.load(WORKFLOW_PATH.read_text(), Loader=yaml.BaseLoader)
    assert isinstance(loaded, dict)
    return loaded


def test_release_only_runs_for_published_github_releases() -> None:
    workflow = _workflow()

    assert workflow["on"] == {"release": {"types": ["published"]}}


def test_release_build_verifies_main_tag_and_distribution() -> None:
    workflow = _workflow()
    jobs = workflow["jobs"]
    assert isinstance(jobs, dict)
    build = jobs["build"]
    assert isinstance(build, dict)
    steps = build["steps"]
    assert isinstance(steps, list)
    rendered = "\n".join(str(step) for step in steps)

    assert "merge-base --is-ancestor HEAD origin/main" in rendered
    assert "RELEASE_TAG" in rendered
    assert 'f"v{version}"' in rendered
    assert "python -m twine check dist/*" in rendered
    assert "actions/upload-artifact@v7" in rendered


def test_publish_uses_protected_oidc_environment_and_verified_artifact() -> None:
    workflow = _workflow()
    jobs = workflow["jobs"]
    assert isinstance(jobs, dict)
    publish = jobs["publish"]
    assert isinstance(publish, dict)

    assert publish["needs"] == "build"
    assert publish["environment"] == {
        "name": "pypi",
        "url": "https://pypi.org/project/streamt/",
    }
    assert publish["permissions"] == {"id-token": "write"}

    steps = publish["steps"]
    assert isinstance(steps, list)
    rendered = "\n".join(str(step) for step in steps)
    assert "actions/download-artifact@v8" in rendered
    assert "pypa/gh-action-pypi-publish@release/v1" in rendered
