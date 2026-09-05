"""No Docker required: scoped cleanup and failed-ownership regression tests."""

import json
import subprocess
from unittest.mock import patch

import pytest
from run_proof import cleanup_owned_broker


def result(stdout):
    return subprocess.CompletedProcess([], 0, stdout=stdout, stderr="")


def test_cleanup_removes_only_owned_container_with_its_anonymous_volumes():
    responses = [
        result("token\n"),
        result(
            json.dumps(
                [
                    {"Type": "volume", "Name": "owned-one"},
                    {"Type": "volume", "Name": "owned-two"},
                ]
            )
        ),
        result("removed\n"),
        result("unrelated-volume\n"),
    ]
    with patch("run_proof.run", side_effect=responses) as run:
        assert cleanup_owned_broker("streamt-kstreams-proof-token", "token") == [
            "owned-one",
            "owned-two",
        ]
    assert run.call_args_list[2].args[0] == [
        "docker",
        "rm",
        "-f",
        "-v",
        "streamt-kstreams-proof-token",
    ]
    assert all("prune" not in call.args[0] for call in run.call_args_list)


@pytest.mark.parametrize(
    ("name", "owner"),
    [
        ("streamt-kstreams-proof-token", "other"),
        ("unrelated-container", "token"),
    ],
)
def test_cleanup_requires_label_and_exact_generated_name(name, owner):
    with (
        patch("run_proof.run", return_value=result(owner)) as run,
        pytest.raises(RuntimeError, match="ownership mismatch"),
    ):
        cleanup_owned_broker(name, "token")
    assert run.call_count == 1


def test_cleanup_fails_if_a_recorded_volume_remains():
    responses = [
        result("token\n"),
        result('[{"Type":"volume","Name":"owned-one"}]'),
        result("removed\n"),
        result("owned-one\n"),
    ]
    with patch("run_proof.run", side_effect=responses), pytest.raises(RuntimeError, match="volume"):
        cleanup_owned_broker("streamt-kstreams-proof-token", "token")
