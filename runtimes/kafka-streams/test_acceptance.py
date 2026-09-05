"""The disposable acceptance fixture deletes only exact owner-checked resources."""

# ruff: noqa: PT009, PT027 -- runnable with the standard-library unittest runner

from __future__ import annotations

import importlib.util
import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location(
    "runtime_acceptance", Path(__file__).with_name("acceptance.py")
)
assert SPEC is not None
assert SPEC.loader is not None
acceptance = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(acceptance)


class CleanupTest(unittest.TestCase):
    def exercise(self, *, foreign: str = "", remaining: bool = False) -> list[list[str]]:
        calls: list[list[str]] = []
        token = "owned-fixture"

        def run(args: list[str]) -> subprocess.CompletedProcess[str]:
            calls.append(args)
            command = args[1:]
            if command[:2] == ["ps", "-aq"]:
                result = "exact-container-id\n"
            elif command == ["inspect", "exact-container-id"]:
                result = json.dumps(
                    [
                        {
                            "Name": "/owned-fixture-broker",
                            "Config": {
                                "Labels": {
                                    acceptance.LABEL: "other" if foreign == "container" else token
                                }
                            },
                            "Mounts": [{"Type": "volume", "Name": "exact-anonymous-volume"}],
                        }
                    ]
                )
            elif command == ["logs", "exact-container-id"]:
                result = "fixture broker logs"
            elif command[:2] == ["volume", "ls"] and "--filter" in command:
                result = "exact-named-volume\n"
            elif command == ["volume", "inspect", "exact-named-volume"]:
                result = json.dumps(
                    [{"Labels": {acceptance.LABEL: "other" if foreign == "volume" else token}}]
                )
            elif command[:2] == ["volume", "ls"]:
                result = "unrelated-user-volume\n" + (
                    "exact-anonymous-volume\n" if remaining else ""
                )
            elif command[:2] == ["network", "ls"]:
                result = "exact-network-id\n"
            elif command == ["network", "inspect", "exact-network-id"]:
                result = json.dumps(
                    [{"Labels": {acceptance.LABEL: "other" if foreign == "network" else token}}]
                )
            else:
                assert command in [
                    ["rm", "-f", "-v", "exact-container-id"],
                    ["volume", "rm", "exact-named-volume"],
                    ["network", "rm", "exact-network-id"],
                ], command
                result = ""
            return subprocess.CompletedProcess(args, 0, result, "")

        with (
            tempfile.TemporaryDirectory(prefix="streamt-cleanup-test-") as temporary,
            patch.object(acceptance, "run", run),
        ):
            if foreign or remaining:
                with self.assertRaisesRegex(RuntimeError, "ownership changed|remains"):
                    acceptance.cleanup(token, Path(temporary))
                self.assertFalse((Path(temporary) / "cleanup.json").exists())
            else:
                acceptance.cleanup(token, Path(temporary))
                evidence = json.loads((Path(temporary) / "cleanup.json").read_text())
                self.assertEqual(
                    evidence["recorded_mount_volume_names_verified_absent"],
                    ["exact-anonymous-volume"],
                )
        return calls

    def test_owned_cleanup_removes_container_anonymous_volume_and_exact_labelled_resources(self):
        calls = self.exercise()
        self.assertIn(["docker", "rm", "-f", "-v", "exact-container-id"], calls)
        self.assertIn(["docker", "volume", "rm", "exact-named-volume"], calls)
        self.assertIn(["docker", "network", "rm", "exact-network-id"], calls)

    def test_foreign_container_is_never_removed(self):
        calls = self.exercise(foreign="container")
        self.assertNotIn(["docker", "rm", "-f", "-v", "exact-container-id"], calls)

    def test_foreign_volume_is_never_removed(self):
        calls = self.exercise(foreign="volume")
        self.assertNotIn(["docker", "volume", "rm", "exact-named-volume"], calls)

    def test_foreign_network_is_never_removed(self):
        calls = self.exercise(foreign="network")
        self.assertNotIn(["docker", "network", "rm", "exact-network-id"], calls)

    def test_known_volume_remaining_does_not_claim_cleanup_success(self):
        self.exercise(remaining=True)


if __name__ == "__main__":
    unittest.main()
