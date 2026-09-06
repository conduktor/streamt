"""Fixture-only checks of public probe accounting; no Docker or Kafka access."""

from __future__ import annotations

import runpy
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest

from streamt.deployer.state_backend import (
    LocalDeploymentStateBackend,
    StateBackendUnknownCommitError,
)


@pytest.mark.parametrize("unknown_write", [False, True])
def test_probe_counts_real_context_exit_on_normal_and_unknown_write_paths(monkeypatch, unknown_write):
    harness = runpy.run_path(str(Path(__file__).with_name("kafka_streams_public_cli_probe.py")))
    lock = SimpleNamespace(is_held=False)
    released = []

    @contextmanager
    def fake_operation(_backend, _address):
        lock.is_held = True
        try:
            yield SimpleNamespace(_lock=lock)
        finally:
            lock.is_held = False
            released.append(True)

    monkeypatch.setattr(LocalDeploymentStateBackend, "operation", fake_operation)
    # No provider is invoked: only the real guard's context accounting matters.
    reviewed = SimpleNamespace(
        actions=(SimpleNamespace(kafka_streams_evidence=object()),), checksum="sha256:" + "a" * 64,
    )
    proof = {}
    backend = object.__new__(LocalDeploymentStateBackend)
    with harness["command_guard"]("resume_lost_clear", reviewed, None, None, proof):
        try:
            with backend.operation(None):
                assert lock.is_held
                if unknown_write:
                    raise StateBackendUnknownCommitError("fixture acknowledgement lost")
        except StateBackendUnknownCommitError:
            assert unknown_write
    assert lock.is_held is False
    assert released == [True]
    assert proof["lock_contexts_exited"] == 1
    assert proof["docker_mutations"] == []
    assert proof["forbidden_write_attempts"] == []
