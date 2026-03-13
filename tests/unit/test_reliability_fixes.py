"""Tests for reliability fixes from Antithesis Reliability Audit."""

from __future__ import annotations

import json
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from streamt.core.errors import ErrorCode

# ============================================================
# BUG-007: ErrorCode.MISSING_CONFIG must exist
# ============================================================


class TestMissingErrorCodes:
    def test_missing_config_error_code_exists(self):
        assert hasattr(ErrorCode, "MISSING_CONFIG")
        assert "E208" in ErrorCode.MISSING_CONFIG

    def test_deploy_error_code_exists(self):
        assert hasattr(ErrorCode, "DEPLOY_ERROR")
        assert "E407" in ErrorCode.DEPLOY_ERROR


# ============================================================
# BUG-003: Consumer leak in get_topic_message_count
# ============================================================


class TestConsumerLeak:
    def test_consumer_closed_on_success(self):
        from streamt.deployer.kafka import KafkaDeployer

        deployer = KafkaDeployer.__new__(KafkaDeployer)
        deployer._check_closed = MagicMock()
        mock_consumer = MagicMock()
        mock_consumer.get_watermark_offsets.return_value = (0, 50)

        meta = MagicMock()
        meta.topics = {"orders": MagicMock(partitions={"0": None, "1": None})}
        mock_admin = MagicMock()
        mock_admin.list_topics.return_value = meta
        deployer.admin = mock_admin
        deployer._config = {}
        deployer._closed = False

        with patch("streamt.deployer.kafka.Consumer", return_value=mock_consumer):
            count = deployer.get_topic_message_count("orders")

        mock_consumer.close.assert_called_once()
        assert count == 100

    def test_consumer_closed_on_exception(self):
        from streamt.deployer.kafka import KafkaDeployer

        deployer = KafkaDeployer.__new__(KafkaDeployer)
        deployer._check_closed = MagicMock()
        mock_consumer = MagicMock()
        mock_consumer.get_watermark_offsets.side_effect = RuntimeError("broker down")

        meta = MagicMock()
        meta.topics = {"orders": MagicMock(partitions={"0": None})}
        mock_admin = MagicMock()
        mock_admin.list_topics.return_value = meta
        deployer.admin = mock_admin
        deployer._config = {}
        deployer._closed = False

        with patch("streamt.deployer.kafka.Consumer", return_value=mock_consumer):
            result = deployer.get_topic_message_count("orders")

        # Consumer must be closed even on error
        mock_consumer.close.assert_called_once()
        assert result == 0  # Returns 0 on exception


# ============================================================
# BUG-005 + BUG-006: fsync + file locking in hash persistence
# ============================================================


class TestHashPersistenceDurability:
    def test_fsync_called_before_rename(self, tmp_path):
        from streamt.deployer.flink import FlinkDeployer

        deployer = FlinkDeployer.__new__(FlinkDeployer)
        deployer._state_dir = tmp_path / ".streamt"
        deployer._sql_hashes = {"job1": "abc123"}

        fsync_calls = []
        orig_fsync = os.fsync

        def track_fsync(fd):
            fsync_calls.append(fd)
            return orig_fsync(fd)

        with patch("streamt.deployer.flink.os.fsync", side_effect=track_fsync):
            deployer._save_hashes()

        assert len(fsync_calls) == 1
        hashes_file = deployer._hashes_file
        assert hashes_file.exists()
        assert json.loads(hashes_file.read_text()) == {"job1": "abc123"}

    def test_file_locking_used_on_save(self, tmp_path):
        from streamt.deployer.flink import FlinkDeployer

        deployer = FlinkDeployer.__new__(FlinkDeployer)
        deployer._state_dir = tmp_path / ".streamt"
        deployer._sql_hashes = {"job2": "xyz"}

        flock_calls = []
        orig_flock = __import__("fcntl").flock

        def track_flock(fd, op):
            flock_calls.append(op)
            return orig_flock(fd, op)

        with patch("streamt.deployer.flink.fcntl.flock", side_effect=track_flock):
            deployer._save_hashes()

        import fcntl

        assert fcntl.LOCK_EX in flock_calls
        assert fcntl.LOCK_UN in flock_calls

    def test_concurrent_save_merges_entries(self, tmp_path):
        """Two instances writing different jobs must not clobber each other."""
        from streamt.deployer.flink import FlinkDeployer

        state_dir = tmp_path / ".streamt"

        d1 = FlinkDeployer.__new__(FlinkDeployer)
        d1._state_dir = state_dir
        d1._sql_hashes = {"job_a": "hash_a"}
        d1._save_hashes()

        d2 = FlinkDeployer.__new__(FlinkDeployer)
        d2._state_dir = state_dir
        d2._sql_hashes = {"job_b": "hash_b"}
        d2._save_hashes()

        result = json.loads((state_dir / "flink_hashes.json").read_text())
        assert result["job_a"] == "hash_a"
        assert result["job_b"] == "hash_b"


# ============================================================
# BUG-001: Cancel+resubmit recovery
# ============================================================


class TestCancelResubmitRecovery:
    def _make_flink(self):
        from streamt.deployer.flink import (
            FlinkDeployer,
        )

        deployer = FlinkDeployer.__new__(FlinkDeployer)
        deployer.rest_url = "http://localhost:8082"
        deployer.sql_gateway_url = None
        deployer._http_session = MagicMock()
        deployer._closed = False
        deployer._state_dir = None
        deployer._sql_hashes = {"my_job": "old_hash"}
        deployer._timeout = 30
        deployer._retries = 3
        deployer._statement_timeout = 60
        return deployer

    def test_resubmit_failure_clears_hash_and_logs_critical(self):
        """If resubmit fails after cancel, hash cleared and CRITICAL logged."""
        from streamt.deployer.flink import FlinkJobArtifact, FlinkJobChange, FlinkJobState

        deployer = self._make_flink()

        artifact = FlinkJobArtifact(name="my_job", sql="SELECT 1")
        change = FlinkJobChange(
            job_name="my_job",
            action="update",
            current=FlinkJobState(name="my_job", exists=True, job_id="j-1", status="RUNNING"),
            desired=artifact,
        )

        with (
            patch.object(deployer, "plan_job", return_value=change),
            patch.object(deployer, "cancel_job"),
            patch.object(deployer, "submit_sql", side_effect=RuntimeError("SQL Gateway down")),
            patch.object(deployer, "_save_hashes"),
            patch("streamt.deployer.flink.logger") as mock_logger,
            pytest.raises(RuntimeError, match="SQL Gateway down"),
        ):
            deployer.apply_job(artifact)

        # Hash must be cleared so next plan sees a missing job
        assert "my_job" not in deployer._sql_hashes
        mock_logger.critical.assert_called_once()
        assert "PIPELINE DOWN" in mock_logger.critical.call_args[0][0]

    def test_successful_resubmit_saves_new_hash(self):
        """Successful resubmit should save the new SQL hash."""
        from streamt.deployer.flink import FlinkJobArtifact, FlinkJobChange, FlinkJobState

        deployer = self._make_flink()

        artifact = FlinkJobArtifact(name="my_job", sql="SELECT 2")
        change = FlinkJobChange(
            job_name="my_job",
            action="update",
            current=FlinkJobState(name="my_job", exists=True, job_id="j-1", status="RUNNING"),
            desired=artifact,
        )

        with (
            patch.object(deployer, "plan_job", return_value=change),
            patch.object(deployer, "cancel_job"),
            patch.object(deployer, "submit_sql"),
            patch.object(deployer, "_save_hashes"),
        ):
            result = deployer.apply_job(artifact)

        assert result == "submitted"
        assert deployer._sql_hashes["my_job"] != "old_hash"


# ============================================================
# BUG-015: Closed-state guards for Connect, SR, Flink
# ============================================================


class TestClosedGuards:
    def test_connect_deployer_raises_when_closed(self):
        from streamt.deployer.connect import ConnectDeployer

        deployer = ConnectDeployer(rest_url="http://localhost:8083")
        deployer.close()
        with pytest.raises(RuntimeError, match="ConnectDeployer is closed"):
            deployer._request("GET", "/connectors")

    def test_schema_registry_raises_when_closed(self):
        from streamt.deployer.schema_registry import SchemaRegistryDeployer

        deployer = SchemaRegistryDeployer(url="http://localhost:8081")
        deployer.close()
        with pytest.raises(RuntimeError, match="SchemaRegistryDeployer is closed"):
            deployer._request("GET", "/subjects")

    def test_flink_raises_when_closed(self):
        from streamt.deployer.flink import FlinkDeployer

        deployer = FlinkDeployer(rest_url="http://localhost:8082")
        deployer.close()
        with pytest.raises(RuntimeError, match="FlinkDeployer is closed"):
            deployer._request("GET", "/jobs")


# ============================================================
# BUG-019: Wall-clock polling timeout
# ============================================================


class TestWallClockPolling:
    def test_polling_uses_monotonic_time(self, tmp_path):
        """Ensure time.monotonic() controls the deadline, not accumulated sleep."""
        from streamt.deployer.flink import FlinkDeployer

        deployer = FlinkDeployer.__new__(FlinkDeployer)
        deployer.rest_url = "http://localhost:8082"
        deployer.sql_gateway_url = "http://localhost:8084"
        deployer.session_id = "s1"
        deployer._http_session = MagicMock()
        deployer._closed = False
        deployer._timeout = 30
        deployer._retries = 3
        deployer._statement_timeout = 5

        call_count = 0

        # Return PENDING forever to force timeout via wall-clock
        def mock_request(method, endpoint, **kwargs):
            nonlocal call_count
            call_count += 1
            if "statements" in endpoint and method == "POST":
                return {"operationHandle": "op1"}
            if "status" in endpoint:
                return {"status": "PENDING"}
            return {}

        with (
            patch.object(deployer, "_request", side_effect=mock_request),
            patch("time.sleep"),
            patch(
                "time.monotonic",
                side_effect=[
                    0.0,  # deadline = 0.0 + 5 = 5.0
                    0.5,
                    1.0,
                    2.0,
                    4.0,  # loop iterations
                    5.1,  # deadline exceeded
                ],
            ),
            pytest.raises(RuntimeError, match="Timeout"),
        ):
            deployer.submit_sql("SELECT 1")


# ============================================================
# BUG-020: Atomic manifest write
# ============================================================


class TestAtomicManifestWrite:
    def test_save_uses_atomic_rename(self, tmp_path):
        from streamt.compiler.manifest import Manifest

        path = tmp_path / "manifest.json"
        rename_calls = []
        orig_replace = Path.replace

        def track_replace(self, target):
            rename_calls.append((str(self), str(target)))
            return orig_replace(self, target)

        manifest = Manifest(version="1.0.0", project_name="test")

        with patch.object(Path, "replace", track_replace):
            manifest.save(path)

        assert len(rename_calls) == 1
        _, dest = rename_calls[0]
        assert dest == str(path)
        assert path.exists()

    def test_fsync_called_in_manifest_save(self, tmp_path):
        from streamt.compiler.manifest import Manifest

        path = tmp_path / "manifest.json"
        fsync_calls = []
        orig_fsync = os.fsync

        def track_fsync(fd):
            fsync_calls.append(fd)
            return orig_fsync(fd)

        manifest = Manifest(version="1.0.0", project_name="test")
        with patch("streamt.compiler.manifest.os.fsync", side_effect=track_fsync):
            manifest.save(path)

        assert len(fsync_calls) == 1


# ============================================================
# BUG-023: URL credential sanitization
# ============================================================


class TestUrlSanitization:
    def test_url_credentials_stripped(self):
        from streamt.deployer.planner import _sanitize_error

        msg = "Connection failed: http://admin:s3cr3t@broker:8081/path"
        result = _sanitize_error(msg)
        assert "s3cr3t" not in result
        assert "admin" not in result
        assert "broker:8081" in result

    def test_password_in_url_stripped(self):
        from streamt.deployer.planner import _sanitize_error

        msg = "Failed: https://user:P@ssw0rd!@host/api"
        result = _sanitize_error(msg)
        assert "P@ssw0rd!" not in result

    def test_safe_message_unchanged(self):
        from streamt.deployer.planner import _sanitize_error

        msg = "Connection to localhost:9092 failed after 3 retries"
        assert _sanitize_error(msg) == msg


# ============================================================
# PATH-001: Path traversal in compiler output
# ============================================================


class TestPathTraversal:
    def test_dotdot_in_name_raises(self):
        from streamt.compiler.compiler import Compiler

        compiler = Compiler.__new__(Compiler)
        with pytest.raises(ValueError, match="Unsafe"):
            compiler._safe_filename("../../../etc/passwd", "topic")

    def test_slash_in_name_raises(self):
        from streamt.compiler.compiler import Compiler

        compiler = Compiler.__new__(Compiler)
        with pytest.raises(ValueError, match="Unsafe"):
            compiler._safe_filename("sub/dir/name", "schema subject")

    def test_backslash_in_name_raises(self):
        from streamt.compiler.compiler import Compiler

        compiler = Compiler.__new__(Compiler)
        with pytest.raises(ValueError, match="Unsafe"):
            compiler._safe_filename("sub\\dir", "connector")

    def test_normal_name_passes(self):
        from streamt.compiler.compiler import Compiler

        compiler = Compiler.__new__(Compiler)
        assert compiler._safe_filename("orders_v2", "topic") == "orders_v2"
        assert compiler._safe_filename("my-schema.value", "schema") == "my-schema.value"


# ============================================================
# BUG-013: Schema compatibility order (register before set_compat)
# ============================================================


class TestSchemaCompatibilityOrderNew:
    def test_new_subject_registers_before_setting_compat(self):
        """On new subject registration, schema must be registered
        BEFORE compatibility is set (validates content first)."""
        from streamt.deployer.schema_registry import (
            SchemaArtifact,
            SchemaChange,
            SchemaRegistryDeployer,
        )

        deployer = SchemaRegistryDeployer.__new__(SchemaRegistryDeployer)
        deployer.url = "http://localhost:8081"
        deployer.auth = None
        deployer.headers = {}
        deployer._closed = False
        deployer._http_session = MagicMock()

        call_order = []
        change = SchemaChange(subject="orders-value", action="register", changes=None)

        with (
            patch.object(deployer, "plan_schema", return_value=change),
            patch.object(
                deployer,
                "register_schema",
                side_effect=lambda *a, **k: call_order.append("register"),
            ),
            patch.object(
                deployer,
                "set_compatibility",
                side_effect=lambda *a, **k: call_order.append("set_compat"),
            ),
        ):
            artifact = SchemaArtifact(
                subject="orders-value",
                schema={"type": "record", "name": "Orders", "fields": []},
                compatibility="FULL",
            )
            deployer.apply_schema(artifact)

        assert call_order == ["register", "set_compat"], (
            f"Expected register then set_compat, got: {call_order}"
        )
