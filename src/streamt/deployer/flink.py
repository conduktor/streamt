"""Flink deployer for job management."""

from __future__ import annotations

import fcntl
import hashlib
import json as _json
import logging
import os
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import requests

from streamt.compiler.manifest import FlinkJobArtifact
from streamt.core import errors

logger = logging.getLogger(__name__)

# Default timeouts (in seconds)
DEFAULT_TIMEOUT = 30
HEALTH_CHECK_TIMEOUT = 10
STATEMENT_TIMEOUT = 60


def _split_sql_statements(sql: str) -> list[str]:
    """Split SQL into statements, respecting string literals.

    Handles semicolons inside single-quoted strings correctly.
    """
    statements = []
    current = []
    in_string = False
    escape_next = False

    for char in sql:
        if escape_next:
            current.append(char)
            escape_next = False
            continue

        if char == "\\" and in_string:
            escape_next = True
            current.append(char)
            continue

        if char == "'":
            in_string = not in_string
            current.append(char)
            continue

        if char == ";" and not in_string:
            stmt = "".join(current).strip()
            if stmt:
                statements.append(stmt)
            current = []
            continue

        current.append(char)

    # Don't forget the last statement (might not end with ;)
    stmt = "".join(current).strip()
    if stmt:
        statements.append(stmt)

    return statements


@dataclass
class FlinkJobState:
    """Current state of a Flink job."""

    name: str
    exists: bool
    job_id: Optional[str] = None
    status: Optional[str] = None


@dataclass
class FlinkJobChange:
    """A change to apply to a Flink job."""

    job_name: str
    action: str  # submit, cancel, update, none
    current: Optional[FlinkJobState] = None
    desired: Optional[FlinkJobArtifact] = None


class FlinkDeployer:
    """Deployer for Flink jobs via REST API and SQL Gateway.

    Supports context manager protocol for proper resource cleanup:

        with FlinkDeployer(rest_url, sql_gateway_url) as deployer:
            deployer.submit_sql("SELECT 1")
        # Session automatically cleaned up
    """

    def __init__(
        self,
        rest_url: str,
        sql_gateway_url: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        api_key: Optional[str] = None,
        ssl_ca_location: Optional[str] = None,
        ssl_certificate_location: Optional[str] = None,
        ssl_key_location: Optional[str] = None,
        ssl_key_password: Optional[str] = None,
        version: Optional[str] = None,
        environment: Optional[str] = None,
        state_dir: Optional[Path] = None,
        timeout: Optional[int] = None,
        retries: Optional[int] = None,
        statement_timeout: Optional[int] = None,
    ) -> None:
        """Initialize Flink deployer."""
        if not rest_url or not rest_url.startswith(("http://", "https://")):
            raise ValueError(
                f"Invalid Flink REST URL: {rest_url!r} — must start with http:// or https://"
            )
        if sql_gateway_url and not sql_gateway_url.startswith(("http://", "https://")):
            raise ValueError(
                f"Invalid SQL Gateway URL: {sql_gateway_url!r} — must start with http:// or https://"
            )

        self.rest_url = rest_url.rstrip("/")
        self.sql_gateway_url = sql_gateway_url.rstrip("/") if sql_gateway_url else None
        self.version = version
        self.environment = environment
        self.session_id: Optional[str] = None
        self._closed = False
        self._timeout = timeout or DEFAULT_TIMEOUT
        self._retries = retries or 3
        self._statement_timeout = statement_timeout or STATEMENT_TIMEOUT
        self._state_dir = state_dir
        self._sql_hashes: dict[str, str] = {}
        self._load_hashes()
        self._http_session = self._configure_http_session(
            username,
            password,
            api_key,
            ssl_ca_location,
            ssl_certificate_location,
            ssl_key_location,
            ssl_key_password,
        )

    @staticmethod
    def _configure_http_session(
        username: Optional[str],
        password: Optional[str],
        api_key: Optional[str],
        ssl_ca_location: Optional[str],
        ssl_certificate_location: Optional[str],
        ssl_key_location: Optional[str],
        ssl_key_password: Optional[str],
    ) -> requests.Session:
        """Build and return a configured requests Session."""
        from streamt.deployer.ssl_utils import configure_session_ssl

        session = requests.Session()
        if username and password:
            session.auth = (username, password)
        if api_key:
            session.headers["Authorization"] = f"Bearer {api_key}"
        configure_session_ssl(
            session,
            ssl_ca_location=ssl_ca_location,
            ssl_certificate_location=ssl_certificate_location,
            ssl_key_location=ssl_key_location,
            ssl_key_password=ssl_key_password,
        )
        return session

    def __enter__(self) -> FlinkDeployer:
        """Enter context manager."""
        return self

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        """Exit context manager, cleaning up resources."""
        self.close()

    def close(self) -> None:
        """Close the deployer and clean up resources."""
        self._closed = True
        self.close_session()
        self._http_session.close()

    def close_session(self) -> None:
        """Close the SQL Gateway session."""
        if self.session_id:
            try:
                self._http_session.delete(
                    f"{self.sql_gateway_url}/v1/sessions/{self.session_id}",
                    timeout=HEALTH_CHECK_TIMEOUT,
                )
            except Exception as e:
                logger.debug(f"Failed to close session {self.session_id}: {e}")
            finally:
                self.session_id = None

    def _request(
        self,
        method: str,
        endpoint: str,
        use_sql_gateway: bool = False,
        timeout: Optional[int] = None,
        **kwargs: object,
    ) -> dict | list | None:
        """Make a request to Flink API. Returns parsed JSON.

        Raises on HTTP errors.
        """
        if self._closed:
            raise RuntimeError("FlinkDeployer is closed")
        if use_sql_gateway:
            if not self.sql_gateway_url:
                raise ValueError(errors.sql_gateway_not_configured())
            base_url = self.sql_gateway_url
        else:
            base_url = self.rest_url
        url = f"{base_url}{endpoint}"
        effective_timeout = timeout or self._timeout
        last_err: Optional[Exception] = None
        for attempt in range(self._retries):
            try:
                response = self._http_session.request(
                    method, url, timeout=effective_timeout, **kwargs
                )
                status_code = getattr(response, "status_code", 200)
                if (
                    isinstance(status_code, int)
                    and status_code >= 500
                    and attempt < self._retries - 1
                ):
                    last_err = requests.HTTPError(response=response)
                    time.sleep(0.5 * (attempt + 1))
                    continue
                break
            except (requests.ConnectionError, requests.Timeout) as e:
                last_err = e
                if attempt < self._retries - 1:
                    time.sleep(0.5 * (attempt + 1))
        else:
            raise last_err  # type: ignore[misc]
        response.raise_for_status()
        if response.status_code == 204 or not response.content:
            return None
        return response.json()

    def check_connection(self) -> bool:
        """Check if Flink cluster is accessible."""
        try:
            self._request("GET", "/overview", timeout=HEALTH_CHECK_TIMEOUT)
            return True
        except Exception as e:
            logger.debug(f"Flink connection check failed: {e}")
            return False

    def get_session(self) -> str:
        """Get or create a SQL session."""
        if self.session_id:
            return self.session_id

        # Create a new session (SQL Gateway API - Flink 1.18+)
        response = self._request(
            "POST",
            "/v1/sessions",
            use_sql_gateway=True,
            json={},
        )
        if not isinstance(response, dict) or "sessionHandle" not in response:
            raise RuntimeError(f"SQL Gateway did not return a sessionHandle. Response: {response}")
        self.session_id = response["sessionHandle"]
        return self.session_id

    def list_jobs(self, include_details: bool = False) -> list[dict]:
        """List all jobs.

        Args:
            include_details: If True, fetch full details for each job (including name).
                             This makes an additional API call per job.
        """
        response = self._request("GET", "/jobs")
        if not isinstance(response, dict):
            return []
        jobs = response.get("jobs", [])

        if include_details:
            detailed_jobs = []
            for job in jobs:
                job_id = job.get("id") or job.get("jid")
                if not job_id:
                    logger.warning("Job entry missing 'id' and 'jid' keys: %s", job)
                    detailed_jobs.append(job)
                    continue
                try:
                    details = self._request("GET", f"/jobs/{job_id}")
                    if isinstance(details, dict):
                        detailed_jobs.append(details)
                    else:
                        detailed_jobs.append(job)
                except requests.HTTPError as e:
                    logger.warning(f"Failed to fetch details for job {job_id}: {e}")
                    detailed_jobs.append(job)
                except Exception as e:
                    logger.error(f"Unexpected error fetching job {job_id}: {e}")
                    detailed_jobs.append(job)
            return detailed_jobs

        return jobs

    def get_job_state(self, job_name: str) -> FlinkJobState:
        """Get current state of a job by name.

        Matches jobs using multiple strategies:
        1. Exact match on job name
        2. Match Flink's auto-generated name pattern for INSERT jobs:
           'insert-into_<catalog>.<database>.<table_name>'

        Handles different naming conventions:
        - Model processors: '{model}_processor' -> sink: '{model}_sink'
        - Test jobs: 'test_{name}' (no _processor suffix) -> sink: 'test_failures_{name}'

        When multiple jobs match, prioritizes RUNNING jobs over FAILED/CANCELED.
        """
        jobs = self.list_jobs(include_details=True)

        # Derive expected sink table names based on job type
        expected_suffixes = []

        # Test jobs: start with test_ but DON'T end with _processor
        # (models named test_foo would have job name test_foo_processor)
        is_test_job = job_name.startswith("test_") and not job_name.endswith("_processor")

        if is_test_job:
            # Test job: test_foo -> sink: test_failures_foo
            test_name = job_name.removeprefix("test_")
            expected_suffixes.append(f"test_failures_{test_name}")
        else:
            # Model processor: foo_processor -> sink: foo_sink
            base_name = job_name.removesuffix("_processor")
            expected_suffixes.append(f"{base_name}_sink")

        def matches_job(flink_job_name: str) -> bool:
            # Strategy 1: Exact match
            if flink_job_name == job_name:
                return True
            # Strategy 2: Match Flink's INSERT job naming pattern
            return any(flink_job_name.endswith(suffix) for suffix in expected_suffixes)

        # Collect all matching jobs
        matching_jobs = []
        for job in jobs:
            flink_job_name = job.get("name", "")
            if matches_job(flink_job_name):
                matching_jobs.append(job)

        if not matching_jobs:
            return FlinkJobState(name=job_name, exists=False)

        # Prioritize: RUNNING > CREATED > others
        priority = {"RUNNING": 0, "CREATED": 1}
        matching_jobs.sort(key=lambda j: priority.get(j.get("state", ""), 99))

        best_match = matching_jobs[0]
        return FlinkJobState(
            name=job_name,
            exists=True,
            job_id=best_match.get("jid") or best_match.get("id"),
            status=best_match.get("state") or best_match.get("status"),
        )

    def _poll_statement(
        self, session_id: str, operation_handle: str, statement: str, timeout: int
    ) -> None:
        """Poll a submitted statement until FINISHED or raise on ERROR/timeout.

        Uses exponential backoff (0.5 s → 5 s cap) with a wall-clock deadline.
        """
        poll_interval = 0.5
        max_poll_interval = 5.0
        deadline = time.monotonic() + timeout

        while time.monotonic() < deadline:
            status_response = self._request(
                "GET",
                f"/v1/sessions/{session_id}/operations/{operation_handle}/status",
                use_sql_gateway=True,
            )
            status = status_response.get("status")

            if status == "FINISHED":
                return
            elif status == "ERROR":
                error_msg = status_response.get("error")
                if not error_msg:
                    try:
                        result_url = f"{self.sql_gateway_url}/v1/sessions/{session_id}/operations/{operation_handle}/result/0"
                        result_resp = self._http_session.get(result_url, timeout=DEFAULT_TIMEOUT)
                        try:
                            result_data = result_resp.json()
                        except ValueError:
                            result_data = {}
                        error_list = result_data.get("errors", [])
                        error_msg = " ".join(error_list) if error_list else "Unknown error"
                    except Exception as e:
                        error_msg = f"Unknown error (failed to fetch details: {e})"
                raise RuntimeError(errors.flink_sql_error(error_msg, statement[:200]))
            elif status in ("RUNNING", "PENDING"):
                time.sleep(poll_interval)
                poll_interval = min(poll_interval * 2, max_poll_interval)
            else:
                raise RuntimeError(f"Unknown status '{status}' for statement: {statement[:50]}...")
        raise RuntimeError(f"Timeout waiting for statement: {statement[:50]}...")

    def submit_sql(self, sql: str, statement_timeout: Optional[int] = None) -> dict:
        """Submit SQL statements to Flink via SQL Gateway.

        Args:
            sql: SQL statements to execute (can contain multiple statements)
            statement_timeout: Timeout per statement in seconds (default 60)

        Returns:
            Dict with 'results' key containing list of statement results
        """
        statement_timeout = statement_timeout or self._statement_timeout
        session_id = self.get_session()
        statements = _split_sql_statements(sql)

        results = []
        try:
            for statement in statements:
                response = self._request(
                    "POST",
                    f"/v1/sessions/{session_id}/statements",
                    use_sql_gateway=True,
                    json={"statement": statement},
                )
                if not isinstance(response, dict):
                    raise RuntimeError(
                        f"Unexpected response type for statement: {statement[:50]}..."
                    )
                operation_handle = response.get("operationHandle")
                if not operation_handle:
                    raise RuntimeError(
                        f"No operationHandle returned for statement: {statement[:50]}..."
                    )

                self._poll_statement(session_id, operation_handle, statement, statement_timeout)
                results.append({"status": "FINISHED", "statement": statement[:100]})
        except Exception:
            self.close_session()
            raise

        return {"results": results}

    def cancel_job(self, job_id: str) -> None:
        """Cancel a running job."""
        self._request("PATCH", f"/jobs/{job_id}", json={"state": "cancelled"})

    @staticmethod
    def _sql_hash(sql: str) -> str:
        """Compute a short hash of SQL for change detection."""
        return hashlib.sha256(sql.encode()).hexdigest()[:16]

    @property
    def _hashes_file(self) -> Optional[Path]:
        """Path to the hashes persistence file, or None if no state_dir."""
        if self._state_dir is None:
            return None
        return self._state_dir / "flink_hashes.json"

    def _load_hashes(self) -> None:
        """Load SQL hashes from state file if available."""
        path = self._hashes_file
        if path is None or not path.exists():
            return
        try:
            data = _json.loads(path.read_text())
            if isinstance(data, dict):
                self._sql_hashes.update(data)
        except Exception:
            logger.warning("Corrupt state file %s — starting with empty hashes", path)

    def _save_hashes(self) -> None:
        """Persist SQL hashes to state file (atomic write with file locking)."""
        path = self._hashes_file
        if path is None:
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        lock_path = path.with_suffix(".lock")
        with open(lock_path, "w") as lock_fd:
            fcntl.flock(lock_fd, fcntl.LOCK_EX)
            try:
                # Re-read on-disk state under the lock and merge
                on_disk: dict[str, str] = {}
                if path.exists():
                    try:
                        data = _json.loads(path.read_text())
                        if isinstance(data, dict):
                            on_disk = data
                    except Exception:
                        pass
                merged = {**on_disk, **self._sql_hashes}
                tmp_name = None
                try:
                    with tempfile.NamedTemporaryFile(
                        mode="w",
                        dir=path.parent,
                        suffix=".tmp",
                        delete=False,
                    ) as fd:
                        tmp_name = fd.name
                        _json.dump(merged, fd)
                        fd.flush()
                        os.fsync(fd.fileno())
                    Path(tmp_name).replace(path)
                except Exception as e:
                    logger.warning("Failed to save hashes to %s: %s", path, e)
                    if tmp_name:
                        try:
                            Path(tmp_name).unlink(missing_ok=True)
                        except Exception:
                            pass
            finally:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)

    def set_sql_hash(self, job_name: str, sql: str) -> None:
        """Record the SQL hash for a job (used to seed state from a prior deploy)."""
        self._sql_hashes[job_name] = self._sql_hash(sql)
        self._save_hashes()

    def plan_job(self, artifact: FlinkJobArtifact) -> FlinkJobChange:
        """Plan changes for a Flink job."""
        current = self.get_job_state(artifact.name)

        if not current.exists:
            return FlinkJobChange(
                job_name=artifact.name,
                action="submit",
                current=current,
                desired=artifact,
            )

        # Job exists - check if running
        if current.status in ["RUNNING", "CREATED"]:
            # Check if SQL changed (requires prior hash from apply or set_sql_hash)
            if artifact.name in self._sql_hashes:
                desired_hash = self._sql_hash(artifact.sql)
                if desired_hash != self._sql_hashes[artifact.name]:
                    return FlinkJobChange(
                        job_name=artifact.name,
                        action="update",  # SQL changed, cancel + re-submit
                        current=current,
                        desired=artifact,
                    )
            else:
                logger.debug(
                    "No prior SQL hash for job '%s'; cannot detect SQL changes. "
                    "Use set_sql_hash() or re-apply to seed the hash.",
                    artifact.name,
                )
            return FlinkJobChange(
                job_name=artifact.name,
                action="none",
                current=current,
                desired=artifact,
            )

        # Job exists but not running (FAILED, CANCELED, etc.)
        return FlinkJobChange(
            job_name=artifact.name,
            action="submit",  # Re-submit
            current=current,
            desired=artifact,
        )

    def apply_job(self, artifact: FlinkJobArtifact) -> str:
        """Apply a Flink job artifact. Returns action taken."""
        change = self.plan_job(artifact)

        if change.action == "submit":
            self.submit_sql(artifact.sql)
            self._sql_hashes[artifact.name] = self._sql_hash(artifact.sql)
            self._save_hashes()
            return "submitted"
        elif change.action == "update":
            # Cancel the running job, then re-submit with new SQL.
            # If resubmit fails, the pipeline is down — log loudly and re-raise.
            if change.current and change.current.job_id:
                self.cancel_job(change.current.job_id)
            try:
                self.submit_sql(artifact.sql)
            except Exception:
                # Job was cancelled but resubmit failed. Clear hash so next plan
                # sees a missing job and retries as "submit" rather than skipping.
                self._sql_hashes.pop(artifact.name, None)
                self._save_hashes()
                logger.critical(
                    "PIPELINE DOWN: job '%s' was cancelled but resubmit failed. "
                    "Re-run 'streamt apply' after fixing the SQL.",
                    artifact.name,
                )
                raise
            self._sql_hashes[artifact.name] = self._sql_hash(artifact.sql)
            self._save_hashes()
            return "submitted"
        elif change.action == "cancel":
            if change.current and change.current.job_id:
                self.cancel_job(change.current.job_id)
            return "cancelled"
        else:
            return "unchanged"
