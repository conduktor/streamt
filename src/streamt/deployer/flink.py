"""Flink deployer for job management."""

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass
from typing import Any, Optional

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

    def __init__(self, rest_url: str, sql_gateway_url: Optional[str] = None) -> None:
        """Initialize Flink deployer.

        Args:
            rest_url: Flink REST API URL (for job management: /jobs, /overview)
            sql_gateway_url: Flink SQL Gateway URL (for SQL execution: /v1/sessions)
        """
        self.rest_url = rest_url.rstrip("/")
        self.sql_gateway_url = sql_gateway_url.rstrip("/") if sql_gateway_url else None
        self.session_id: Optional[str] = None
        self._http_session = requests.Session()

    def __enter__(self) -> "FlinkDeployer":
        """Enter context manager."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit context manager, cleaning up resources."""
        self.close()

    def close(self) -> None:
        """Close the deployer and clean up resources."""
        self.close_session()
        self._http_session.close()

    def close_session(self) -> None:
        """Close the SQL Gateway session."""
        if self.session_id:
            try:
                self._http_session.delete(
                    f"{self.sql_gateway_url}/v1/sessions/{self.session_id}",
                    timeout=DEFAULT_TIMEOUT,
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
        timeout: int = DEFAULT_TIMEOUT,
        **kwargs: Any,
    ) -> dict:
        """Make a request to Flink API.

        Args:
            method: HTTP method
            endpoint: API endpoint
            use_sql_gateway: If True, use SQL Gateway URL instead of REST URL
            timeout: Request timeout in seconds
            **kwargs: Additional arguments passed to requests
        """
        if use_sql_gateway:
            if not self.sql_gateway_url:
                raise ValueError(errors.sql_gateway_not_configured())
            base_url = self.sql_gateway_url
        else:
            base_url = self.rest_url
        url = f"{base_url}{endpoint}"
        response = self._http_session.request(method, url, timeout=timeout, **kwargs)
        response.raise_for_status()
        return response.json() if response.content else {}

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
        self.session_id = response.get("sessionHandle")
        return self.session_id

    def list_jobs(self, include_details: bool = False) -> list[dict]:
        """List all jobs.

        Args:
            include_details: If True, fetch full details for each job (including name).
                             This makes an additional API call per job.
        """
        response = self._request("GET", "/jobs")
        jobs = response.get("jobs", [])

        if include_details:
            detailed_jobs = []
            for job in jobs:
                try:
                    details = self._request("GET", f"/jobs/{job['id']}")
                    detailed_jobs.append(details)
                except requests.HTTPError as e:
                    logger.warning(f"Failed to fetch details for job {job['id']}: {e}")
                    detailed_jobs.append(job)
                except Exception as e:
                    logger.error(f"Unexpected error fetching job {job['id']}: {e}")
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
            for suffix in expected_suffixes:
                if flink_job_name.endswith(suffix):
                    return True
            return False

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

    def submit_sql(self, sql: str, statement_timeout: int = STATEMENT_TIMEOUT) -> dict:
        """Submit SQL statements to Flink via SQL Gateway.

        Args:
            sql: SQL statements to execute (can contain multiple statements)
            statement_timeout: Timeout per statement in seconds (default 60)

        Returns:
            Dict with 'results' key containing list of statement results
        """
        session_id = self.get_session()

        # Split SQL into statements
        statements = _split_sql_statements(sql)

        results = []
        for statement in statements:
            # Submit statement
            response = self._request(
                "POST",
                f"/v1/sessions/{session_id}/statements",
                use_sql_gateway=True,
                json={"statement": statement},
            )
            operation_handle = response.get("operationHandle")

            if not operation_handle:
                raise RuntimeError(f"No operationHandle returned for statement: {statement[:50]}...")

            # Poll for completion
            poll_interval = 0.5
            elapsed = 0.0

            while elapsed < statement_timeout:
                status_response = self._request(
                    "GET",
                    f"/v1/sessions/{session_id}/operations/{operation_handle}/status",
                    use_sql_gateway=True,
                )
                status = status_response.get("status")

                if status == "FINISHED":
                    results.append({"status": "FINISHED", "statement": statement[:100]})
                    break
                elif status == "ERROR":
                    # Get error details - try result endpoint first
                    error_msg = status_response.get("error")
                    if not error_msg:
                        # Flink SQL Gateway returns errors in result endpoint (often as 500)
                        try:
                            result_url = f"{self.sql_gateway_url}/v1/sessions/{session_id}/operations/{operation_handle}/result/0"
                            result_resp = self._http_session.get(result_url, timeout=DEFAULT_TIMEOUT)
                            result_data = result_resp.json()
                            error_list = result_data.get("errors", [])
                            error_msg = " ".join(error_list) if error_list else "Unknown error"
                        except Exception as e:
                            error_msg = f"Unknown error (failed to fetch details: {e})"
                    # Use rich error message with suggestions
                    raise RuntimeError(errors.flink_sql_error(error_msg, statement[:200]))
                elif status in ("RUNNING", "PENDING"):
                    time.sleep(poll_interval)
                    elapsed += poll_interval
                else:
                    raise RuntimeError(f"Unknown status '{status}' for statement: {statement[:50]}...")

            if elapsed >= statement_timeout:
                raise RuntimeError(f"Timeout waiting for statement: {statement[:50]}...")

        return {"results": results}

    def cancel_job(self, job_id: str) -> None:
        """Cancel a running job."""
        self._request("PATCH", f"/jobs/{job_id}", json={"state": "cancelled"})

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
            return FlinkJobChange(
                job_name=artifact.name,
                action="none",  # Already running
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
            return "submitted"
        elif change.action == "cancel":
            if change.current and change.current.job_id:
                self.cancel_job(change.current.job_id)
            return "cancelled"
        else:
            return "unchanged"
