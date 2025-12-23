"""Flink SQL helper for integration tests."""

import time
from typing import Optional

import requests

from .config import FlinkConfig
from .polling import poll_for_result
from .retry import retry_on_transient_error


class FlinkHelper:
    """Helper class for Flink SQL operations in tests.

    Can be initialized with either:
    - Two separate URLs: FlinkHelper(rest_url, sql_gateway_url)
    - A FlinkConfig object: FlinkHelper(config=flink_config)
    """

    def __init__(
        self,
        rest_url: Optional[str] = None,
        sql_gateway_url: Optional[str] = None,
        *,
        config: Optional[FlinkConfig] = None,
    ):
        if config is not None:
            self.rest_url = config.rest_url
            self.sql_gateway_url = config.sql_gateway_url
        else:
            self.rest_url = rest_url
            self.sql_gateway_url = sql_gateway_url
        self.session_handle: Optional[str] = None

    @retry_on_transient_error(max_retries=3, delay=1.0)
    def get_cluster_overview(self) -> dict:
        """Get Flink cluster overview."""
        response = requests.get(f"{self.rest_url}/overview", timeout=10)
        response.raise_for_status()
        return response.json()

    @retry_on_transient_error(max_retries=3, delay=1.0)
    def list_jobs(self) -> list[dict]:
        """List all jobs."""
        response = requests.get(f"{self.rest_url}/jobs", timeout=10)
        response.raise_for_status()
        return response.json().get("jobs", [])

    def cancel_job(self, job_id: str) -> bool:
        """Cancel a job. Returns True if successful."""
        response = requests.patch(f"{self.rest_url}/jobs/{job_id}?mode=cancel")
        return response.status_code in [200, 202]

    def get_job_status(self, job_id: str) -> Optional[str]:
        """Get job status by ID. Returns None if job not found."""
        try:
            response = requests.get(f"{self.rest_url}/jobs/{job_id}")
            if response.status_code == 200:
                return response.json().get("state")
            return None
        except Exception:
            return None

    def wait_for_job_status(
        self,
        job_id: str,
        expected_status: str,
        timeout: int = 30,
    ) -> bool:
        """Wait for a job to reach expected status."""
        def check() -> tuple[bool, bool]:
            status = self.get_job_status(job_id)
            if status == expected_status:
                return True, True
            if status in ["FAILED", "CANCELED", "FINISHED"]:
                return True, status == expected_status
            return False, False

        result = poll_for_result(check, timeout=timeout, interval=1.0)
        return result if result is not None else False

    def get_running_jobs(self) -> list[dict]:
        """Get all currently running jobs."""
        jobs = self.list_jobs()
        return [j for j in jobs if j.get("status") == "RUNNING"]

    def cancel_all_running_jobs(self) -> int:
        """Cancel all running jobs. Returns count of cancelled jobs."""
        running_jobs = self.get_running_jobs()
        cancelled = 0
        for job in running_jobs:
            if self.cancel_job(job["id"]):
                cancelled += 1
        if cancelled > 0:
            time.sleep(2)
        return cancelled

    def wait_for_new_running_job(
        self,
        known_job_ids: set[str],
        timeout: float = 30.0,
        interval: float = 1.0,
    ) -> Optional[str]:
        """Wait for a new running job to appear."""
        def check() -> tuple[bool, Optional[str]]:
            current_jobs = self.get_running_jobs()
            for job in current_jobs:
                if job["id"] not in known_job_ids:
                    return True, job["id"]
            return False, None

        return poll_for_result(check, timeout=timeout, interval=interval)

    @retry_on_transient_error(max_retries=3, delay=1.0)
    def open_sql_session(self) -> str:
        """Open a SQL Gateway session."""
        response = requests.post(
            f"{self.sql_gateway_url}/v1/sessions",
            json={},
            timeout=10,
        )
        response.raise_for_status()
        self.session_handle = response.json().get("sessionHandle")
        return self.session_handle

    def close_sql_session(self) -> None:
        """Close the SQL Gateway session."""
        if self.session_handle:
            try:
                requests.delete(
                    f"{self.sql_gateway_url}/v1/sessions/{self.session_handle}",
                    timeout=5,
                )
            except Exception:
                pass
            self.session_handle = None

    @retry_on_transient_error(max_retries=3, delay=1.0)
    def execute_sql(self, statement: str) -> dict:
        """Execute a SQL statement via SQL Gateway."""
        if not self.session_handle:
            self.open_sql_session()

        response = requests.post(
            f"{self.sql_gateway_url}/v1/sessions/{self.session_handle}/statements",
            json={"statement": statement},
            timeout=30,
        )
        response.raise_for_status()
        return response.json()

    def get_statement_result(
        self,
        operation_handle: str,
        timeout: int = 60,
    ) -> dict:
        """Get the result of a statement execution."""
        def check() -> tuple[bool, Optional[dict]]:
            response = requests.get(
                f"{self.sql_gateway_url}/v1/sessions/{self.session_handle}/operations/{operation_handle}/result/0"
            )
            if response.status_code == 200:
                return True, response.json()
            return False, None

        result = poll_for_result(check, timeout=timeout, interval=1.0)
        if result is None:
            raise TimeoutError("Statement did not complete in time")
        return result

    def execute_sql_and_check_error(
        self,
        statement: str,
        timeout: float = 10.0,
    ) -> Optional[str]:
        """Execute SQL and return error message if any, None if successful.

        Flink SQL Gateway is async - errors appear when fetching results.
        This method submits the statement and polls for the result to detect errors.
        """
        if not self.session_handle:
            self.open_sql_session()

        # Submit the statement
        response = requests.post(
            f"{self.sql_gateway_url}/v1/sessions/{self.session_handle}/statements",
            json={"statement": statement},
            timeout=30,
        )
        response.raise_for_status()
        op_handle = response.json().get("operationHandle")

        def check() -> tuple[bool, Optional[str]]:
            result_response = requests.get(
                f"{self.sql_gateway_url}/v1/sessions/{self.session_handle}/operations/{op_handle}/result/0"
            )
            if result_response.status_code != 200:
                # Got an error response
                try:
                    error_data = result_response.json()
                    errors = error_data.get("errors", [])
                    return True, " ".join(errors) if errors else result_response.text
                except Exception:
                    return True, result_response.text
            # 200 means still in progress or completed - check for completion
            result_data = result_response.json()
            if result_data.get("resultType") in ["PAYLOAD", "EOS"]:
                # Successfully completed
                return True, None
            return False, None

        return poll_for_result(check, timeout=timeout, interval=0.5)
