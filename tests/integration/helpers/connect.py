"""Kafka Connect helper for integration tests."""

import time

import requests


class ConnectHelper:
    """Helper class for Kafka Connect operations in tests."""

    def __init__(self, connect_url: str):
        self.connect_url = connect_url

    def list_connectors(self) -> list[str]:
        """List all connectors."""
        response = requests.get(f"{self.connect_url}/connectors")
        response.raise_for_status()
        return response.json()

    def create_connector(self, name: str, config: dict) -> dict:
        """Create a connector."""
        payload = {"name": name, "config": config}
        response = requests.post(
            f"{self.connect_url}/connectors",
            json=payload,
            headers={"Content-Type": "application/json"},
        )
        response.raise_for_status()
        return response.json()

    def delete_connector(self, name: str) -> None:
        """Delete a connector."""
        response = requests.delete(f"{self.connect_url}/connectors/{name}")
        if response.status_code != 404:
            response.raise_for_status()

    def get_connector_status(self, name: str) -> dict:
        """Get connector status."""
        response = requests.get(f"{self.connect_url}/connectors/{name}/status")
        response.raise_for_status()
        return response.json()

    def wait_for_connector_running(
        self,
        name: str,
        timeout: int = 60,
        require_tasks: bool = True,
    ) -> bool:
        """Wait for connector AND its tasks to be in RUNNING state.

        Args:
            name: Connector name
            timeout: Maximum time to wait in seconds
            require_tasks: If True, also wait for at least one task to be RUNNING

        Returns:
            True if connector (and tasks if required) are running, False on timeout
        """
        start = time.time()
        last_error = None
        while time.time() - start < timeout:
            try:
                status = self.get_connector_status(name)
                connector_state = status.get("connector", {}).get("state")

                if connector_state == "FAILED":
                    trace = status.get("connector", {}).get("trace", "No trace")
                    raise RuntimeError(f"Connector '{name}' FAILED: {trace[:500]}")

                if connector_state == "RUNNING":
                    if not require_tasks:
                        return True
                    # Also check tasks are running
                    tasks = status.get("tasks", [])
                    if len(tasks) > 0:
                        running_tasks = [t for t in tasks if t.get("state") == "RUNNING"]
                        if len(running_tasks) > 0:
                            return True
                        # Check for failed tasks
                        failed_tasks = [t for t in tasks if t.get("state") == "FAILED"]
                        if failed_tasks:
                            trace = failed_tasks[0].get("trace", "No trace")
                            raise RuntimeError(f"Connector '{name}' task FAILED: {trace[:500]}")
            except RuntimeError:
                raise  # Re-raise explicit failures
            except requests.exceptions.RequestException as e:
                last_error = e  # Track for timeout message
            time.sleep(2)

        error_msg = f"Connector '{name}' did not reach RUNNING state within {timeout}s"
        if last_error:
            error_msg += f". Last error: {last_error}"
        return False
