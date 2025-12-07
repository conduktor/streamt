"""Schema Registry helper for integration tests."""

import json
from typing import Optional

import requests

from .retry import retry_on_transient_error


class SchemaRegistryHelper:
    """Helper class for Schema Registry operations in tests."""

    def __init__(self, url: str):
        self.url = url

    def check_connection(self) -> bool:
        """Check if Schema Registry is available."""
        try:
            response = requests.get(f"{self.url}/subjects", timeout=5)
            return response.status_code == 200
        except Exception:
            return False

    @retry_on_transient_error(max_retries=3, delay=1.0)
    def list_subjects(self) -> list[str]:
        """List all registered subjects."""
        response = requests.get(f"{self.url}/subjects", timeout=10)
        response.raise_for_status()
        return response.json()

    @retry_on_transient_error(max_retries=3, delay=1.0)
    def register_schema(
        self,
        subject: str,
        schema: dict,
        schema_type: str = "AVRO",
    ) -> int:
        """Register a schema and return the schema ID."""
        payload = {
            "schema": json.dumps(schema) if isinstance(schema, dict) else schema,
            "schemaType": schema_type,
        }
        response = requests.post(
            f"{self.url}/subjects/{subject}/versions",
            json=payload,
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
            timeout=10,
        )
        response.raise_for_status()
        return response.json()["id"]

    @retry_on_transient_error(max_retries=3, delay=1.0)
    def get_schema(self, subject: str, version: str = "latest") -> dict:
        """Get a schema by subject and version."""
        response = requests.get(f"{self.url}/subjects/{subject}/versions/{version}", timeout=10)
        response.raise_for_status()
        return response.json()

    @retry_on_transient_error(max_retries=3, delay=1.0)
    def get_schema_by_id(self, schema_id: int) -> dict:
        """Get a schema by its global ID."""
        response = requests.get(f"{self.url}/schemas/ids/{schema_id}", timeout=10)
        response.raise_for_status()
        return response.json()

    def delete_subject(self, subject: str, permanent: bool = False) -> list[int]:
        """Delete a subject (soft delete by default)."""
        url = f"{self.url}/subjects/{subject}"
        if permanent:
            url += "?permanent=true"
        response = requests.delete(url)
        if response.status_code == 404:
            return []
        response.raise_for_status()
        return response.json()

    def check_compatibility(
        self,
        subject: str,
        schema: dict,
        schema_type: str = "AVRO",
    ) -> bool:
        """Check if a schema is compatible with existing versions."""
        payload = {
            "schema": json.dumps(schema) if isinstance(schema, dict) else schema,
            "schemaType": schema_type,
        }
        response = requests.post(
            f"{self.url}/compatibility/subjects/{subject}/versions/latest",
            json=payload,
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        )
        if response.status_code == 404:
            # No existing schema, anything is compatible
            return True
        response.raise_for_status()
        return response.json().get("is_compatible", False)

    def get_compatibility_level(self, subject: Optional[str] = None) -> str:
        """Get compatibility level for a subject or global default."""
        if subject:
            url = f"{self.url}/config/{subject}"
        else:
            url = f"{self.url}/config"
        response = requests.get(url)
        response.raise_for_status()
        return response.json().get("compatibilityLevel", "BACKWARD")

    def set_compatibility_level(
        self,
        level: str,
        subject: Optional[str] = None,
    ) -> None:
        """Set compatibility level for a subject or global default."""
        if subject:
            url = f"{self.url}/config/{subject}"
        else:
            url = f"{self.url}/config"
        response = requests.put(
            url,
            json={"compatibility": level},
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        )
        response.raise_for_status()
