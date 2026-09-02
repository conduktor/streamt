"""Unit tests for Schema Registry deployer."""

import json
from unittest.mock import Mock, patch

import pytest

from streamt.compiler.manifest import SchemaArtifact
from streamt.deployer.schema_registry import (
    SchemaChange,
    SchemaRegistryDeployer,
    SchemaRegistryResolutionError,
    SchemaState,
)


class TestSchemaArtifact:
    """Test SchemaArtifact dataclass."""

    def test_basic_artifact(self):
        """Test creating a basic schema artifact."""
        artifact = SchemaArtifact(
            subject="test-value",
            schema={"type": "record", "name": "Test", "fields": []},
        )
        assert artifact.subject == "test-value"
        assert artifact.schema_type == "AVRO"
        assert artifact.compatibility is None

    def test_artifact_with_all_fields(self):
        """Test artifact with all fields specified."""
        artifact = SchemaArtifact(
            subject="orders-value",
            schema={"type": "record", "name": "Order", "fields": []},
            schema_type="JSON",
            compatibility="BACKWARD",
        )
        assert artifact.subject == "orders-value"
        assert artifact.schema_type == "JSON"
        assert artifact.compatibility == "BACKWARD"

    def test_to_dict(self):
        """Test artifact serialization."""
        artifact = SchemaArtifact(
            subject="test-value",
            schema={"type": "record", "name": "Test", "fields": []},
            schema_type="AVRO",
            compatibility="FULL",
        )
        result = artifact.to_dict()
        assert result["subject"] == "test-value"
        assert result["schema"]["type"] == "record"
        assert result["schema_type"] == "AVRO"
        assert result["compatibility"] == "FULL"


class TestSchemaState:
    """Test SchemaState dataclass."""

    def test_non_existent_state(self):
        """Test state for non-existent subject."""
        state = SchemaState(subject="new-topic", exists=False)
        assert state.subject == "new-topic"
        assert state.exists is False
        assert state.version is None

    def test_existing_state(self):
        """Test state for existing subject."""
        state = SchemaState(
            subject="orders-value",
            exists=True,
            version=3,
            schema_id=42,
            schema={"type": "record"},
            schema_type="AVRO",
            compatibility="BACKWARD",
        )
        assert state.exists is True
        assert state.version == 3
        assert state.schema_id == 42


class TestSchemaChange:
    """Test SchemaChange dataclass."""

    def test_register_action(self):
        """Test change for new registration."""
        change = SchemaChange(
            subject="new-topic-value",
            action="register",
        )
        assert change.action == "register"
        assert change.changes == {}

    def test_update_action_with_changes(self):
        """Test change for update with changes dict."""
        change = SchemaChange(
            subject="orders-value",
            action="update",
            changes={"schema": {"from_version": 1, "to_version": 2}},
        )
        assert change.action == "update"
        assert change.changes["schema"]["from_version"] == 1


class TestSchemaRegistryDeployerInit:
    """Test SchemaRegistryDeployer initialization."""

    def test_basic_init(self):
        """Test basic initialization."""
        deployer = SchemaRegistryDeployer("http://localhost:8081")
        assert deployer.url == "http://localhost:8081"
        assert deployer.auth is None
        deployer.close()

    def test_init_with_trailing_slash(self):
        """Test URL normalization."""
        deployer = SchemaRegistryDeployer("http://localhost:8081/")
        assert deployer.url == "http://localhost:8081"
        deployer.close()

    def test_init_with_auth(self):
        """Test initialization with authentication."""
        deployer = SchemaRegistryDeployer(
            "http://localhost:8081",
            username="user",
            password="pass",
        )
        assert deployer.auth == ("user", "pass")
        deployer.close()

    def test_context_manager(self):
        """Test context manager protocol."""
        with SchemaRegistryDeployer("http://localhost:8081") as deployer:
            assert deployer.url == "http://localhost:8081"


class TestSchemaRegistryDeployerMocked:
    """Test SchemaRegistryDeployer with mocked requests."""

    @pytest.fixture
    def deployer(self):
        """Create deployer instance."""
        d = SchemaRegistryDeployer("http://localhost:8081")
        yield d
        d.close()

    @pytest.fixture
    def sample_schema(self):
        """Sample Avro schema."""
        return {
            "type": "record",
            "name": "Order",
            "namespace": "com.example",
            "fields": [
                {"name": "order_id", "type": "string"},
                {"name": "amount", "type": "double"},
            ],
        }

    def test_check_connection_success(self, deployer):
        """Test successful connection check."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = []
        deployer._http_session.request = Mock(return_value=mock_response)

        assert deployer.check_connection() is True
        deployer._http_session.request.assert_called_once()

    def test_check_connection_failure(self, deployer):
        """Test failed connection check."""
        deployer._http_session.request = Mock(side_effect=Exception("Connection refused"))
        assert deployer.check_connection() is False

    def test_list_subjects(self, deployer):
        """Test listing subjects."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = ["topic1-value", "topic2-value"]
        mock_response.raise_for_status = Mock()
        deployer._http_session.request = Mock(return_value=mock_response)

        subjects = deployer.list_subjects()
        assert subjects == ["topic1-value", "topic2-value"]

    def test_get_schema_state_not_found(self, deployer):
        """Test getting state for non-existent subject."""
        mock_response = Mock()
        mock_response.status_code = 404
        deployer._http_session.request = Mock(return_value=mock_response)

        state = deployer.get_schema_state("new-topic-value")
        assert state.exists is False
        assert state.subject == "new-topic-value"

    def test_get_schema_state_exists(self, deployer, sample_schema):
        """Test getting state for existing subject."""
        # First call for schema version
        schema_response = Mock()
        schema_response.status_code = 200
        schema_response.json.return_value = {
            "subject": "orders-value",
            "version": 2,
            "id": 42,
            "schema": json.dumps(sample_schema),
            "schemaType": "AVRO",
        }
        schema_response.raise_for_status = Mock()

        # Second call for compatibility
        compat_response = Mock()
        compat_response.status_code = 200
        compat_response.json.return_value = {"compatibilityLevel": "BACKWARD"}

        deployer._http_session.request = Mock(side_effect=[schema_response, compat_response])

        state = deployer.get_schema_state("orders-value")
        assert state.exists is True
        assert state.version == 2
        assert state.schema_id == 42
        assert state.compatibility == "BACKWARD"

    def test_resolves_specific_version_and_references_read_only(self, deployer, sample_schema):
        """A selected version and all pinned references are resolved with GETs only."""
        referenced_schema = {"type": "record", "name": "Money", "fields": []}
        deployer._request = Mock(
            side_effect=[
                {
                    "subject": "orders/team-value",
                    "version": 3,
                    "id": 42,
                    "schema": json.dumps(sample_schema),
                    "schemaType": "AVRO",
                    "references": [{"name": "money.avsc", "subject": "common/money", "version": 2}],
                },
                {"compatibilityLevel": "FULL_TRANSITIVE"},
                {
                    "subject": "common/money",
                    "version": 2,
                    "id": 7,
                    "schema": json.dumps(referenced_schema),
                    "schemaType": "AVRO",
                },
            ]
        )

        state = deployer.resolve_schema_state("orders/team-value", 3)

        assert state.version == 3
        assert state.compatibility == "FULL_TRANSITIVE"
        assert state.references[0].subject == "common/money"
        assert state.resolved_references[0].schema == referenced_schema
        calls = deployer._request.call_args_list
        assert calls[0].args[:2] == (
            "GET",
            "/subjects/orders%2Fteam-value/versions/3",
        )
        assert calls[2].args[:2] == (
            "GET",
            "/subjects/common%2Fmoney/versions/2",
        )
        assert all(call.args[0] == "GET" for call in calls)

    def test_retains_protobuf_text_while_resolving_metadata(self, deployer):
        """Protobuf content stays raw; version, type, and references remain inspectable."""
        proto = 'syntax = "proto3"; message Order { string id = 1; }'
        deployer._request = Mock(
            side_effect=[
                {
                    "subject": "orders-proto-value",
                    "version": 4,
                    "id": 88,
                    "schema": proto,
                    "schemaType": "PROTOBUF",
                    "references": [],
                },
                {"compatibilityLevel": "BACKWARD"},
            ]
        )

        state = deployer.get_schema_state("orders-proto-value")

        assert state.schema == proto
        assert state.schema_type == "PROTOBUF"
        assert state.version == 4

    def test_decodes_json_schema_content(self, deployer):
        json_schema = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": {"id": {"type": "string"}},
        }
        deployer._request = Mock(
            side_effect=[
                {
                    "subject": "orders-json-value",
                    "version": 2,
                    "id": 89,
                    "schema": json.dumps(json_schema),
                    "schemaType": "JSON",
                },
                {"compatibilityLevel": "BACKWARD"},
            ]
        )

        state = deployer.get_schema_state("orders-json-value", 2)

        assert state.schema == json_schema
        assert state.schema_type == "JSON"

    def test_missing_pinned_reference_is_actionable(self, deployer, sample_schema):
        deployer._request = Mock(
            side_effect=[
                {
                    "subject": "orders-value",
                    "version": 3,
                    "id": 42,
                    "schema": json.dumps(sample_schema),
                    "schemaType": "AVRO",
                    "references": [{"name": "money.avsc", "subject": "common-money", "version": 9}],
                },
                {"compatibilityLevel": "BACKWARD"},
                None,
            ]
        )

        with pytest.raises(
            SchemaRegistryResolutionError,
            match=r"money.avsc.*missing subject 'common-money' version 9",
        ):
            deployer.resolve_schema_state("orders-value")

    def test_register_schema(self, deployer, sample_schema):
        """Test registering a new schema."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"id": 1}
        mock_response.raise_for_status = Mock()
        deployer._http_session.request = Mock(return_value=mock_response)

        schema_id = deployer.register_schema("orders-value", sample_schema)
        assert schema_id == 1

    def test_check_compatibility_compatible(self, deployer, sample_schema):
        """Test compatibility check for compatible schema."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"is_compatible": True}
        mock_response.raise_for_status = Mock()
        deployer._http_session.request = Mock(return_value=mock_response)

        result = deployer.check_compatibility("orders-value", sample_schema)
        assert result is True

    def test_check_compatibility_incompatible(self, deployer, sample_schema):
        """Test compatibility check for incompatible schema."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"is_compatible": False}
        mock_response.raise_for_status = Mock()
        deployer._http_session.request = Mock(return_value=mock_response)

        result = deployer.check_compatibility("orders-value", sample_schema)
        assert result is False

    def test_check_compatibility_no_existing_schema(self, deployer, sample_schema):
        """Test compatibility check when no existing schema."""
        mock_response = Mock()
        mock_response.status_code = 404
        deployer._http_session.request = Mock(return_value=mock_response)

        result = deployer.check_compatibility("new-topic-value", sample_schema)
        assert result is True  # No existing schema means anything is compatible

    def test_set_compatibility(self, deployer):
        """Test setting compatibility level."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.raise_for_status = Mock()
        deployer._http_session.request = Mock(return_value=mock_response)

        deployer.set_compatibility("orders-value", "FULL")
        deployer._http_session.request.assert_called_once()

    def test_delete_subject(self, deployer):
        """Test deleting a subject."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = [1, 2, 3]
        mock_response.raise_for_status = Mock()
        deployer._http_session.request = Mock(return_value=mock_response)

        versions = deployer.delete_subject("orders-value")
        assert versions == [1, 2, 3]

    def test_delete_nonexistent_subject(self, deployer):
        """Test deleting a non-existent subject."""
        mock_response = Mock()
        mock_response.status_code = 404
        deployer._http_session.request = Mock(return_value=mock_response)

        versions = deployer.delete_subject("nonexistent-value")
        assert versions == []


class TestSchemaRegistryDeployerPlanning:
    """Test SchemaRegistryDeployer planning functionality."""

    @pytest.fixture
    def deployer(self):
        """Create deployer instance."""
        d = SchemaRegistryDeployer("http://localhost:8081")
        yield d
        d.close()

    @pytest.fixture
    def sample_schema(self):
        """Sample Avro schema."""
        return {
            "type": "record",
            "name": "Order",
            "fields": [{"name": "order_id", "type": "string"}],
        }

    @patch.object(SchemaRegistryDeployer, "get_schema_state")
    def test_plan_schema_new_registration(self, mock_get_state, deployer, sample_schema):
        """Test planning for new schema registration."""
        mock_get_state.return_value = SchemaState(
            subject="new-topic-value",
            exists=False,
        )

        artifact = SchemaArtifact(
            subject="new-topic-value",
            schema=sample_schema,
        )

        change = deployer.plan_schema(artifact)
        assert change.action == "register"
        assert change.subject == "new-topic-value"

    @patch.object(SchemaRegistryDeployer, "get_schema_state")
    def test_plan_schema_no_changes(self, mock_get_state, deployer, sample_schema):
        """Test planning when schema hasn't changed."""
        mock_get_state.return_value = SchemaState(
            subject="orders-value",
            exists=True,
            version=1,
            schema=sample_schema,  # Same schema
        )

        artifact = SchemaArtifact(
            subject="orders-value",
            schema=sample_schema,
        )

        change = deployer.plan_schema(artifact)
        assert change.action == "none"

    @patch.object(SchemaRegistryDeployer, "check_compatibility")
    @patch.object(SchemaRegistryDeployer, "get_schema_state")
    def test_plan_schema_compatible_update(
        self, mock_get_state, mock_check_compat, deployer, sample_schema
    ):
        """Test planning for compatible schema update."""
        old_schema = {"type": "record", "name": "Order", "fields": []}
        mock_get_state.return_value = SchemaState(
            subject="orders-value",
            exists=True,
            version=1,
            schema=old_schema,
        )
        mock_check_compat.return_value = True

        artifact = SchemaArtifact(
            subject="orders-value",
            schema=sample_schema,  # New schema with field
        )

        change = deployer.plan_schema(artifact)
        assert change.action == "update"
        assert "schema" in change.changes
        assert change.changes["schema"]["compatible"] is True

    @patch.object(SchemaRegistryDeployer, "check_compatibility")
    @patch.object(SchemaRegistryDeployer, "get_schema_state")
    def test_plan_schema_type_transition_is_an_explicit_update(
        self, mock_get_state, mock_check_compat, deployer, sample_schema
    ):
        """A format transition cannot disappear behind an identical JSON body."""
        mock_get_state.return_value = SchemaState(
            subject="orders-value",
            exists=True,
            version=1,
            schema=sample_schema,
            schema_type="AVRO",
        )
        mock_check_compat.return_value = True
        artifact = SchemaArtifact(
            subject="orders-value",
            schema=sample_schema,
            schema_type="JSON",
        )

        change = deployer.plan_schema(artifact)

        assert change.action == "update"
        assert change.changes["schema_type"] == {"from": "AVRO", "to": "JSON"}
        mock_check_compat.assert_called_once_with(
            "orders-value", sample_schema, "JSON"
        )

    @patch.object(SchemaRegistryDeployer, "check_compatibility")
    @patch.object(SchemaRegistryDeployer, "get_schema_state")
    def test_plan_schema_incompatible_update(
        self, mock_get_state, mock_check_compat, deployer, sample_schema
    ):
        """Test planning for incompatible schema update."""
        old_schema = {"type": "record", "name": "Order", "fields": []}
        mock_get_state.return_value = SchemaState(
            subject="orders-value",
            exists=True,
            version=1,
            schema=old_schema,
        )
        mock_check_compat.return_value = False

        artifact = SchemaArtifact(
            subject="orders-value",
            schema=sample_schema,
        )

        change = deployer.plan_schema(artifact)
        assert change.action == "update"
        assert "schema_incompatible" in change.changes


class TestSchemaRegistryDeployerApply:
    """Test SchemaRegistryDeployer apply functionality."""

    @pytest.fixture
    def deployer(self):
        """Create deployer instance."""
        d = SchemaRegistryDeployer("http://localhost:8081")
        yield d
        d.close()

    @pytest.fixture
    def sample_schema(self):
        """Sample Avro schema."""
        return {
            "type": "record",
            "name": "Order",
            "fields": [{"name": "order_id", "type": "string"}],
        }

    @patch.object(SchemaRegistryDeployer, "set_compatibility")
    @patch.object(SchemaRegistryDeployer, "register_schema")
    @patch.object(SchemaRegistryDeployer, "plan_schema")
    def test_apply_schema_register(
        self, mock_plan, mock_register, mock_set_compat, deployer, sample_schema
    ):
        """Test applying new schema registration."""
        mock_plan.return_value = SchemaChange(
            subject="new-topic-value",
            action="register",
        )
        mock_register.return_value = 1

        artifact = SchemaArtifact(
            subject="new-topic-value",
            schema=sample_schema,
        )

        result = deployer.apply_schema(artifact)
        assert result == "registered"
        mock_register.assert_called_once()

    @patch.object(SchemaRegistryDeployer, "plan_schema")
    def test_apply_schema_no_changes(self, mock_plan, deployer, sample_schema):
        """Test applying when no changes needed."""
        mock_plan.return_value = SchemaChange(
            subject="orders-value",
            action="none",
        )

        artifact = SchemaArtifact(
            subject="orders-value",
            schema=sample_schema,
        )

        result = deployer.apply_schema(artifact)
        assert result == "unchanged"

    @patch.object(SchemaRegistryDeployer, "plan_schema")
    def test_apply_schema_incompatible_error(self, mock_plan, deployer, sample_schema):
        """Test applying incompatible schema raises error."""
        mock_plan.return_value = SchemaChange(
            subject="orders-value",
            action="update",
            changes={
                "schema_incompatible": {
                    "message": "Schema is not compatible",
                }
            },
        )

        artifact = SchemaArtifact(
            subject="orders-value",
            schema=sample_schema,
        )

        with pytest.raises(RuntimeError, match="not compatible"):
            deployer.apply_schema(artifact)

    @patch.object(SchemaRegistryDeployer, "set_compatibility")
    @patch.object(SchemaRegistryDeployer, "register_schema")
    @patch.object(SchemaRegistryDeployer, "plan_schema")
    def test_apply_schema_with_compatibility_change(
        self, mock_plan, mock_register, mock_set_compat, deployer, sample_schema
    ):
        """Test applying schema with compatibility level change."""
        mock_plan.return_value = SchemaChange(
            subject="orders-value",
            action="update",
            changes={
                "compatibility": {"from": "BACKWARD", "to": "FULL"},
            },
        )

        artifact = SchemaArtifact(
            subject="orders-value",
            schema=sample_schema,
            compatibility="FULL",
        )

        result = deployer.apply_schema(artifact)
        assert result == "updated"
        mock_set_compat.assert_called_once_with("orders-value", "FULL")

    def test_apply_alias(self, deployer, sample_schema):
        """Test apply() is an alias for apply_schema()."""
        artifact = SchemaArtifact(
            subject="test-value",
            schema=sample_schema,
        )

        with patch.object(deployer, "apply_schema", return_value="registered") as mock_apply:
            result = deployer.apply(artifact)
            assert result == "registered"
            mock_apply.assert_called_once_with(artifact)
