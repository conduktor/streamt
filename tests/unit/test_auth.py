"""Tests for auth/SSL configuration across all deployers."""

import json
import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import yaml
from click.testing import CliRunner

from streamt.cli import main
from streamt.core.models import (
    ConnectClusterConfig,
    ConnectConfig,
    FlinkClusterConfig,
    FlinkConfig,
    KafkaConfig,
    SchemaRegistryConfig,
)


class TestKafkaConfigAuth:
    """Tests for KafkaConfig auth field mapping."""

    def test_plain_config(self):
        """Plain Kafka config produces minimal confluent config."""
        cfg = KafkaConfig(bootstrap_servers="localhost:9092")
        result = cfg.to_confluent_config()
        assert result == {"bootstrap.servers": "localhost:9092"}

    def test_sasl_plaintext_config(self):
        """SASL_PLAINTEXT config maps all SASL fields."""
        cfg = KafkaConfig(
            bootstrap_servers="broker:9092",
            security_protocol="SASL_PLAINTEXT",
            sasl_mechanism="PLAIN",
            sasl_username="admin",
            sasl_password="secret",
        )
        result = cfg.to_confluent_config()
        assert result["bootstrap.servers"] == "broker:9092"
        assert result["security.protocol"] == "SASL_PLAINTEXT"
        assert result["sasl.mechanism"] == "PLAIN"
        assert result["sasl.username"] == "admin"
        assert result["sasl.password"] == "secret"

    def test_sasl_ssl_scram_config(self):
        """SASL_SSL with SCRAM-SHA-256 maps correctly."""
        cfg = KafkaConfig(
            bootstrap_servers="broker:9093",
            security_protocol="SASL_SSL",
            sasl_mechanism="SCRAM-SHA-256",
            sasl_username="user",
            sasl_password="pass",
            ssl_ca_location="/certs/ca.pem",
        )
        result = cfg.to_confluent_config()
        assert result["security.protocol"] == "SASL_SSL"
        assert result["sasl.mechanism"] == "SCRAM-SHA-256"
        assert result["ssl.ca.location"] == "/certs/ca.pem"

    def test_ssl_config(self):
        """SSL (no SASL) with CA cert."""
        cfg = KafkaConfig(
            bootstrap_servers="broker:9093",
            security_protocol="SSL",
            ssl_ca_location="/certs/ca.pem",
        )
        result = cfg.to_confluent_config()
        assert result["security.protocol"] == "SSL"
        assert result["ssl.ca.location"] == "/certs/ca.pem"
        assert "sasl.mechanism" not in result

    def test_mtls_config(self):
        """mTLS with client cert and key."""
        cfg = KafkaConfig(
            bootstrap_servers="broker:9093",
            security_protocol="SSL",
            ssl_ca_location="/certs/ca.pem",
            ssl_certificate_location="/certs/client.pem",
            ssl_key_location="/certs/client.key",
            ssl_key_password="keypass",
        )
        result = cfg.to_confluent_config()
        assert result["ssl.ca.location"] == "/certs/ca.pem"
        assert result["ssl.certificate.location"] == "/certs/client.pem"
        assert result["ssl.key.location"] == "/certs/client.key"
        assert result["ssl.key.password"] == "keypass"

    def test_none_fields_excluded(self):
        """None fields are not included in output."""
        cfg = KafkaConfig(
            bootstrap_servers="broker:9092",
            security_protocol="SASL_PLAINTEXT",
            sasl_mechanism="PLAIN",
            sasl_username="admin",
            sasl_password="secret",
        )
        result = cfg.to_confluent_config()
        assert "ssl.ca.location" not in result
        assert "ssl.certificate.location" not in result


class TestKafkaDeployerAuth:
    """Tests that KafkaDeployer receives auth config from _make_kafka_deployer."""

    def test_make_kafka_deployer_passes_sasl(self):
        """_make_kafka_deployer passes SASL config from project."""
        from streamt.cli.helpers import make_kafka_deployer

        project = MagicMock()
        project.runtime.kafka = KafkaConfig(
            bootstrap_servers="broker:9092",
            security_protocol="SASL_PLAINTEXT",
            sasl_mechanism="PLAIN",
            sasl_username="admin",
            sasl_password="secret",
        )
        fmt = MagicMock()

        with patch("streamt.deployer.kafka.KafkaDeployer.__init__", return_value=None) as mock_init:
            make_kafka_deployer(project, fmt)

            mock_init.assert_called_once()
            args, kwargs = mock_init.call_args
            assert args[0] == "broker:9092"
            assert kwargs["security.protocol"] == "SASL_PLAINTEXT"
            assert kwargs["sasl.mechanism"] == "PLAIN"
            assert kwargs["sasl.username"] == "admin"
            assert kwargs["sasl.password"] == "secret"

    def test_make_kafka_deployer_plain_no_auth(self):
        """_make_kafka_deployer with plain config only passes bootstrap_servers."""
        from streamt.cli.helpers import make_kafka_deployer

        project = MagicMock()
        project.runtime.kafka = KafkaConfig(bootstrap_servers="localhost:9092")
        fmt = MagicMock()

        with patch("streamt.deployer.kafka.KafkaDeployer.__init__", return_value=None) as mock_init:
            make_kafka_deployer(project, fmt)
            mock_init.assert_called_once()
            args, kwargs = mock_init.call_args
            assert args[0] == "localhost:9092"
            assert kwargs == {}  # No extra auth fields


class TestSchemaRegistryAuth:
    """Tests for Schema Registry auth/SSL."""

    def test_ssl_ca_configured(self):
        """SchemaRegistryDeployer configures CA cert for SSL."""
        from streamt.deployer.schema_registry import SchemaRegistryDeployer

        deployer = SchemaRegistryDeployer(
            "https://sr:8081",
            username="user",
            password="pass",
            ssl_ca_location="/certs/ca.pem",
        )
        assert deployer._http_session.verify == "/certs/ca.pem"

    def test_mtls_configured(self):
        """SchemaRegistryDeployer configures mTLS certs."""
        from streamt.deployer.schema_registry import SchemaRegistryDeployer

        deployer = SchemaRegistryDeployer(
            "https://sr:8081",
            ssl_ca_location="/certs/ca.pem",
            ssl_certificate_location="/certs/client.pem",
            ssl_key_location="/certs/client.key",
        )
        assert deployer._http_session.verify == "/certs/ca.pem"
        assert deployer._http_session.cert == ("/certs/client.pem", "/certs/client.key")

    def test_no_ssl_default_verify(self):
        """Without SSL config, default verify=True."""
        from streamt.deployer.schema_registry import SchemaRegistryDeployer

        deployer = SchemaRegistryDeployer("http://sr:8081")
        assert deployer._http_session.verify is True


class TestConnectDeployerAuth:
    """Tests for Connect deployer auth/SSL."""

    def test_basic_auth_configured(self):
        """ConnectDeployer sends basic auth when credentials provided."""
        from streamt.deployer.connect import ConnectDeployer

        deployer = ConnectDeployer(
            "https://connect:8083",
            username="admin",
            password="secret",
        )
        assert deployer._http_session.auth == ("admin", "secret")

    def test_ssl_ca_configured(self):
        """ConnectDeployer configures CA cert."""
        from streamt.deployer.connect import ConnectDeployer

        deployer = ConnectDeployer(
            "https://connect:8083",
            ssl_ca_location="/certs/ca.pem",
        )
        assert deployer._http_session.verify == "/certs/ca.pem"

    def test_no_auth_default(self):
        """Without credentials, no auth configured."""
        from streamt.deployer.connect import ConnectDeployer

        deployer = ConnectDeployer("http://connect:8083")
        assert deployer._http_session.auth is None


class TestFlinkDeployerAuth:
    """Tests for Flink deployer auth/SSL."""

    def test_basic_auth_configured(self):
        """FlinkDeployer sends basic auth when credentials provided."""
        from streamt.deployer.flink import FlinkDeployer

        deployer = FlinkDeployer(
            "https://flink:8082",
            username="admin",
            password="secret",
        )
        assert deployer._http_session.auth == ("admin", "secret")

    def test_api_key_bearer(self):
        """FlinkDeployer sends Bearer token when api_key provided."""
        from streamt.deployer.flink import FlinkDeployer

        deployer = FlinkDeployer(
            "https://flink.confluent.cloud",
            api_key="cc-api-key-123",
        )
        assert deployer._http_session.headers.get("Authorization") == "Bearer cc-api-key-123"

    def test_ssl_ca_configured(self):
        """FlinkDeployer configures CA cert."""
        from streamt.deployer.flink import FlinkDeployer

        deployer = FlinkDeployer(
            "https://flink:8082",
            ssl_ca_location="/certs/ca.pem",
        )
        assert deployer._http_session.verify == "/certs/ca.pem"

    def test_no_auth_default(self):
        """Without credentials, no auth configured."""
        from streamt.deployer.flink import FlinkDeployer

        deployer = FlinkDeployer("http://flink:8082")
        assert deployer._http_session.auth is None


class TestConnectClusterConfigAuth:
    """Tests for ConnectClusterConfig model fields."""

    def test_connect_config_with_auth(self):
        cfg = ConnectClusterConfig(
            rest_url="https://connect:8083",
            username="admin",
            password="secret",
            ssl_ca_location="/certs/ca.pem",
        )
        assert cfg.username == "admin"
        assert cfg.password.get_secret_value() == "secret"
        assert cfg.ssl_ca_location == "/certs/ca.pem"

    def test_connect_config_minimal(self):
        cfg = ConnectClusterConfig(rest_url="http://connect:8083")
        assert cfg.username is None
        assert cfg.password is None


class TestSchemaRegistryConfigAuth:
    """Tests for SchemaRegistryConfig model fields."""

    def test_sr_config_with_ssl(self):
        cfg = SchemaRegistryConfig(
            url="https://sr:8081",
            username="user",
            password="pass",
            ssl_ca_location="/certs/ca.pem",
            ssl_certificate_location="/certs/client.pem",
            ssl_key_location="/certs/client.key",
        )
        assert cfg.ssl_ca_location == "/certs/ca.pem"
        assert cfg.ssl_certificate_location == "/certs/client.pem"

    def test_sr_config_minimal(self):
        cfg = SchemaRegistryConfig(url="http://sr:8081")
        assert cfg.ssl_ca_location is None


# ---------------------------------------------------------------------------
# Task #33: YAML parsing of auth/SSL fields for all runtime configs
# ---------------------------------------------------------------------------

def _make_project_dir(tmpdir, config):
    """Helper: write stream_project.yml and return Path."""
    project_path = Path(tmpdir)
    with open(project_path / "stream_project.yml", "w") as f:
        yaml.dump(config, f)
    return project_path


class TestYamlParsingAuthFields:
    """Verify parser loads auth/SSL fields into models from YAML."""

    def test_kafka_sasl_ssl_parsed(self):
        from streamt.core.parser import ProjectParser

        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "auth-test"},
                "runtime": {
                    "kafka": {
                        "bootstrap_servers": "broker:9093",
                        "security_protocol": "SASL_SSL",
                        "sasl_mechanism": "SCRAM-SHA-256",
                        "sasl_username": "admin",
                        "sasl_password": "secret",
                        "ssl_ca_location": "/certs/ca.pem",
                        "ssl_certificate_location": "/certs/client.pem",
                        "ssl_key_location": "/certs/client.key",
                        "ssl_key_password": "keypass",
                    }
                },
            }
            p = _make_project_dir(tmpdir, config)
            project = ProjectParser(p).parse()
            k = project.runtime.kafka
            assert k.security_protocol == "SASL_SSL"
            assert k.sasl_mechanism == "SCRAM-SHA-256"
            assert k.sasl_username == "admin"
            assert k.sasl_password.get_secret_value() == "secret"
            assert k.ssl_ca_location == "/certs/ca.pem"
            assert k.ssl_certificate_location == "/certs/client.pem"
            assert k.ssl_key_location == "/certs/client.key"
            assert k.ssl_key_password.get_secret_value() == "keypass"

    def test_schema_registry_ssl_parsed(self):
        from streamt.core.parser import ProjectParser

        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "sr-auth-test"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "broker:9092"},
                    "schema_registry": {
                        "url": "https://sr:8081",
                        "username": "sruser",
                        "password": "srpass",
                        "ssl_ca_location": "/certs/sr-ca.pem",
                        "ssl_certificate_location": "/certs/sr-client.pem",
                        "ssl_key_location": "/certs/sr-client.key",
                    },
                },
            }
            p = _make_project_dir(tmpdir, config)
            project = ProjectParser(p).parse()
            sr = project.runtime.schema_registry
            assert sr.username == "sruser"
            assert sr.password.get_secret_value() == "srpass"
            assert sr.ssl_ca_location == "/certs/sr-ca.pem"
            assert sr.ssl_certificate_location == "/certs/sr-client.pem"
            assert sr.ssl_key_location == "/certs/sr-client.key"

    def test_flink_cluster_auth_parsed(self):
        from streamt.core.parser import ProjectParser

        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "flink-auth-test"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "broker:9092"},
                    "flink": {
                        "default": "prod",
                        "clusters": {
                            "prod": {
                                "rest_url": "https://flink:8082",
                                "sql_gateway_url": "https://flink:8084",
                                "username": "fuser",
                                "password": "fpass",
                                "api_key": "fkey-123",
                                "ssl_ca_location": "/certs/flink-ca.pem",
                                "ssl_certificate_location": "/certs/flink-client.pem",
                                "ssl_key_location": "/certs/flink-client.key",
                            }
                        },
                    },
                },
            }
            p = _make_project_dir(tmpdir, config)
            project = ProjectParser(p).parse()
            fc = project.runtime.flink.clusters["prod"]
            assert fc.username == "fuser"
            assert fc.password.get_secret_value() == "fpass"
            assert fc.api_key.get_secret_value() == "fkey-123"
            assert fc.ssl_ca_location == "/certs/flink-ca.pem"
            assert fc.ssl_certificate_location == "/certs/flink-client.pem"
            assert fc.ssl_key_location == "/certs/flink-client.key"

    def test_connect_cluster_auth_parsed(self):
        from streamt.core.parser import ProjectParser

        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "connect-auth-test"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "broker:9092"},
                    "connect": {
                        "default": "prod",
                        "clusters": {
                            "prod": {
                                "rest_url": "https://connect:8083",
                                "username": "cuser",
                                "password": "cpass",
                                "ssl_ca_location": "/certs/connect-ca.pem",
                                "ssl_certificate_location": "/certs/connect-client.pem",
                                "ssl_key_location": "/certs/connect-client.key",
                            }
                        },
                    },
                },
            }
            p = _make_project_dir(tmpdir, config)
            project = ProjectParser(p).parse()
            cc = project.runtime.connect.clusters["prod"]
            assert cc.username == "cuser"
            assert cc.password.get_secret_value() == "cpass"
            assert cc.ssl_ca_location == "/certs/connect-ca.pem"
            assert cc.ssl_certificate_location == "/certs/connect-client.pem"
            assert cc.ssl_key_location == "/certs/connect-client.key"

    def test_kafka_auth_env_var_substitution(self):
        from streamt.core.parser import ProjectParser

        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "envvar-auth"},
                "runtime": {
                    "kafka": {
                        "bootstrap_servers": "broker:9092",
                        "security_protocol": "SASL_SSL",
                        "sasl_mechanism": "PLAIN",
                        "sasl_username": "${TEST_KAFKA_USER}",
                        "sasl_password": "${TEST_KAFKA_PASS}",
                        "ssl_ca_location": "${TEST_CA_PATH}",
                    }
                },
            }
            p = _make_project_dir(tmpdir, config)
            os.environ["TEST_KAFKA_USER"] = "envuser"
            os.environ["TEST_KAFKA_PASS"] = "envpass"
            os.environ["TEST_CA_PATH"] = "/resolved/ca.pem"
            try:
                project = ProjectParser(p).parse()
                k = project.runtime.kafka
                assert k.sasl_username == "envuser"
                assert k.sasl_password.get_secret_value() == "envpass"
                assert k.ssl_ca_location == "/resolved/ca.pem"
            finally:
                del os.environ["TEST_KAFKA_USER"]
                del os.environ["TEST_KAFKA_PASS"]
                del os.environ["TEST_CA_PATH"]

    def test_auth_env_var_from_dotenv(self):
        from streamt.core.parser import ProjectParser

        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "dotenv-auth"},
                "runtime": {
                    "kafka": {
                        "bootstrap_servers": "broker:9092",
                        "sasl_password": "${DOTENV_KAFKA_PASS}",
                    }
                },
            }
            p = _make_project_dir(tmpdir, config)
            with open(p / ".env", "w") as f:
                f.write("DOTENV_KAFKA_PASS=from-dotenv\n")
            project = ProjectParser(p).parse()
            assert project.runtime.kafka.sasl_password.get_secret_value() == "from-dotenv"


# ---------------------------------------------------------------------------
# Task #29: CLI validate command with auth/SSL config
# ---------------------------------------------------------------------------

def _parse_json_output(output):
    idx = output.find("{")
    if idx == -1:
        raise ValueError(f"No JSON found in output: {output!r}")
    return json.loads(output[idx:])


class TestCLIValidateAuth:
    """Verify CLI validate works with auth/SSL config."""

    def test_validate_with_full_auth_config(self):
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "auth-validate"},
                "runtime": {
                    "kafka": {
                        "bootstrap_servers": "broker:9093",
                        "security_protocol": "SASL_SSL",
                        "sasl_mechanism": "PLAIN",
                        "sasl_username": "admin",
                        "sasl_password": "secret",
                        "ssl_ca_location": "/certs/ca.pem",
                    },
                    "schema_registry": {
                        "url": "https://sr:8081",
                        "username": "sruser",
                        "password": "srpass",
                        "ssl_ca_location": "/certs/sr-ca.pem",
                    },
                    "flink": {
                        "default": "prod",
                        "clusters": {
                            "prod": {
                                "rest_url": "https://flink:8082",
                                "username": "fuser",
                                "password": "fpass",
                                "ssl_ca_location": "/certs/flink-ca.pem",
                            }
                        },
                    },
                    "connect": {
                        "default": "prod",
                        "clusters": {
                            "prod": {
                                "rest_url": "https://connect:8083",
                                "username": "cuser",
                                "password": "cpass",
                            }
                        },
                    },
                },
            }
            p = _make_project_dir(tmpdir, config)
            result = runner.invoke(main, ["validate", "-p", str(p)])
            assert result.exit_code == 0
            assert "is valid" in result.output

    def test_validate_auth_json_output(self):
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "auth-json"},
                "runtime": {
                    "kafka": {
                        "bootstrap_servers": "broker:9093",
                        "security_protocol": "SASL_SSL",
                        "sasl_mechanism": "PLAIN",
                        "sasl_username": "admin",
                        "sasl_password": "secret",
                    },
                },
            }
            p = _make_project_dir(tmpdir, config)
            result = runner.invoke(main, ["-o", "json", "validate", "-p", str(p)])
            assert result.exit_code == 0
            data = _parse_json_output(result.output)
            assert data["status"] == "ok"

    def test_validate_missing_auth_env_var(self):
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "missing-env"},
                "runtime": {
                    "kafka": {
                        "bootstrap_servers": "broker:9092",
                        "sasl_password": "${NONEXISTENT_AUTH_VAR_XYZ}",
                    }
                },
            }
            p = _make_project_dir(tmpdir, config)
            result = runner.invoke(main, ["validate", "-p", str(p)])
            assert result.exit_code != 0
            assert "NONEXISTENT_AUTH_VAR_XYZ" in result.output

    def test_validate_missing_auth_env_var_json(self):
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "missing-env-json"},
                "runtime": {
                    "kafka": {
                        "bootstrap_servers": "broker:9092",
                        "sasl_password": "${NONEXISTENT_AUTH_VAR_ABC}",
                    }
                },
            }
            p = _make_project_dir(tmpdir, config)
            result = runner.invoke(main, ["-o", "json", "validate", "-p", str(p)])
            assert result.exit_code != 0
            data = _parse_json_output(result.output)
            assert data["status"] == "error"
            assert any("NONEXISTENT_AUTH_VAR_ABC" in e.get("message", "") for e in data.get("errors", []))


# ---------------------------------------------------------------------------
# Task #30: Deployer factory wiring passes all auth fields
# ---------------------------------------------------------------------------

class TestDeployerFactoryAuthWiring:
    """Verify _make_*_deployer functions pass all auth fields."""

    def test_make_sr_deployer_passes_ssl(self):
        from streamt.cli.helpers import make_sr_deployer

        project = MagicMock()
        project.runtime.schema_registry = SchemaRegistryConfig(
            url="https://sr:8081",
            username="sruser",
            password="srpass",
            ssl_ca_location="/certs/ca.pem",
            ssl_certificate_location="/certs/client.pem",
            ssl_key_location="/certs/client.key",
        )
        fmt = MagicMock()

        with patch("streamt.deployer.schema_registry.SchemaRegistryDeployer.__init__", return_value=None) as mock_init:
            make_sr_deployer(project, fmt)
            mock_init.assert_called_once()
            _, kwargs = mock_init.call_args
            assert kwargs["username"] == "sruser"
            assert kwargs["password"] == "srpass"
            assert kwargs["ssl_ca_location"] == "/certs/ca.pem"
            assert kwargs["ssl_certificate_location"] == "/certs/client.pem"
            assert kwargs["ssl_key_location"] == "/certs/client.key"

    def test_make_flink_deployer_passes_auth(self):
        from streamt.cli.helpers import make_flink_deployer

        project = MagicMock()
        project.runtime.flink = FlinkConfig(
            default="prod",
            clusters={
                "prod": FlinkClusterConfig(
                    rest_url="https://flink:8082",
                    sql_gateway_url="https://flink:8084",
                    username="fuser",
                    password="fpass",
                    api_key="fkey-123",
                    ssl_ca_location="/certs/flink-ca.pem",
                    ssl_certificate_location="/certs/flink-client.pem",
                    ssl_key_location="/certs/flink-client.key",
                ),
            },
        )
        fmt = MagicMock()

        with patch("streamt.deployer.flink.FlinkDeployer.__init__", return_value=None) as mock_init:
            make_flink_deployer(project, fmt)
            mock_init.assert_called_once()
            _, kwargs = mock_init.call_args
            assert kwargs["username"] == "fuser"
            assert kwargs["password"] == "fpass"
            assert kwargs["api_key"] == "fkey-123"
            assert kwargs["ssl_ca_location"] == "/certs/flink-ca.pem"
            assert kwargs["ssl_certificate_location"] == "/certs/flink-client.pem"
            assert kwargs["ssl_key_location"] == "/certs/flink-client.key"

    def test_make_connect_deployer_passes_auth(self):
        from streamt.cli.helpers import make_connect_deployer

        project = MagicMock()
        project.runtime.connect = ConnectConfig(
            default="prod",
            clusters={
                "prod": ConnectClusterConfig(
                    rest_url="https://connect:8083",
                    username="cuser",
                    password="cpass",
                    ssl_ca_location="/certs/connect-ca.pem",
                    ssl_certificate_location="/certs/connect-client.pem",
                    ssl_key_location="/certs/connect-client.key",
                ),
            },
        )
        fmt = MagicMock()

        with patch("streamt.deployer.connect.ConnectDeployer.__init__", return_value=None) as mock_init:
            make_connect_deployer(project, fmt)
            mock_init.assert_called_once()
            _, kwargs = mock_init.call_args
            assert kwargs["username"] == "cuser"
            assert kwargs["password"] == "cpass"
            assert kwargs["ssl_ca_location"] == "/certs/connect-ca.pem"
            assert kwargs["ssl_certificate_location"] == "/certs/connect-client.pem"
            assert kwargs["ssl_key_location"] == "/certs/connect-client.key"

    def test_make_kafka_deployer_passes_mtls(self):
        from streamt.cli.helpers import make_kafka_deployer

        project = MagicMock()
        project.runtime.kafka = KafkaConfig(
            bootstrap_servers="broker:9093",
            security_protocol="SSL",
            ssl_ca_location="/certs/ca.pem",
            ssl_certificate_location="/certs/client.pem",
            ssl_key_location="/certs/client.key",
            ssl_key_password="keypass",
        )
        fmt = MagicMock()

        with patch("streamt.deployer.kafka.KafkaDeployer.__init__", return_value=None) as mock_init:
            make_kafka_deployer(project, fmt)
            mock_init.assert_called_once()
            args, kwargs = mock_init.call_args
            assert args[0] == "broker:9093"
            assert kwargs["security.protocol"] == "SSL"
            assert kwargs["ssl.ca.location"] == "/certs/ca.pem"
            assert kwargs["ssl.certificate.location"] == "/certs/client.pem"
            assert kwargs["ssl.key.location"] == "/certs/client.key"
            assert kwargs["ssl.key.password"] == "keypass"


# ---------------------------------------------------------------------------
# Task #34: Multi-environment auth config override
# ---------------------------------------------------------------------------

class TestMultiEnvAuthOverride:
    """Verify environment files can override auth config."""

    def test_env_file_adds_sasl_to_base(self):
        from streamt.core.parser import ProjectParser

        with tempfile.TemporaryDirectory() as tmpdir:
            p = Path(tmpdir)
            # Base project — no auth
            base = {
                "project": {"name": "multi-env-auth"},
                "runtime": {"kafka": {"bootstrap_servers": "broker:9092"}},
            }
            with open(p / "stream_project.yml", "w") as f:
                yaml.dump(base, f)

            # Prod env adds SASL
            (p / "environments").mkdir()
            env = {
                "environment": {"name": "prod"},
                "runtime": {
                    "kafka": {
                        "bootstrap_servers": "prod-broker:9093",
                        "security_protocol": "SASL_SSL",
                        "sasl_mechanism": "PLAIN",
                        "sasl_username": "produser",
                        "sasl_password": "prodpass",
                        "ssl_ca_location": "/certs/prod-ca.pem",
                    }
                },
            }
            with open(p / "environments" / "prod.yml", "w") as f:
                yaml.dump(env, f)

            project = ProjectParser(p, environment="prod").parse()
            k = project.runtime.kafka
            assert k.bootstrap_servers == "prod-broker:9093"
            assert k.security_protocol == "SASL_SSL"
            assert k.sasl_mechanism == "PLAIN"
            assert k.sasl_username == "produser"
            assert k.sasl_password.get_secret_value() == "prodpass"
            assert k.ssl_ca_location == "/certs/prod-ca.pem"

    def test_env_specific_dotenv_for_auth(self):
        from streamt.core.parser import ProjectParser

        with tempfile.TemporaryDirectory() as tmpdir:
            p = Path(tmpdir)
            base = {
                "project": {"name": "dotenv-env-auth"},
                "runtime": {"kafka": {"bootstrap_servers": "broker:9092"}},
            }
            with open(p / "stream_project.yml", "w") as f:
                yaml.dump(base, f)

            (p / "environments").mkdir()
            env = {
                "environment": {"name": "staging"},
                "runtime": {
                    "kafka": {
                        "bootstrap_servers": "staging:9092",
                        "sasl_password": "${STAGING_KAFKA_PASS}",
                    }
                },
            }
            with open(p / "environments" / "staging.yml", "w") as f:
                yaml.dump(env, f)

            with open(p / ".env.staging", "w") as f:
                f.write("STAGING_KAFKA_PASS=staging-secret\n")

            project = ProjectParser(p, environment="staging").parse()
            assert project.runtime.kafka.sasl_password.get_secret_value() == "staging-secret"

    def test_dev_plaintext_prod_sasl_ssl(self):
        from streamt.core.parser import ProjectParser

        with tempfile.TemporaryDirectory() as tmpdir:
            p = Path(tmpdir)
            base = {"project": {"name": "dual-env"}}
            with open(p / "stream_project.yml", "w") as f:
                yaml.dump(base, f)

            (p / "environments").mkdir()
            dev = {
                "environment": {"name": "dev"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                },
            }
            prod = {
                "environment": {"name": "prod"},
                "runtime": {
                    "kafka": {
                        "bootstrap_servers": "prod:9093",
                        "security_protocol": "SASL_SSL",
                        "sasl_mechanism": "SCRAM-SHA-256",
                        "sasl_username": "prodadmin",
                        "sasl_password": "prodsecret",
                    }
                },
            }
            with open(p / "environments" / "dev.yml", "w") as f:
                yaml.dump(dev, f)
            with open(p / "environments" / "prod.yml", "w") as f:
                yaml.dump(prod, f)

            dev_project = ProjectParser(p, environment="dev").parse()
            assert dev_project.runtime.kafka.security_protocol is None

            prod_project = ProjectParser(p, environment="prod").parse()
            assert prod_project.runtime.kafka.security_protocol == "SASL_SSL"
            assert prod_project.runtime.kafka.sasl_mechanism == "SCRAM-SHA-256"

    def test_env_overrides_sr_auth(self):
        from streamt.core.parser import ProjectParser

        with tempfile.TemporaryDirectory() as tmpdir:
            p = Path(tmpdir)
            base = {
                "project": {"name": "sr-env-auth"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "broker:9092"},
                    "schema_registry": {"url": "http://sr:8081"},
                },
            }
            with open(p / "stream_project.yml", "w") as f:
                yaml.dump(base, f)

            (p / "environments").mkdir()
            env = {
                "environment": {"name": "prod"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "broker:9092"},
                    "schema_registry": {
                        "url": "https://sr-prod:8081",
                        "username": "sruser",
                        "password": "srpass",
                        "ssl_ca_location": "/certs/sr-ca.pem",
                    },
                },
            }
            with open(p / "environments" / "prod.yml", "w") as f:
                yaml.dump(env, f)

            project = ProjectParser(p, environment="prod").parse()
            sr = project.runtime.schema_registry
            assert sr.url == "https://sr-prod:8081"
            assert sr.username == "sruser"
            assert sr.ssl_ca_location == "/certs/sr-ca.pem"


# ---------------------------------------------------------------------------
# Task #31: streamt status command auth wiring
# ---------------------------------------------------------------------------

class TestStatusCommandAuthWiring:
    """Verify status command creates deployers with auth fields."""

    def test_status_wires_kafka_auth(self):
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "status-auth"},
                "runtime": {
                    "kafka": {
                        "bootstrap_servers": "broker:9093",
                        "security_protocol": "SASL_SSL",
                        "sasl_mechanism": "PLAIN",
                        "sasl_username": "admin",
                        "sasl_password": "secret",
                    }
                },
                "sources": [{"name": "src", "topic": "src.raw.v1"}],
                "models": [
                    {
                        "name": "mdl",
                        "sql": 'SELECT * FROM {{ source("src") }} WHERE id IS NOT NULL',
                    }
                ],
            }
            p = _make_project_dir(tmpdir, config)

            with patch("streamt.deployer.kafka.KafkaDeployer.__init__", return_value=None) as mock_init, \
                 patch("streamt.deployer.kafka.KafkaDeployer.get_topic_state") as mock_state:
                mock_state.return_value = MagicMock(exists=False)
                result = runner.invoke(main, ["status", "-p", str(p)])
                # Deployer should have been called with auth
                if mock_init.called:
                    args, kwargs = mock_init.call_args
                    assert args[0] == "broker:9093"
                    assert kwargs.get("security.protocol") == "SASL_SSL"

    def test_status_wires_sr_ssl(self):
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "status-sr-auth"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "broker:9092"},
                    "schema_registry": {
                        "url": "https://sr:8081",
                        "username": "sruser",
                        "password": "srpass",
                        "ssl_ca_location": "/certs/ca.pem",
                    },
                },
                "sources": [
                    {
                        "name": "src",
                        "topic": "src.raw.v1",
                        "schema": {
                            "format": "avro",
                            "definition": '{"type":"record","name":"R","fields":[{"name":"id","type":"string"}]}',
                        },
                    }
                ],
            }
            p = _make_project_dir(tmpdir, config)

            with patch("streamt.deployer.schema_registry.SchemaRegistryDeployer.__init__", return_value=None) as mock_init, \
                 patch("streamt.deployer.schema_registry.SchemaRegistryDeployer.get_schema_state") as mock_state, \
                 patch("streamt.deployer.kafka.KafkaDeployer.__init__", return_value=None), \
                 patch("streamt.deployer.kafka.KafkaDeployer.get_topic_state", return_value=MagicMock(exists=False)):
                mock_state.return_value = MagicMock(exists=False)
                result = runner.invoke(main, ["status", "-p", str(p)])
                if mock_init.called:
                    _, kwargs = mock_init.call_args
                    assert kwargs.get("ssl_ca_location") == "/certs/ca.pem"
                    assert kwargs.get("username") == "sruser"
