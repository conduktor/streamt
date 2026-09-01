"""Tests for ssl_key_password support across configs and deployers."""

from unittest.mock import MagicMock, patch

from streamt.core.models import ConnectClusterConfig, FlinkClusterConfig, SchemaRegistryConfig


class TestModelSslKeyPassword:
    """ssl_key_password field exists as SecretStr on all three configs."""

    def test_sr_config_has_field(self):
        cfg = SchemaRegistryConfig(url="http://sr:8081", ssl_key_password="s3cret")
        assert cfg.ssl_key_password.get_secret_value() == "s3cret"

    def test_sr_config_none_default(self):
        cfg = SchemaRegistryConfig(url="http://sr:8081")
        assert cfg.ssl_key_password is None

    def test_flink_config_has_field(self):
        cfg = FlinkClusterConfig(rest_url="http://flink:8081", ssl_key_password="flinksecret")
        assert cfg.ssl_key_password.get_secret_value() == "flinksecret"

    def test_connect_config_has_field(self):
        cfg = ConnectClusterConfig(rest_url="http://connect:8083", ssl_key_password="connpw")
        assert cfg.ssl_key_password.get_secret_value() == "connpw"

    def test_sr_ssl_key_password_masked_in_repr(self):
        cfg = SchemaRegistryConfig(url="http://sr:8081", ssl_key_password="topsecret")
        assert "topsecret" not in repr(cfg)


class TestConfigureSessionSsl:
    """configure_session_ssl utility works correctly."""

    def test_no_ssl_noop(self):
        import requests
        session = requests.Session()
        from streamt.deployer.ssl_utils import SSLAdapter, configure_session_ssl
        configure_session_ssl(session)
        assert session.verify is True  # default
        assert session.cert is None
        http_adapter = session.get_adapter("http://example.com")
        https_adapter = session.get_adapter("https://example.com")
        assert http_adapter is https_adapter
        assert not isinstance(https_adapter, SSLAdapter)

    def test_ca_only(self):
        import requests
        session = requests.Session()
        from streamt.deployer.ssl_utils import configure_session_ssl
        configure_session_ssl(session, ssl_ca_location="/path/ca.pem")
        assert session.verify == "/path/ca.pem"

    def test_cert_and_key_no_password(self):
        import requests
        session = requests.Session()
        from streamt.deployer.ssl_utils import configure_session_ssl
        configure_session_ssl(
            session,
            ssl_certificate_location="/path/cert.pem",
            ssl_key_location="/path/key.pem",
        )
        assert session.cert == ("/path/cert.pem", "/path/key.pem")

    def test_cert_only(self):
        import requests
        session = requests.Session()
        from streamt.deployer.ssl_utils import configure_session_ssl
        configure_session_ssl(session, ssl_certificate_location="/path/cert.pem")
        assert session.cert == "/path/cert.pem"

    def test_key_password_mounts_ssl_adapter(self):
        import requests
        session = requests.Session()
        from streamt.deployer.ssl_utils import SSLAdapter, configure_session_ssl

        with patch("streamt.deployer.ssl_utils.create_urllib3_context") as mock_ctx:
            mock_ssl_ctx = MagicMock()
            mock_ctx.return_value = mock_ssl_ctx
            configure_session_ssl(
                session,
                ssl_ca_location="/path/ca.pem",
                ssl_certificate_location="/path/cert.pem",
                ssl_key_location="/path/key.pem",
                ssl_key_password="s3cret",
            )
            mock_ssl_ctx.load_verify_locations.assert_called_once_with("/path/ca.pem")
            mock_ssl_ctx.load_cert_chain.assert_called_once_with(
                certfile="/path/cert.pem",
                keyfile="/path/key.pem",
                password="s3cret",
            )
            # SSLAdapter should be mounted
            adapter = session.get_adapter("https://example.com")
            assert isinstance(adapter, SSLAdapter)


class TestDeployerSslKeyPasswordWiring:
    """Deployer factories pass ssl_key_password through."""

    def test_sr_deployer_accepts_ssl_key_password(self):
        """SchemaRegistryDeployer constructor accepts ssl_key_password."""
        with patch("streamt.deployer.ssl_utils.configure_session_ssl") as mock_ssl:
            from streamt.deployer.schema_registry import SchemaRegistryDeployer
            d = SchemaRegistryDeployer(
                "http://sr:8081",
                ssl_key_password="pw",
                ssl_certificate_location="/cert.pem",
                ssl_key_location="/key.pem",
            )
            mock_ssl.assert_called_once()
            _, kwargs = mock_ssl.call_args
            assert kwargs["ssl_key_password"] == "pw"
            d.close()

    def test_flink_deployer_accepts_ssl_key_password(self):
        with patch("streamt.deployer.ssl_utils.configure_session_ssl") as mock_ssl:
            from streamt.deployer.flink import FlinkDeployer
            d = FlinkDeployer(
                "http://flink:8081",
                ssl_key_password="pw",
                ssl_certificate_location="/cert.pem",
                ssl_key_location="/key.pem",
            )
            mock_ssl.assert_called_once()
            _, kwargs = mock_ssl.call_args
            assert kwargs["ssl_key_password"] == "pw"
            d.close()

    def test_connect_deployer_accepts_ssl_key_password(self):
        with patch("streamt.deployer.ssl_utils.configure_session_ssl") as mock_ssl:
            from streamt.deployer.connect import ConnectDeployer
            d = ConnectDeployer(
                "http://connect:8083",
                ssl_key_password="pw",
                ssl_certificate_location="/cert.pem",
                ssl_key_location="/key.pem",
            )
            mock_ssl.assert_called_once()
            _, kwargs = mock_ssl.call_args
            assert kwargs["ssl_key_password"] == "pw"
            d.close()
