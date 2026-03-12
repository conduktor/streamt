"""Tests for SSL/mTLS support in GatewayConfig and GatewayDeployer."""

from unittest.mock import MagicMock, patch

from streamt.core.models import GatewayConfig


class TestGatewayConfigSslFields:
    """GatewayConfig has SSL fields with SecretStr for password."""

    def test_ssl_fields_default_none(self):
        cfg = GatewayConfig(admin_url="http://gw:8888")
        assert cfg.ssl_ca_location is None
        assert cfg.ssl_certificate_location is None
        assert cfg.ssl_key_location is None
        assert cfg.ssl_key_password is None

    def test_ssl_ca_location(self):
        cfg = GatewayConfig(admin_url="http://gw:8888", ssl_ca_location="/ca.pem")
        assert cfg.ssl_ca_location == "/ca.pem"

    def test_ssl_certificate_location(self):
        cfg = GatewayConfig(admin_url="http://gw:8888", ssl_certificate_location="/cert.pem")
        assert cfg.ssl_certificate_location == "/cert.pem"

    def test_ssl_key_location(self):
        cfg = GatewayConfig(admin_url="http://gw:8888", ssl_key_location="/key.pem")
        assert cfg.ssl_key_location == "/key.pem"

    def test_ssl_key_password_is_secret(self):
        cfg = GatewayConfig(admin_url="http://gw:8888", ssl_key_password="s3cret")
        assert cfg.ssl_key_password.get_secret_value() == "s3cret"
        assert "s3cret" not in repr(cfg)

    def test_ssl_path_validation_warning(self, caplog):
        cfg = GatewayConfig(admin_url="http://gw:8888", ssl_ca_location="/no/such/ca.pem")
        assert cfg.ssl_ca_location == "/no/such/ca.pem"
        assert "does not exist" in caplog.text

    def test_full_mtls_config(self):
        cfg = GatewayConfig(
            admin_url="https://gw:8888",
            ssl_ca_location="/ca.pem",
            ssl_certificate_location="/cert.pem",
            ssl_key_location="/key.pem",
            ssl_key_password="pw",
        )
        assert cfg.ssl_ca_location == "/ca.pem"
        assert cfg.ssl_certificate_location == "/cert.pem"
        assert cfg.ssl_key_location == "/key.pem"
        assert cfg.ssl_key_password.get_secret_value() == "pw"


class TestGatewayDeployerSsl:
    """GatewayDeployer calls configure_session_ssl with SSL params."""

    def test_no_ssl_params(self):
        with patch("streamt.deployer.gateway.configure_session_ssl") as mock_ssl:
            from streamt.deployer.gateway import GatewayDeployer
            d = GatewayDeployer("http://gw:8888")
            mock_ssl.assert_called_once()
            _, kwargs = mock_ssl.call_args
            assert kwargs["ssl_ca_location"] is None
            assert kwargs["ssl_key_password"] is None
            d.close()

    def test_ssl_params_forwarded(self):
        with patch("streamt.deployer.gateway.configure_session_ssl") as mock_ssl:
            from streamt.deployer.gateway import GatewayDeployer
            d = GatewayDeployer(
                "https://gw:8888",
                ssl_ca_location="/ca.pem",
                ssl_certificate_location="/cert.pem",
                ssl_key_location="/key.pem",
                ssl_key_password="s3cret",
            )
            mock_ssl.assert_called_once()
            _, kwargs = mock_ssl.call_args
            assert kwargs["ssl_ca_location"] == "/ca.pem"
            assert kwargs["ssl_certificate_location"] == "/cert.pem"
            assert kwargs["ssl_key_location"] == "/key.pem"
            assert kwargs["ssl_key_password"] == "s3cret"
            d.close()

    def test_ssl_adapter_mounted_with_key_password(self):
        from streamt.deployer.ssl_utils import SSLAdapter

        with patch("streamt.deployer.ssl_utils.create_urllib3_context") as mock_ctx:
            mock_ssl_ctx = MagicMock()
            mock_ctx.return_value = mock_ssl_ctx
            from streamt.deployer.gateway import GatewayDeployer
            d = GatewayDeployer(
                "https://gw:8888",
                ssl_ca_location="/ca.pem",
                ssl_certificate_location="/cert.pem",
                ssl_key_location="/key.pem",
                ssl_key_password="s3cret",
            )
            adapter = d._session.get_adapter("https://gw:8888")
            assert isinstance(adapter, SSLAdapter)
            d.close()


class TestMakeGatewayDeployer:
    """make_gateway_deployer wires SSL from GatewayConfig."""

    def test_no_conduktor_returns_none(self):
        from streamt.cli.helpers import make_gateway_deployer
        from streamt.output import OutputFormatter

        project = MagicMock()
        project.runtime.conduktor = None
        fmt = OutputFormatter("text")
        assert make_gateway_deployer(project, fmt) is None

    def test_no_gateway_returns_none(self):
        from streamt.cli.helpers import make_gateway_deployer
        from streamt.output import OutputFormatter

        project = MagicMock()
        project.runtime.conduktor.gateway = None
        fmt = OutputFormatter("text")
        assert make_gateway_deployer(project, fmt) is None

    def test_no_admin_url_returns_none(self):
        from streamt.cli.helpers import make_gateway_deployer
        from streamt.output import OutputFormatter

        project = MagicMock()
        project.runtime.conduktor.gateway.admin_url = None
        fmt = OutputFormatter("text")
        assert make_gateway_deployer(project, fmt) is None

    def test_creates_deployer_with_ssl(self):
        from streamt.cli.helpers import make_gateway_deployer
        from streamt.output import OutputFormatter

        project = MagicMock()
        gw = project.runtime.conduktor.gateway
        gw.admin_url = "https://gw:8888"
        gw.username = "admin"
        gw.password = None
        gw.virtual_cluster = "vc1"
        gw.ssl_ca_location = "/ca.pem"
        gw.ssl_certificate_location = "/cert.pem"
        gw.ssl_key_location = "/key.pem"
        gw.ssl_key_password = None
        fmt = OutputFormatter("text")

        with patch("streamt.deployer.gateway.configure_session_ssl"):
            deployer = make_gateway_deployer(project, fmt)
            assert deployer is not None
            assert deployer.admin_url == "https://gw:8888"
            assert deployer.virtual_cluster == "vc1"
            deployer.close()
