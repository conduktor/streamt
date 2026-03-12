"""Tests for mask_secrets — no false positives on file paths, no misses on secrets."""

from streamt.core.environment import mask_secrets


class TestMaskSecretsCorrectness:
    """Exact-match masking: only known secret field names are masked."""

    def test_password_masked(self):
        assert mask_secrets({"password": "s3cret"}) == {"password": "****"}

    def test_sasl_password_masked(self):
        assert mask_secrets({"sasl_password": "s3cret"}) == {"sasl_password": "****"}

    def test_ssl_key_password_masked(self):
        assert mask_secrets({"ssl_key_password": "s3cret"}) == {"ssl_key_password": "****"}

    def test_api_key_masked(self):
        assert mask_secrets({"api_key": "ABCDEF"}) == {"api_key": "****"}

    def test_api_secret_masked(self):
        assert mask_secrets({"api_secret": "xyz"}) == {"api_secret": "****"}

    def test_token_masked(self):
        assert mask_secrets({"token": "tok123"}) == {"token": "****"}

    def test_secret_masked(self):
        assert mask_secrets({"secret": "shh"}) == {"secret": "****"}

    def test_credential_masked(self):
        assert mask_secrets({"credential": "cred"}) == {"credential": "****"}


class TestMaskSecretsNoFalsePositives:
    """File path fields must NOT be masked."""

    def test_ssl_key_location_not_masked(self):
        data = {"ssl_key_location": "/path/to/key.pem"}
        assert mask_secrets(data) == {"ssl_key_location": "/path/to/key.pem"}

    def test_ssl_certificate_location_not_masked(self):
        data = {"ssl_certificate_location": "/path/to/cert.pem"}
        assert mask_secrets(data) == {"ssl_certificate_location": "/path/to/cert.pem"}

    def test_ssl_ca_location_not_masked(self):
        data = {"ssl_ca_location": "/path/to/ca.pem"}
        assert mask_secrets(data) == {"ssl_ca_location": "/path/to/ca.pem"}

    def test_bootstrap_servers_not_masked(self):
        data = {"bootstrap_servers": "kafka:9092"}
        assert mask_secrets(data) == {"bootstrap_servers": "kafka:9092"}

    def test_username_not_masked(self):
        data = {"username": "admin"}
        assert mask_secrets(data) == {"username": "admin"}

    def test_rest_url_not_masked(self):
        data = {"rest_url": "http://flink:8081"}
        assert mask_secrets(data) == {"rest_url": "http://flink:8081"}


class TestMaskSecretsNested:
    """Recursion into nested dicts and lists."""

    def test_nested_dict(self):
        data = {"kafka": {"password": "secret", "bootstrap_servers": "kafka:9092"}}
        result = mask_secrets(data)
        assert result["kafka"]["password"] == "****"
        assert result["kafka"]["bootstrap_servers"] == "kafka:9092"

    def test_list_of_dicts(self):
        data = [{"password": "a"}, {"username": "b"}]
        result = mask_secrets(data)
        assert result[0]["password"] == "****"
        assert result[1]["username"] == "b"

    def test_non_string_values_not_masked(self):
        data = {"password": 12345}
        assert mask_secrets(data) == {"password": 12345}

    def test_case_insensitive_keys(self):
        data = {"Password": "secret", "API_KEY": "key123"}
        result = mask_secrets(data)
        assert result["Password"] == "****"
        assert result["API_KEY"] == "****"
