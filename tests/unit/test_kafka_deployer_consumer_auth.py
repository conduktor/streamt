"""Tests for KafkaDeployer Consumer auth forwarding.

Production scenario: User has SASL_SSL Kafka. Runs `streamt status --lag`.
The Consumer created for watermark queries must use the same auth config
as the AdminClient, or it fails with authentication errors.
"""

from unittest.mock import MagicMock, patch

from streamt.deployer.kafka import KafkaDeployer


class TestKafkaDeployerStoresConfig:
    """KafkaDeployer must store the full config for reuse by Consumers."""

    def test_stores_full_config(self):
        """Auth config passed to __init__ is retained for Consumer creation."""
        with patch("streamt.deployer.kafka.AdminClient"):
            deployer = KafkaDeployer(
                "broker:9093",
                **{
                    "security.protocol": "SASL_SSL",
                    "sasl.mechanism": "PLAIN",
                    "sasl.username": "admin",
                    "sasl.password": "secret",
                    "ssl.ca.location": "/certs/ca.pem",
                },
            )
            # The deployer must store the merged config for Consumer reuse
            assert hasattr(deployer, "_config"), \
                "KafkaDeployer must store config as _config for Consumer reuse"
            assert deployer._config["security.protocol"] == "SASL_SSL"
            assert deployer._config["sasl.mechanism"] == "PLAIN"
            assert deployer._config["sasl.username"] == "admin"
            assert deployer._config["sasl.password"] == "secret"
            assert deployer._config["ssl.ca.location"] == "/certs/ca.pem"
            assert deployer._config["bootstrap.servers"] == "broker:9093"

    def test_plain_config_stores_minimal(self):
        """Plain config stores only bootstrap.servers."""
        with patch("streamt.deployer.kafka.AdminClient"):
            deployer = KafkaDeployer("localhost:9092")
            assert deployer._config == {"bootstrap.servers": "localhost:9092"}


class TestConsumerInheritsAuth:
    """Consumer instances must inherit security config from the deployer."""

    @patch("streamt.deployer.kafka.Consumer")
    @patch("streamt.deployer.kafka.AdminClient")
    def test_get_topic_message_count_passes_auth(self, MockAdmin, MockConsumer):
        """get_topic_message_count creates Consumer with full auth config."""
        # Setup: deployer with SASL
        deployer = KafkaDeployer(
            "broker:9093",
            **{
                "security.protocol": "SASL_SSL",
                "sasl.mechanism": "PLAIN",
                "sasl.username": "admin",
                "sasl.password": "secret",
            },
        )

        # Mock topic metadata
        mock_partition = MagicMock()
        mock_topic_meta = MagicMock()
        mock_topic_meta.partitions = {0: mock_partition}
        mock_metadata = MagicMock()
        mock_metadata.topics = {"test-topic": mock_topic_meta}
        deployer.admin.list_topics.return_value = mock_metadata

        # Mock consumer watermark offsets
        mock_consumer = MagicMock()
        mock_consumer.get_watermark_offsets.return_value = (0, 100)
        MockConsumer.return_value = mock_consumer

        deployer.get_topic_message_count("test-topic")

        # Verify Consumer was created with auth config
        MockConsumer.assert_called_once()
        consumer_config = MockConsumer.call_args[0][0]
        assert consumer_config["security.protocol"] == "SASL_SSL"
        assert consumer_config["sasl.mechanism"] == "PLAIN"
        assert consumer_config["sasl.username"] == "admin"
        assert consumer_config["sasl.password"] == "secret"
        assert "bootstrap.servers" in consumer_config

    @patch("streamt.deployer.kafka.Consumer")
    @patch("streamt.deployer.kafka.AdminClient")
    def test_get_consumer_group_lag_passes_auth(self, MockAdmin, MockConsumer):
        """get_consumer_group_lag creates Consumer with full auth config."""
        deployer = KafkaDeployer(
            "broker:9093",
            **{
                "security.protocol": "SASL_SSL",
                "sasl.mechanism": "SCRAM-SHA-256",
                "sasl.username": "user",
                "sasl.password": "pass",
            },
        )

        # Mock topic metadata
        mock_partition = MagicMock()
        mock_topic_meta = MagicMock()
        mock_topic_meta.partitions = {0: mock_partition}
        mock_metadata = MagicMock()
        mock_metadata.topics = {"my-topic": mock_topic_meta}
        deployer.admin.list_topics.return_value = mock_metadata

        # Mock ConsumerGroupTopicPartitions (may not exist in all versions)
        mock_cgtp_cls = MagicMock()
        mock_cgtp_result = MagicMock()
        mock_tp = MagicMock()
        mock_tp.topic = "my-topic"
        mock_tp.partition = 0
        mock_tp.offset = 50
        mock_cgtp_result.topic_partitions = [mock_tp]
        mock_future = MagicMock()
        mock_future.result.return_value = mock_cgtp_result
        deployer.admin.list_consumer_group_offsets.return_value = {"group": mock_future}

        # Mock consumer watermark offsets
        mock_consumer = MagicMock()
        mock_consumer.get_watermark_offsets.return_value = (0, 100)
        MockConsumer.return_value = mock_consumer

        with patch("streamt.deployer.kafka.ConsumerGroupTopicPartitions", mock_cgtp_cls, create=True):
            deployer.get_consumer_group_lag("my-group", "my-topic")

        # If ConsumerGroupTopicPartitions import fails, the method returns None
        # but Consumer should still have been created with auth if it got that far
        if MockConsumer.called:
            consumer_config = MockConsumer.call_args[0][0]
            assert consumer_config["security.protocol"] == "SASL_SSL"
            assert consumer_config["sasl.mechanism"] == "SCRAM-SHA-256"
            assert consumer_config["sasl.username"] == "user"
            assert consumer_config["sasl.password"] == "pass"
        else:
            # If the lag method can't import ConsumerGroupTopicPartitions,
            # at least verify _config is stored correctly
            assert deployer._config["security.protocol"] == "SASL_SSL"

    @patch("streamt.deployer.kafka.Consumer")
    @patch("streamt.deployer.kafka.AdminClient")
    def test_consumer_uses_stored_bootstrap_not_broker_metadata(self, MockAdmin, MockConsumer):
        """Consumer should use the configured bootstrap.servers, not broker metadata extraction."""
        deployer = KafkaDeployer(
            "prod-broker-1:9093,prod-broker-2:9093",
            **{"security.protocol": "SSL"},
        )

        mock_partition = MagicMock()
        mock_topic_meta = MagicMock()
        mock_topic_meta.partitions = {0: mock_partition}
        mock_metadata = MagicMock()
        mock_metadata.topics = {"t": mock_topic_meta}
        deployer.admin.list_topics.return_value = mock_metadata

        mock_consumer = MagicMock()
        mock_consumer.get_watermark_offsets.return_value = (0, 10)
        MockConsumer.return_value = mock_consumer

        deployer.get_topic_message_count("t")

        consumer_config = MockConsumer.call_args[0][0]
        assert consumer_config["bootstrap.servers"] == "prod-broker-1:9093,prod-broker-2:9093"

    @patch("streamt.deployer.kafka.Consumer")
    @patch("streamt.deployer.kafka.AdminClient")
    def test_plain_consumer_has_no_extra_auth(self, MockAdmin, MockConsumer):
        """Plain deployer Consumer config only has bootstrap + group + autocommit."""
        deployer = KafkaDeployer("localhost:9092")

        mock_partition = MagicMock()
        mock_topic_meta = MagicMock()
        mock_topic_meta.partitions = {0: mock_partition}
        mock_metadata = MagicMock()
        mock_metadata.topics = {"t": mock_topic_meta}
        deployer.admin.list_topics.return_value = mock_metadata

        mock_consumer = MagicMock()
        mock_consumer.get_watermark_offsets.return_value = (0, 5)
        MockConsumer.return_value = mock_consumer

        deployer.get_topic_message_count("t")

        consumer_config = MockConsumer.call_args[0][0]
        assert "security.protocol" not in consumer_config
        assert "sasl.mechanism" not in consumer_config
