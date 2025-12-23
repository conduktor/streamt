"""End-to-end tests for Conduktor Gateway integration.

These tests verify the complete data flow through Gateway:
1. Produce messages to physical Kafka topics
2. Configure Gateway with alias topics and interceptors
3. Consume through Gateway proxy and verify transformations

Test categories:
- Connection: Basic connectivity tests
- Alias Topics: Topic aliasing (virtual -> physical mapping)
- Filtering: SQL-based message filtering
- Masking: Field-level data masking (when supported)
- Failure Modes: Error handling and edge cases
"""

import uuid
from typing import Generator

import pytest

from streamt.compiler.manifest import GatewayRuleArtifact
from streamt.deployer import GatewayDeployer

from .conftest import GatewayHelper, KafkaHelper
from .helpers import INFRA_CONFIG

# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def unique_topic_name() -> str:
    """Generate a unique topic name for test isolation."""
    return f"test-gw-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def physical_topic(
    kafka_helper: KafkaHelper,
    unique_topic_name: str,
) -> Generator[str, None, None]:
    """Create a physical Kafka topic for testing."""
    topic_name = f"{unique_topic_name}-physical"
    kafka_helper.create_topic(topic_name, partitions=1)
    yield topic_name
    kafka_helper.delete_topic(topic_name)


# =============================================================================
# Connection Tests
# =============================================================================


@pytest.mark.integration
@pytest.mark.gateway
class TestGatewayConnection:
    """Test Gateway connectivity."""

    def test_gateway_health_check(
        self,
        docker_services,
        gateway_helper: GatewayHelper,
    ):
        """Gateway health endpoint should return 200."""
        assert gateway_helper.check_connection() is True

    def test_gateway_invalid_url(self):
        """Connection to invalid URL should fail gracefully."""
        helper = GatewayHelper("http://localhost:99999")
        assert helper.check_connection() is False

    def test_gateway_deployer_health(self, docker_services):
        """GatewayDeployer health check should work."""
        gateway_config = INFRA_CONFIG.gateway
        with GatewayDeployer(
            admin_url=gateway_config.admin_url,
            username=gateway_config.username,
            password=gateway_config.password,
        ) as deployer:
            assert deployer.health_check() is True


# =============================================================================
# Alias Topic Tests - Basic Mapping
# =============================================================================


@pytest.mark.integration
@pytest.mark.gateway
@pytest.mark.kafka
class TestAliasTopic:
    """Test alias topic functionality - mapping virtual topics to physical ones."""

    def test_alias_topic_passthrough(
        self,
        docker_services,
        kafka_helper: KafkaHelper,
        gateway_helper: GatewayHelper,
        physical_topic: str,
    ):
        """Messages produced to physical topic should be readable via alias.

        Flow: Producer -> physical_topic -> Gateway alias -> Consumer
        """
        alias_name = f"test-alias-{uuid.uuid4().hex[:8]}"
        group_id = f"test-group-{uuid.uuid4().hex[:8]}"

        try:
            # Create alias mapping
            gateway_helper.create_alias_topic(alias_name, physical_topic)

            # Produce messages to physical topic
            messages = [
                {"order_id": "1", "amount": 100, "status": "pending"},
                {"order_id": "2", "amount": 200, "status": "completed"},
                {"order_id": "3", "amount": 300, "status": "pending"},
            ]
            kafka_helper.produce_messages(physical_topic, messages)

            # Consume through Gateway using alias name
            received = gateway_helper.consume_through_gateway(
                proxy_bootstrap=INFRA_CONFIG.gateway.proxy_bootstrap,
                topic=alias_name,
                group_id=group_id,
                timeout=15.0,
                expected_count=3,
            )

            # All messages should pass through unchanged
            assert len(received) == 3
            order_ids = {m["order_id"] for m in received}
            assert order_ids == {"1", "2", "3"}

        finally:
            gateway_helper.delete_alias_topic(alias_name)

    def test_alias_topic_update_physical(
        self,
        docker_services,
        kafka_helper: KafkaHelper,
        gateway_helper: GatewayHelper,
        unique_topic_name: str,
    ):
        """Alias can be updated to point to a different physical topic.

        This is useful for topic migrations without changing consumer configs.
        """
        alias_name = f"test-alias-update-{uuid.uuid4().hex[:8]}"
        physical_v1 = f"{unique_topic_name}-v1"
        physical_v2 = f"{unique_topic_name}-v2"
        group_id = f"test-group-{uuid.uuid4().hex[:8]}"

        try:
            # Create two physical topics with different data
            kafka_helper.create_topic(physical_v1, partitions=1)
            kafka_helper.create_topic(physical_v2, partitions=1)

            kafka_helper.produce_messages(physical_v1, [{"version": "v1", "data": "old"}])
            kafka_helper.produce_messages(physical_v2, [{"version": "v2", "data": "new"}])

            # Point alias to v1
            gateway_helper.create_alias_topic(alias_name, physical_v1)

            received = gateway_helper.consume_through_gateway(
                proxy_bootstrap=INFRA_CONFIG.gateway.proxy_bootstrap,
                topic=alias_name,
                group_id=f"{group_id}-1",
                timeout=10.0,
                expected_count=1,
            )
            assert received[0]["version"] == "v1"

            # Update alias to point to v2
            gateway_helper.create_alias_topic(alias_name, physical_v2)

            received = gateway_helper.consume_through_gateway(
                proxy_bootstrap=INFRA_CONFIG.gateway.proxy_bootstrap,
                topic=alias_name,
                group_id=f"{group_id}-2",
                timeout=10.0,
                expected_count=1,
            )
            assert received[0]["version"] == "v2"

        finally:
            gateway_helper.delete_alias_topic(alias_name)
            kafka_helper.delete_topic(physical_v1)
            kafka_helper.delete_topic(physical_v2)


# =============================================================================
# Filtering Tests - SQL WHERE Clause
# =============================================================================


@pytest.mark.integration
@pytest.mark.gateway
@pytest.mark.kafka
class TestGatewayFiltering:
    """Test Gateway filtering with various SQL patterns."""

    def test_filter_numeric_comparison(
        self,
        docker_services,
        kafka_helper: KafkaHelper,
        gateway_helper: GatewayHelper,
        physical_topic: str,
    ):
        """Filter messages where amount > threshold.

        Expected: Only high-value orders pass through.
        """
        alias_name = f"test-filter-numeric-{uuid.uuid4().hex[:8]}"
        interceptor_name = f"{alias_name}_filter"
        group_id = f"test-group-{uuid.uuid4().hex[:8]}"

        try:
            # Create alias
            gateway_helper.create_alias_topic(alias_name, physical_topic)

            # Create filter interceptor: amount > 150
            gateway_helper.create_interceptor(
                name=interceptor_name,
                plugin_class="io.conduktor.gateway.interceptor.VirtualSqlTopicPlugin",
                config={
                    "virtualTopic": alias_name,
                    "statement": f'SELECT * FROM "{physical_topic}" WHERE amount > 150',
                },
            )

            # Produce test messages
            messages = [
                {"order_id": "1", "amount": 100},  # Should be filtered OUT
                {"order_id": "2", "amount": 200},  # Should pass
                {"order_id": "3", "amount": 50},   # Should be filtered OUT
                {"order_id": "4", "amount": 300},  # Should pass
                {"order_id": "5", "amount": 150},  # Should be filtered OUT (not > 150)
            ]
            kafka_helper.produce_messages(physical_topic, messages)

            # Consume through Gateway
            received = gateway_helper.consume_through_gateway(
                proxy_bootstrap=INFRA_CONFIG.gateway.proxy_bootstrap,
                topic=alias_name,
                group_id=group_id,
                timeout=15.0,
                expected_count=2,
            )

            # Only orders 2 and 4 should pass
            assert len(received) == 2
            order_ids = {m["order_id"] for m in received}
            assert order_ids == {"2", "4"}

        finally:
            gateway_helper.delete_interceptor(interceptor_name)
            gateway_helper.delete_alias_topic(alias_name)

    def test_filter_string_equality(
        self,
        docker_services,
        kafka_helper: KafkaHelper,
        gateway_helper: GatewayHelper,
        physical_topic: str,
    ):
        """Filter messages where status equals specific value.

        Expected: Only completed orders pass through.
        """
        alias_name = f"test-filter-string-{uuid.uuid4().hex[:8]}"
        interceptor_name = f"{alias_name}_filter"
        group_id = f"test-group-{uuid.uuid4().hex[:8]}"

        try:
            gateway_helper.create_alias_topic(alias_name, physical_topic)
            gateway_helper.create_interceptor(
                name=interceptor_name,
                plugin_class="io.conduktor.gateway.interceptor.VirtualSqlTopicPlugin",
                config={
                    "virtualTopic": alias_name,
                    "statement": f"SELECT * FROM \"{physical_topic}\" WHERE status = 'completed'",
                },
            )

            messages = [
                {"order_id": "1", "status": "pending"},
                {"order_id": "2", "status": "completed"},
                {"order_id": "3", "status": "cancelled"},
                {"order_id": "4", "status": "completed"},
                {"order_id": "5", "status": "pending"},
            ]
            kafka_helper.produce_messages(physical_topic, messages)

            received = gateway_helper.consume_through_gateway(
                proxy_bootstrap=INFRA_CONFIG.gateway.proxy_bootstrap,
                topic=alias_name,
                group_id=group_id,
                timeout=15.0,
                expected_count=2,
            )

            assert len(received) == 2
            order_ids = {m["order_id"] for m in received}
            assert order_ids == {"2", "4"}

        finally:
            gateway_helper.delete_interceptor(interceptor_name)
            gateway_helper.delete_alias_topic(alias_name)

    def test_filter_string_regexp(
        self,
        docker_services,
        kafka_helper: KafkaHelper,
        gateway_helper: GatewayHelper,
        physical_topic: str,
    ):
        """Filter messages using REGEXP pattern matching.

        Note: Gateway VirtualSqlTopicPlugin doesn't support IN clause.
        Use REGEXP as an alternative for pattern matching.
        Note: Anchor characters like ^ may not work - use 'EU.*' instead of '^EU'.

        Expected: Only orders from EU regions (EU, EU-WEST, EU-EAST) pass through.
        """
        alias_name = f"test-filter-regexp-{uuid.uuid4().hex[:8]}"
        interceptor_name = f"{alias_name}_filter"
        group_id = f"test-group-{uuid.uuid4().hex[:8]}"

        try:
            gateway_helper.create_alias_topic(alias_name, physical_topic)
            gateway_helper.create_interceptor(
                name=interceptor_name,
                plugin_class="io.conduktor.gateway.interceptor.VirtualSqlTopicPlugin",
                config={
                    "virtualTopic": alias_name,
                    "statement": f"SELECT * FROM \"{physical_topic}\" WHERE region REGEXP 'EU.*'",
                },
            )

            messages = [
                {"order_id": "1", "region": "EU"},
                {"order_id": "2", "region": "APAC"},
                {"order_id": "3", "region": "EU-WEST"},
                {"order_id": "4", "region": "LATAM"},
                {"order_id": "5", "region": "EU-EAST"},
            ]
            kafka_helper.produce_messages(physical_topic, messages)

            received = gateway_helper.consume_through_gateway(
                proxy_bootstrap=INFRA_CONFIG.gateway.proxy_bootstrap,
                topic=alias_name,
                group_id=group_id,
                timeout=15.0,
                expected_count=3,
            )

            assert len(received) == 3
            order_ids = {m["order_id"] for m in received}
            assert order_ids == {"1", "3", "5"}

        finally:
            gateway_helper.delete_interceptor(interceptor_name)
            gateway_helper.delete_alias_topic(alias_name)

    def test_filter_multiple_and_conditions(
        self,
        docker_services,
        kafka_helper: KafkaHelper,
        gateway_helper: GatewayHelper,
        physical_topic: str,
    ):
        """Filter with multiple AND conditions.

        Note: Gateway VirtualSqlTopicPlugin doesn't support OR operator.
        Only AND-combined conditions are supported.

        Expected: High-value completed premium orders pass through.
        """
        alias_name = f"test-filter-and-{uuid.uuid4().hex[:8]}"
        interceptor_name = f"{alias_name}_filter"
        group_id = f"test-group-{uuid.uuid4().hex[:8]}"

        try:
            gateway_helper.create_alias_topic(alias_name, physical_topic)
            gateway_helper.create_interceptor(
                name=interceptor_name,
                plugin_class="io.conduktor.gateway.interceptor.VirtualSqlTopicPlugin",
                config={
                    "virtualTopic": alias_name,
                    "statement": (
                        f"SELECT * FROM \"{physical_topic}\" "
                        "WHERE amount > 100 AND status = 'completed' AND tier = 'premium'"
                    ),
                },
            )

            messages = [
                {"order_id": "1", "amount": 100, "status": "completed", "tier": "premium"},   # OUT (amount not > 100)
                {"order_id": "2", "amount": 600, "status": "completed", "tier": "standard"},  # OUT (not premium)
                {"order_id": "3", "amount": 150, "status": "pending", "tier": "premium"},     # OUT (not completed)
                {"order_id": "4", "amount": 200, "status": "completed", "tier": "premium"},   # IN (all conditions met)
                {"order_id": "5", "amount": 300, "status": "completed", "tier": "premium"},   # IN (all conditions met)
            ]
            kafka_helper.produce_messages(physical_topic, messages)

            received = gateway_helper.consume_through_gateway(
                proxy_bootstrap=INFRA_CONFIG.gateway.proxy_bootstrap,
                topic=alias_name,
                group_id=group_id,
                timeout=15.0,
                expected_count=2,
            )

            assert len(received) == 2
            order_ids = {m["order_id"] for m in received}
            assert order_ids == {"4", "5"}

        finally:
            gateway_helper.delete_interceptor(interceptor_name)
            gateway_helper.delete_alias_topic(alias_name)

    def test_filter_email_format_with_regexp(
        self,
        docker_services,
        kafka_helper: KafkaHelper,
        gateway_helper: GatewayHelper,
        physical_topic: str,
    ):
        """Filter messages using REGEXP to validate email format.

        Note: Gateway VirtualSqlTopicPlugin doesn't support IS NULL / IS NOT NULL.
        Use REGEXP to filter by pattern instead.
        Note: Simple pattern like '@' alone doesn't work - use '.*@.*' pattern.

        Expected: Only messages with @ in email pass through.
        """
        alias_name = f"test-filter-email-{uuid.uuid4().hex[:8]}"
        interceptor_name = f"{alias_name}_filter"
        group_id = f"test-group-{uuid.uuid4().hex[:8]}"

        try:
            gateway_helper.create_alias_topic(alias_name, physical_topic)
            gateway_helper.create_interceptor(
                name=interceptor_name,
                plugin_class="io.conduktor.gateway.interceptor.VirtualSqlTopicPlugin",
                config={
                    "virtualTopic": alias_name,
                    "statement": f"SELECT * FROM \"{physical_topic}\" WHERE email REGEXP '.*@.*'",
                },
            )

            messages = [
                {"user_id": "1", "email": "user1@example.com"},   # IN (has @)
                {"user_id": "2", "email": "invalid-email"},       # OUT (no @ sign)
                {"user_id": "3", "email": "user3@test.org"},      # IN (has @)
                {"user_id": "4", "email": "noemail"},             # OUT (no @)
            ]
            kafka_helper.produce_messages(physical_topic, messages)

            received = gateway_helper.consume_through_gateway(
                proxy_bootstrap=INFRA_CONFIG.gateway.proxy_bootstrap,
                topic=alias_name,
                group_id=group_id,
                timeout=15.0,
                expected_count=2,
            )

            assert len(received) == 2
            user_ids = {m["user_id"] for m in received}
            assert user_ids == {"1", "3"}

        finally:
            gateway_helper.delete_interceptor(interceptor_name)
            gateway_helper.delete_alias_topic(alias_name)


# =============================================================================
# Unsupported SQL Syntax Tests (skipped - Gateway limitation)
# =============================================================================

# These tests document SQL syntax that would be useful but is NOT supported
# by Gateway's VirtualSqlTopicPlugin. They are skipped to show what's missing.
# See: GatewayOperator.java and GatewayStatementAnalyzer.java in conduktor-proxy

GATEWAY_SQL_LIMITATION = (
    "Gateway VirtualSqlTopicPlugin doesn't support this SQL syntax. "
    "Supported: =, >, <, >=, <=, <>, REGEXP, AND. "
    "Not supported: IN, OR, IS NULL, IS NOT NULL, LIKE, BETWEEN."
)


@pytest.mark.integration
@pytest.mark.gateway
@pytest.mark.kafka
class TestGatewayUnsupportedSQL:
    """Tests for SQL syntax NOT supported by Gateway VirtualSqlTopicPlugin.

    These tests are skipped but document what SQL features are missing.
    This helps distinguish between bugs in our code vs Gateway limitations.
    """

    @pytest.mark.skip(reason=f"IN clause not supported. {GATEWAY_SQL_LIMITATION}")
    def test_filter_in_clause(
        self,
        docker_services,
        kafka_helper: KafkaHelper,
        gateway_helper: GatewayHelper,
        physical_topic: str,
    ):
        """Filter messages where region is in allowed list using IN clause.

        SQL: WHERE region IN ('EU', 'US')
        Expected: Only EU and US orders pass through.

        This would be the ideal syntax but Gateway doesn't support it.
        Workaround: Use multiple OR conditions (also not supported) or REGEXP.
        """
        alias_name = f"test-filter-in-{uuid.uuid4().hex[:8]}"
        interceptor_name = f"{alias_name}_filter"
        group_id = f"test-group-{uuid.uuid4().hex[:8]}"

        try:
            gateway_helper.create_alias_topic(alias_name, physical_topic)
            gateway_helper.create_interceptor(
                name=interceptor_name,
                plugin_class="io.conduktor.gateway.interceptor.VirtualSqlTopicPlugin",
                config={
                    "virtualTopic": alias_name,
                    "statement": f"SELECT * FROM \"{physical_topic}\" WHERE region IN ('EU', 'US')",
                },
            )

            messages = [
                {"order_id": "1", "region": "EU"},
                {"order_id": "2", "region": "APAC"},
                {"order_id": "3", "region": "US"},
                {"order_id": "4", "region": "LATAM"},
                {"order_id": "5", "region": "EU"},
            ]
            kafka_helper.produce_messages(physical_topic, messages)

            received = gateway_helper.consume_through_gateway(
                proxy_bootstrap=INFRA_CONFIG.gateway.proxy_bootstrap,
                topic=alias_name,
                group_id=group_id,
                timeout=15.0,
                expected_count=3,
            )

            assert len(received) == 3
            order_ids = {m["order_id"] for m in received}
            assert order_ids == {"1", "3", "5"}

        finally:
            gateway_helper.delete_interceptor(interceptor_name)
            gateway_helper.delete_alias_topic(alias_name)

    @pytest.mark.skip(reason=f"OR operator not supported. {GATEWAY_SQL_LIMITATION}")
    def test_filter_or_conditions(
        self,
        docker_services,
        kafka_helper: KafkaHelper,
        gateway_helper: GatewayHelper,
        physical_topic: str,
    ):
        """Filter with OR compound conditions.

        SQL: WHERE (amount > 500 AND status = 'completed') OR tier = 'premium'
        Expected: High-value completed orders OR any premium orders.

        This would allow complex business logic but Gateway doesn't support OR.
        Workaround: Create multiple interceptors or filter in application code.
        """
        alias_name = f"test-filter-or-{uuid.uuid4().hex[:8]}"
        interceptor_name = f"{alias_name}_filter"
        group_id = f"test-group-{uuid.uuid4().hex[:8]}"

        try:
            gateway_helper.create_alias_topic(alias_name, physical_topic)
            gateway_helper.create_interceptor(
                name=interceptor_name,
                plugin_class="io.conduktor.gateway.interceptor.VirtualSqlTopicPlugin",
                config={
                    "virtualTopic": alias_name,
                    "statement": (
                        f"SELECT * FROM \"{physical_topic}\" "
                        "WHERE (amount > 500 AND status = 'completed') OR tier = 'premium'"
                    ),
                },
            )

            messages = [
                {"order_id": "1", "amount": 100, "status": "completed", "tier": "standard"},
                {"order_id": "2", "amount": 600, "status": "completed", "tier": "standard"},
                {"order_id": "3", "amount": 50, "status": "pending", "tier": "premium"},
                {"order_id": "4", "amount": 700, "status": "pending", "tier": "standard"},
                {"order_id": "5", "amount": 200, "status": "completed", "tier": "premium"},
            ]
            kafka_helper.produce_messages(physical_topic, messages)

            received = gateway_helper.consume_through_gateway(
                proxy_bootstrap=INFRA_CONFIG.gateway.proxy_bootstrap,
                topic=alias_name,
                group_id=group_id,
                timeout=15.0,
                expected_count=3,
            )

            assert len(received) == 3
            order_ids = {m["order_id"] for m in received}
            assert order_ids == {"2", "3", "5"}

        finally:
            gateway_helper.delete_interceptor(interceptor_name)
            gateway_helper.delete_alias_topic(alias_name)

    @pytest.mark.skip(reason=f"IS NOT NULL not supported. {GATEWAY_SQL_LIMITATION}")
    def test_filter_is_not_null(
        self,
        docker_services,
        kafka_helper: KafkaHelper,
        gateway_helper: GatewayHelper,
        physical_topic: str,
    ):
        """Filter out messages with null values using IS NOT NULL.

        SQL: WHERE email IS NOT NULL
        Expected: Only messages with non-null email pass.

        This would filter incomplete records but Gateway doesn't support NULL checks.
        Workaround: Use REGEXP to check for expected patterns.
        """
        alias_name = f"test-filter-null-{uuid.uuid4().hex[:8]}"
        interceptor_name = f"{alias_name}_filter"
        group_id = f"test-group-{uuid.uuid4().hex[:8]}"

        try:
            gateway_helper.create_alias_topic(alias_name, physical_topic)
            gateway_helper.create_interceptor(
                name=interceptor_name,
                plugin_class="io.conduktor.gateway.interceptor.VirtualSqlTopicPlugin",
                config={
                    "virtualTopic": alias_name,
                    "statement": f'SELECT * FROM "{physical_topic}" WHERE email IS NOT NULL',
                },
            )

            messages = [
                {"user_id": "1", "email": "user1@example.com"},
                {"user_id": "2", "email": None},
                {"user_id": "3"},  # Missing email field
                {"user_id": "4", "email": "user4@example.com"},
            ]
            kafka_helper.produce_messages(physical_topic, messages)

            received = gateway_helper.consume_through_gateway(
                proxy_bootstrap=INFRA_CONFIG.gateway.proxy_bootstrap,
                topic=alias_name,
                group_id=group_id,
                timeout=15.0,
                expected_count=2,
            )

            assert len(received) == 2
            user_ids = {m["user_id"] for m in received}
            assert user_ids == {"1", "4"}

        finally:
            gateway_helper.delete_interceptor(interceptor_name)
            gateway_helper.delete_alias_topic(alias_name)

    @pytest.mark.skip(reason=f"IS NULL not supported. {GATEWAY_SQL_LIMITATION}")
    def test_filter_is_null(
        self,
        docker_services,
        kafka_helper: KafkaHelper,
        gateway_helper: GatewayHelper,
        physical_topic: str,
    ):
        """Filter messages WITH null values using IS NULL.

        SQL: WHERE email IS NULL
        Expected: Only messages with null or missing email pass.

        Useful for finding incomplete records but not supported.
        """
        alias_name = f"test-filter-isnull-{uuid.uuid4().hex[:8]}"
        interceptor_name = f"{alias_name}_filter"
        group_id = f"test-group-{uuid.uuid4().hex[:8]}"

        try:
            gateway_helper.create_alias_topic(alias_name, physical_topic)
            gateway_helper.create_interceptor(
                name=interceptor_name,
                plugin_class="io.conduktor.gateway.interceptor.VirtualSqlTopicPlugin",
                config={
                    "virtualTopic": alias_name,
                    "statement": f'SELECT * FROM "{physical_topic}" WHERE email IS NULL',
                },
            )

            messages = [
                {"user_id": "1", "email": "user1@example.com"},
                {"user_id": "2", "email": None},
                {"user_id": "3"},  # Missing email field
                {"user_id": "4", "email": "user4@example.com"},
            ]
            kafka_helper.produce_messages(physical_topic, messages)

            received = gateway_helper.consume_through_gateway(
                proxy_bootstrap=INFRA_CONFIG.gateway.proxy_bootstrap,
                topic=alias_name,
                group_id=group_id,
                timeout=15.0,
                expected_count=2,
            )

            assert len(received) == 2
            user_ids = {m["user_id"] for m in received}
            assert user_ids == {"2", "3"}

        finally:
            gateway_helper.delete_interceptor(interceptor_name)
            gateway_helper.delete_alias_topic(alias_name)

    @pytest.mark.skip(reason=f"LIKE operator not supported. {GATEWAY_SQL_LIMITATION}")
    def test_filter_like_pattern(
        self,
        docker_services,
        kafka_helper: KafkaHelper,
        gateway_helper: GatewayHelper,
        physical_topic: str,
    ):
        """Filter using LIKE pattern matching.

        SQL: WHERE email LIKE '%@example.com'
        Expected: Only emails from example.com domain pass.

        LIKE is standard SQL but not supported. Use REGEXP instead.
        """
        alias_name = f"test-filter-like-{uuid.uuid4().hex[:8]}"
        interceptor_name = f"{alias_name}_filter"
        group_id = f"test-group-{uuid.uuid4().hex[:8]}"

        try:
            gateway_helper.create_alias_topic(alias_name, physical_topic)
            gateway_helper.create_interceptor(
                name=interceptor_name,
                plugin_class="io.conduktor.gateway.interceptor.VirtualSqlTopicPlugin",
                config={
                    "virtualTopic": alias_name,
                    "statement": f"SELECT * FROM \"{physical_topic}\" WHERE email LIKE '%@example.com'",
                },
            )

            messages = [
                {"user_id": "1", "email": "user1@example.com"},
                {"user_id": "2", "email": "user2@other.com"},
                {"user_id": "3", "email": "user3@example.com"},
            ]
            kafka_helper.produce_messages(physical_topic, messages)

            received = gateway_helper.consume_through_gateway(
                proxy_bootstrap=INFRA_CONFIG.gateway.proxy_bootstrap,
                topic=alias_name,
                group_id=group_id,
                timeout=15.0,
                expected_count=2,
            )

            assert len(received) == 2
            user_ids = {m["user_id"] for m in received}
            assert user_ids == {"1", "3"}

        finally:
            gateway_helper.delete_interceptor(interceptor_name)
            gateway_helper.delete_alias_topic(alias_name)

    @pytest.mark.skip(reason=f"BETWEEN operator not supported. {GATEWAY_SQL_LIMITATION}")
    def test_filter_between_range(
        self,
        docker_services,
        kafka_helper: KafkaHelper,
        gateway_helper: GatewayHelper,
        physical_topic: str,
    ):
        """Filter using BETWEEN for range queries.

        SQL: WHERE amount BETWEEN 100 AND 500
        Expected: Only orders with amount 100-500 pass.

        BETWEEN is convenient but not supported. Use >= AND <= instead.
        """
        alias_name = f"test-filter-between-{uuid.uuid4().hex[:8]}"
        interceptor_name = f"{alias_name}_filter"
        group_id = f"test-group-{uuid.uuid4().hex[:8]}"

        try:
            gateway_helper.create_alias_topic(alias_name, physical_topic)
            gateway_helper.create_interceptor(
                name=interceptor_name,
                plugin_class="io.conduktor.gateway.interceptor.VirtualSqlTopicPlugin",
                config={
                    "virtualTopic": alias_name,
                    "statement": f'SELECT * FROM "{physical_topic}" WHERE amount BETWEEN 100 AND 500',
                },
            )

            messages = [
                {"order_id": "1", "amount": 50},
                {"order_id": "2", "amount": 100},
                {"order_id": "3", "amount": 300},
                {"order_id": "4", "amount": 500},
                {"order_id": "5", "amount": 600},
            ]
            kafka_helper.produce_messages(physical_topic, messages)

            received = gateway_helper.consume_through_gateway(
                proxy_bootstrap=INFRA_CONFIG.gateway.proxy_bootstrap,
                topic=alias_name,
                group_id=group_id,
                timeout=15.0,
                expected_count=3,
            )

            assert len(received) == 3
            order_ids = {m["order_id"] for m in received}
            assert order_ids == {"2", "3", "4"}

        finally:
            gateway_helper.delete_interceptor(interceptor_name)
            gateway_helper.delete_alias_topic(alias_name)


# =============================================================================
# GatewayDeployer Integration Tests
# =============================================================================


@pytest.mark.integration
@pytest.mark.gateway
@pytest.mark.kafka
class TestGatewayDeployerE2E:
    """Test GatewayDeployer with actual data flow."""

    def test_deploy_virtual_topic_with_filter(
        self,
        docker_services,
        kafka_helper: KafkaHelper,
        gateway_helper: GatewayHelper,
        physical_topic: str,
    ):
        """Deploy a GatewayRuleArtifact and verify filtering works."""
        rule_name = f"test-deployer-{uuid.uuid4().hex[:8]}"
        group_id = f"test-group-{uuid.uuid4().hex[:8]}"

        artifact = GatewayRuleArtifact(
            name=rule_name,
            virtual_topic=rule_name,
            physical_topic=physical_topic,
            interceptors=[
                {
                    "type": "filter",
                    "config": {"where": "amount > 100"},
                }
            ],
        )

        gateway_config = INFRA_CONFIG.gateway

        try:
            # Deploy using GatewayDeployer
            with GatewayDeployer(
                admin_url=gateway_config.admin_url,
                username=gateway_config.username,
                password=gateway_config.password,
            ) as deployer:
                result = deployer.apply(artifact)
                assert result == "created"

            # Produce messages
            messages = [
                {"order_id": "1", "amount": 50},
                {"order_id": "2", "amount": 150},
                {"order_id": "3", "amount": 75},
                {"order_id": "4", "amount": 200},
            ]
            kafka_helper.produce_messages(physical_topic, messages)

            # Verify filtering through Gateway
            received = gateway_helper.consume_through_gateway(
                proxy_bootstrap=gateway_config.proxy_bootstrap,
                topic=rule_name,
                group_id=group_id,
                timeout=15.0,
                expected_count=2,
            )

            assert len(received) == 2
            order_ids = {m["order_id"] for m in received}
            assert order_ids == {"2", "4"}

        finally:
            with GatewayDeployer(
                admin_url=gateway_config.admin_url,
                username=gateway_config.username,
                password=gateway_config.password,
            ) as deployer:
                deployer.delete(rule_name)

    def test_deployer_idempotent_apply(
        self,
        docker_services,
        gateway_helper: GatewayHelper,
        physical_topic: str,
    ):
        """Applying same artifact twice should be idempotent."""
        rule_name = f"test-idempotent-{uuid.uuid4().hex[:8]}"

        artifact = GatewayRuleArtifact(
            name=rule_name,
            virtual_topic=rule_name,
            physical_topic=physical_topic,
            interceptors=[],
        )

        gateway_config = INFRA_CONFIG.gateway

        try:
            with GatewayDeployer(
                admin_url=gateway_config.admin_url,
                username=gateway_config.username,
                password=gateway_config.password,
            ) as deployer:
                # First apply
                result1 = deployer.apply(artifact)
                assert result1 == "created"

                # Second apply
                result2 = deployer.apply(artifact)
                assert result2 == "updated"

                # Alias should still exist
                alias = gateway_helper.get_alias_topic(rule_name)
                assert alias is not None

        finally:
            with GatewayDeployer(
                admin_url=gateway_config.admin_url,
                username=gateway_config.username,
                password=gateway_config.password,
            ) as deployer:
                deployer.delete(rule_name)


# =============================================================================
# Failure Mode Tests
# =============================================================================


@pytest.mark.integration
@pytest.mark.gateway
class TestGatewayFailureModes:
    """Test error handling and edge cases."""

    def test_consume_nonexistent_alias(
        self,
        docker_services,
        gateway_helper: GatewayHelper,
    ):
        """Consuming from non-existent alias should timeout gracefully."""
        nonexistent = f"nonexistent-{uuid.uuid4().hex[:8]}"
        group_id = f"test-group-{uuid.uuid4().hex[:8]}"

        # Should return empty list (no messages, no topic)
        received = gateway_helper.consume_through_gateway(
            proxy_bootstrap=INFRA_CONFIG.gateway.proxy_bootstrap,
            topic=nonexistent,
            group_id=group_id,
            timeout=5.0,
            max_messages=10,
        )

        # Either empty or might get error - depends on Gateway config
        # The key is it doesn't crash
        assert isinstance(received, list)

    def test_delete_nonexistent_alias(
        self,
        docker_services,
        gateway_helper: GatewayHelper,
    ):
        """Deleting non-existent alias should not raise."""
        nonexistent = f"nonexistent-{uuid.uuid4().hex[:8]}"

        # Should return True (delete succeeded or didn't exist)
        result = gateway_helper.delete_alias_topic(nonexistent)
        assert result is True

    def test_delete_nonexistent_interceptor(
        self,
        docker_services,
        gateway_helper: GatewayHelper,
    ):
        """Deleting non-existent interceptor should not raise."""
        nonexistent = f"nonexistent-interceptor-{uuid.uuid4().hex[:8]}"

        result = gateway_helper.delete_interceptor(nonexistent)
        assert result is True

    def test_filter_all_messages(
        self,
        docker_services,
        kafka_helper: KafkaHelper,
        gateway_helper: GatewayHelper,
        physical_topic: str,
    ):
        """Filter that matches no messages should return empty."""
        alias_name = f"test-filter-all-{uuid.uuid4().hex[:8]}"
        interceptor_name = f"{alias_name}_filter"
        group_id = f"test-group-{uuid.uuid4().hex[:8]}"

        try:
            gateway_helper.create_alias_topic(alias_name, physical_topic)
            gateway_helper.create_interceptor(
                name=interceptor_name,
                plugin_class="io.conduktor.gateway.interceptor.VirtualSqlTopicPlugin",
                config={
                    "virtualTopic": alias_name,
                    "statement": f'SELECT * FROM "{physical_topic}" WHERE amount > 9999999',
                },
            )

            # Produce messages that don't match filter
            messages = [
                {"order_id": "1", "amount": 100},
                {"order_id": "2", "amount": 200},
            ]
            kafka_helper.produce_messages(physical_topic, messages)

            # Should get no messages through filter
            received = gateway_helper.consume_through_gateway(
                proxy_bootstrap=INFRA_CONFIG.gateway.proxy_bootstrap,
                topic=alias_name,
                group_id=group_id,
                timeout=5.0,
                max_messages=10,
            )

            assert len(received) == 0

        finally:
            gateway_helper.delete_interceptor(interceptor_name)
            gateway_helper.delete_alias_topic(alias_name)

    def test_empty_topic(
        self,
        docker_services,
        kafka_helper: KafkaHelper,
        gateway_helper: GatewayHelper,
        physical_topic: str,
    ):
        """Alias to empty topic should return no messages."""
        alias_name = f"test-empty-{uuid.uuid4().hex[:8]}"
        group_id = f"test-group-{uuid.uuid4().hex[:8]}"

        try:
            gateway_helper.create_alias_topic(alias_name, physical_topic)

            # Don't produce any messages - topic is empty

            received = gateway_helper.consume_through_gateway(
                proxy_bootstrap=INFRA_CONFIG.gateway.proxy_bootstrap,
                topic=alias_name,
                group_id=group_id,
                timeout=5.0,
                max_messages=10,
            )

            assert len(received) == 0

        finally:
            gateway_helper.delete_alias_topic(alias_name)


# =============================================================================
# Invalid SQL / Schema Mismatch Tests
# =============================================================================


@pytest.mark.integration
@pytest.mark.gateway
@pytest.mark.kafka
class TestGatewayInvalidSQL:
    """Test Gateway behavior with invalid SQL or schema mismatches.

    These tests verify how Gateway handles:
    - Non-existent columns in WHERE clause
    - Type mismatches (comparing string to number)
    - Invalid SQL syntax
    - Missing fields in messages
    """

    def test_filter_nonexistent_column(
        self,
        docker_services,
        kafka_helper: KafkaHelper,
        gateway_helper: GatewayHelper,
        physical_topic: str,
    ):
        """Filter on column that doesn't exist in messages.

        Gateway behavior varies:
        - May treat missing column as NULL
        - May pass all messages through
        - May filter all messages out

        This test documents the actual behavior.
        """
        alias_name = f"test-nonexistent-col-{uuid.uuid4().hex[:8]}"
        interceptor_name = f"{alias_name}_filter"
        group_id = f"test-group-{uuid.uuid4().hex[:8]}"

        try:
            gateway_helper.create_alias_topic(alias_name, physical_topic)
            gateway_helper.create_interceptor(
                name=interceptor_name,
                plugin_class="io.conduktor.gateway.interceptor.VirtualSqlTopicPlugin",
                config={
                    "virtualTopic": alias_name,
                    # 'nonexistent_field' doesn't exist in any message
                    "statement": f"SELECT * FROM \"{physical_topic}\" WHERE nonexistent_field = 'value'",
                },
            )

            # Messages don't have 'nonexistent_field'
            messages = [
                {"order_id": "1", "amount": 100},
                {"order_id": "2", "amount": 200},
                {"order_id": "3", "amount": 300},
            ]
            kafka_helper.produce_messages(physical_topic, messages)

            received = gateway_helper.consume_through_gateway(
                proxy_bootstrap=INFRA_CONFIG.gateway.proxy_bootstrap,
                topic=alias_name,
                group_id=group_id,
                timeout=10.0,
                max_messages=10,
            )

            # Document actual Gateway behavior:
            # - If Gateway treats missing field as NULL, no messages pass
            # - If Gateway ignores invalid filter, all messages pass
            # Either way, test should not crash
            assert isinstance(received, list)
            # Log for debugging
            print(f"Nonexistent column filter: received {len(received)} messages")

        finally:
            gateway_helper.delete_interceptor(interceptor_name)
            gateway_helper.delete_alias_topic(alias_name)

    def test_filter_column_exists_in_some_messages(
        self,
        docker_services,
        kafka_helper: KafkaHelper,
        gateway_helper: GatewayHelper,
        physical_topic: str,
    ):
        """Filter on column that exists only in some messages.

        Expected: Only messages WITH the column AND matching value pass.
        """
        alias_name = f"test-partial-col-{uuid.uuid4().hex[:8]}"
        interceptor_name = f"{alias_name}_filter"
        group_id = f"test-group-{uuid.uuid4().hex[:8]}"

        try:
            gateway_helper.create_alias_topic(alias_name, physical_topic)
            gateway_helper.create_interceptor(
                name=interceptor_name,
                plugin_class="io.conduktor.gateway.interceptor.VirtualSqlTopicPlugin",
                config={
                    "virtualTopic": alias_name,
                    "statement": f"SELECT * FROM \"{physical_topic}\" WHERE priority = 'high'",
                },
            )

            # Mixed schema - some messages have 'priority', some don't
            messages = [
                {"order_id": "1", "amount": 100},                          # No priority field
                {"order_id": "2", "amount": 200, "priority": "high"},      # Has priority=high
                {"order_id": "3", "amount": 300, "priority": "low"},       # Has priority=low
                {"order_id": "4", "amount": 400},                          # No priority field
                {"order_id": "5", "amount": 500, "priority": "high"},      # Has priority=high
            ]
            kafka_helper.produce_messages(physical_topic, messages)

            received = gateway_helper.consume_through_gateway(
                proxy_bootstrap=INFRA_CONFIG.gateway.proxy_bootstrap,
                topic=alias_name,
                group_id=group_id,
                timeout=15.0,
                expected_count=2,
            )

            # Should only get messages with priority='high'
            assert len(received) == 2
            order_ids = {m["order_id"] for m in received}
            assert order_ids == {"2", "5"}

        finally:
            gateway_helper.delete_interceptor(interceptor_name)
            gateway_helper.delete_alias_topic(alias_name)

    def test_filter_type_mismatch_string_vs_number(
        self,
        docker_services,
        kafka_helper: KafkaHelper,
        gateway_helper: GatewayHelper,
        physical_topic: str,
    ):
        """Filter comparing string field to number.

        Tests: WHERE amount > 100 when amount is stored as string "150"
        """
        alias_name = f"test-type-mismatch-{uuid.uuid4().hex[:8]}"
        interceptor_name = f"{alias_name}_filter"
        group_id = f"test-group-{uuid.uuid4().hex[:8]}"

        try:
            gateway_helper.create_alias_topic(alias_name, physical_topic)
            gateway_helper.create_interceptor(
                name=interceptor_name,
                plugin_class="io.conduktor.gateway.interceptor.VirtualSqlTopicPlugin",
                config={
                    "virtualTopic": alias_name,
                    # Numeric comparison
                    "statement": f'SELECT * FROM "{physical_topic}" WHERE amount > 100',
                },
            )

            # Amount as STRING, not number
            messages = [
                {"order_id": "1", "amount": "50"},    # String "50"
                {"order_id": "2", "amount": "150"},   # String "150"
                {"order_id": "3", "amount": "abc"},   # Non-numeric string
                {"order_id": "4", "amount": 200},     # Actual number
            ]
            kafka_helper.produce_messages(physical_topic, messages)

            received = gateway_helper.consume_through_gateway(
                proxy_bootstrap=INFRA_CONFIG.gateway.proxy_bootstrap,
                topic=alias_name,
                group_id=group_id,
                timeout=10.0,
                max_messages=10,
            )

            # Behavior depends on Gateway's type coercion
            # Document what actually happens
            assert isinstance(received, list)
            print(f"Type mismatch test: received {len(received)} messages")
            for msg in received:
                print(f"  - order_id={msg.get('order_id')}, amount={msg.get('amount')}")

        finally:
            gateway_helper.delete_interceptor(interceptor_name)
            gateway_helper.delete_alias_topic(alias_name)

    def test_filter_nested_field_access(
        self,
        docker_services,
        kafka_helper: KafkaHelper,
        gateway_helper: GatewayHelper,
        physical_topic: str,
    ):
        """Filter on nested field (e.g., metadata.region).

        Tests if Gateway supports JSON path expressions.
        """
        alias_name = f"test-nested-{uuid.uuid4().hex[:8]}"
        interceptor_name = f"{alias_name}_filter"
        group_id = f"test-group-{uuid.uuid4().hex[:8]}"

        try:
            gateway_helper.create_alias_topic(alias_name, physical_topic)
            # Try different nested field syntaxes
            gateway_helper.create_interceptor(
                name=interceptor_name,
                plugin_class="io.conduktor.gateway.interceptor.VirtualSqlTopicPlugin",
                config={
                    "virtualTopic": alias_name,
                    # Attempt nested field access - syntax varies by implementation
                    "statement": f"SELECT * FROM \"{physical_topic}\" WHERE metadata.region = 'EU'",
                },
            )

            messages = [
                {"order_id": "1", "metadata": {"region": "EU", "source": "web"}},
                {"order_id": "2", "metadata": {"region": "US", "source": "mobile"}},
                {"order_id": "3", "metadata": {"region": "EU", "source": "api"}},
            ]
            kafka_helper.produce_messages(physical_topic, messages)

            received = gateway_helper.consume_through_gateway(
                proxy_bootstrap=INFRA_CONFIG.gateway.proxy_bootstrap,
                topic=alias_name,
                group_id=group_id,
                timeout=10.0,
                max_messages=10,
            )

            # Document Gateway's nested field behavior
            assert isinstance(received, list)
            print(f"Nested field test: received {len(received)} messages")

        finally:
            gateway_helper.delete_interceptor(interceptor_name)
            gateway_helper.delete_alias_topic(alias_name)

    def test_filter_special_characters_in_value(
        self,
        docker_services,
        kafka_helper: KafkaHelper,
        gateway_helper: GatewayHelper,
        physical_topic: str,
    ):
        """Filter with special characters in the comparison value.

        Tests SQL injection safety and special char handling.
        """
        alias_name = f"test-special-chars-{uuid.uuid4().hex[:8]}"
        interceptor_name = f"{alias_name}_filter"
        group_id = f"test-group-{uuid.uuid4().hex[:8]}"

        try:
            gateway_helper.create_alias_topic(alias_name, physical_topic)
            gateway_helper.create_interceptor(
                name=interceptor_name,
                plugin_class="io.conduktor.gateway.interceptor.VirtualSqlTopicPlugin",
                config={
                    "virtualTopic": alias_name,
                    # Value with special characters
                    "statement": f"SELECT * FROM \"{physical_topic}\" WHERE name = 'O''Brien'",
                },
            )

            messages = [
                {"order_id": "1", "name": "Smith"},
                {"order_id": "2", "name": "O'Brien"},  # Has apostrophe
                {"order_id": "3", "name": "Johnson"},
            ]
            kafka_helper.produce_messages(physical_topic, messages)

            received = gateway_helper.consume_through_gateway(
                proxy_bootstrap=INFRA_CONFIG.gateway.proxy_bootstrap,
                topic=alias_name,
                group_id=group_id,
                timeout=10.0,
                max_messages=10,
            )

            # Should match O'Brien if SQL escaping works correctly
            assert isinstance(received, list)
            print(f"Special chars test: received {len(received)} messages")

        finally:
            gateway_helper.delete_interceptor(interceptor_name)
            gateway_helper.delete_alias_topic(alias_name)

    def test_filter_empty_string_vs_null(
        self,
        docker_services,
        kafka_helper: KafkaHelper,
        gateway_helper: GatewayHelper,
        physical_topic: str,
    ):
        """Distinguish between empty string and null/missing field.

        Tests: WHERE email = '' vs WHERE email IS NULL
        """
        alias_name = f"test-empty-vs-null-{uuid.uuid4().hex[:8]}"
        interceptor_name = f"{alias_name}_filter"
        group_id = f"test-group-{uuid.uuid4().hex[:8]}"

        try:
            gateway_helper.create_alias_topic(alias_name, physical_topic)
            # Filter for empty string (not null)
            gateway_helper.create_interceptor(
                name=interceptor_name,
                plugin_class="io.conduktor.gateway.interceptor.VirtualSqlTopicPlugin",
                config={
                    "virtualTopic": alias_name,
                    "statement": f"SELECT * FROM \"{physical_topic}\" WHERE email = ''",
                },
            )

            messages = [
                {"user_id": "1", "email": "user@example.com"},  # Has email
                {"user_id": "2", "email": ""},                   # Empty string
                {"user_id": "3", "email": None},                 # Null
                {"user_id": "4"},                                # Missing field
                {"user_id": "5", "email": ""},                   # Empty string
            ]
            kafka_helper.produce_messages(physical_topic, messages)

            received = gateway_helper.consume_through_gateway(
                proxy_bootstrap=INFRA_CONFIG.gateway.proxy_bootstrap,
                topic=alias_name,
                group_id=group_id,
                timeout=10.0,
                max_messages=10,
            )

            # Should only match empty strings (users 2 and 5), not null/missing
            assert isinstance(received, list)
            print(f"Empty vs null test: received {len(received)} messages")
            if received:
                user_ids = {m["user_id"] for m in received}
                print(f"  User IDs: {user_ids}")

        finally:
            gateway_helper.delete_interceptor(interceptor_name)
            gateway_helper.delete_alias_topic(alias_name)

    def test_filter_case_sensitivity(
        self,
        docker_services,
        kafka_helper: KafkaHelper,
        gateway_helper: GatewayHelper,
        physical_topic: str,
    ):
        """Test case sensitivity in string comparisons.

        SQL standard is case-sensitive for string comparisons.
        """
        alias_name = f"test-case-{uuid.uuid4().hex[:8]}"
        interceptor_name = f"{alias_name}_filter"
        group_id = f"test-group-{uuid.uuid4().hex[:8]}"

        try:
            gateway_helper.create_alias_topic(alias_name, physical_topic)
            gateway_helper.create_interceptor(
                name=interceptor_name,
                plugin_class="io.conduktor.gateway.interceptor.VirtualSqlTopicPlugin",
                config={
                    "virtualTopic": alias_name,
                    # Lowercase 'active'
                    "statement": f"SELECT * FROM \"{physical_topic}\" WHERE status = 'active'",
                },
            )

            messages = [
                {"order_id": "1", "status": "active"},    # Exact match
                {"order_id": "2", "status": "ACTIVE"},    # Uppercase
                {"order_id": "3", "status": "Active"},    # Title case
                {"order_id": "4", "status": "inactive"},  # Different value
                {"order_id": "5", "status": "active"},    # Exact match
            ]
            kafka_helper.produce_messages(physical_topic, messages)

            received = gateway_helper.consume_through_gateway(
                proxy_bootstrap=INFRA_CONFIG.gateway.proxy_bootstrap,
                topic=alias_name,
                group_id=group_id,
                timeout=10.0,
                max_messages=10,
            )

            # Standard SQL: only exact case matches (orders 1 and 5)
            # Some systems are case-insensitive
            assert isinstance(received, list)
            print(f"Case sensitivity test: received {len(received)} messages")
            if received:
                order_ids = {m["order_id"] for m in received}
                print(f"  Order IDs: {order_ids}")
                # Document actual behavior
                if order_ids == {"1", "5"}:
                    print("  -> Gateway is CASE-SENSITIVE")
                elif order_ids == {"1", "2", "3", "5"}:
                    print("  -> Gateway is CASE-INSENSITIVE")

        finally:
            gateway_helper.delete_interceptor(interceptor_name)
            gateway_helper.delete_alias_topic(alias_name)


# =============================================================================
# Cleanup Tests
# =============================================================================


@pytest.mark.integration
@pytest.mark.gateway
@pytest.mark.kafka
class TestGatewayCleanup:
    """Test cleanup utilities for test isolation."""

    def test_cleanup_by_prefix(
        self,
        docker_services,
        kafka_helper: KafkaHelper,
        gateway_helper: GatewayHelper,
    ):
        """Cleanup should only delete resources with matching prefix."""
        # Create physical topics first (Gateway validates they exist)
        physical1 = f"physical-cleanup-1-{uuid.uuid4().hex[:8]}"
        physical2 = f"physical-cleanup-2-{uuid.uuid4().hex[:8]}"
        physical3 = f"physical-cleanup-3-{uuid.uuid4().hex[:8]}"
        kafka_helper.create_topic(physical1, partitions=1)
        kafka_helper.create_topic(physical2, partitions=1)
        kafka_helper.create_topic(physical3, partitions=1)

        # Create test resources
        test_alias1 = f"test-cleanup-a-{uuid.uuid4().hex[:8]}"
        test_alias2 = f"test-cleanup-b-{uuid.uuid4().hex[:8]}"
        prod_alias = f"prod-{uuid.uuid4().hex[:8]}"

        test_interceptor = f"test-cleanup-int-{uuid.uuid4().hex[:8]}"
        prod_interceptor = f"prod-int-{uuid.uuid4().hex[:8]}"

        try:
            gateway_helper.create_alias_topic(test_alias1, physical1)
            gateway_helper.create_alias_topic(test_alias2, physical2)
            gateway_helper.create_alias_topic(prod_alias, physical3)

            gateway_helper.create_interceptor(
                test_interceptor,
                "io.conduktor.gateway.interceptor.safeguard.ClientIdRequiredPolicyPlugin",
                {},
            )
            gateway_helper.create_interceptor(
                prod_interceptor,
                "io.conduktor.gateway.interceptor.safeguard.ClientIdRequiredPolicyPlugin",
                {},
            )

            # Cleanup test- prefix only
            gateway_helper.cleanup_test_resources(prefix="test-")

            # Test resources should be deleted
            assert gateway_helper.get_alias_topic(test_alias1) is None
            assert gateway_helper.get_alias_topic(test_alias2) is None
            assert gateway_helper.get_interceptor(test_interceptor) is None

            # Prod resources should remain
            assert gateway_helper.get_alias_topic(prod_alias) is not None
            assert gateway_helper.get_interceptor(prod_interceptor) is not None

        finally:
            # Manual cleanup of prod resources
            gateway_helper.delete_alias_topic(prod_alias)
            gateway_helper.delete_interceptor(prod_interceptor)
            # Cleanup physical topics
            kafka_helper.delete_topic(physical1)
            kafka_helper.delete_topic(physical2)
            kafka_helper.delete_topic(physical3)
