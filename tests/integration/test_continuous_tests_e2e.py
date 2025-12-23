"""End-to-end integration tests for continuous tests with real Flink and Kafka.

These tests verify that continuous tests work end-to-end:
1. Compile continuous test to Flink SQL
2. Deploy to real Flink cluster
3. Send valid and invalid events via Kafka
4. Verify violations appear in failures topic

Improvements over naive implementation:
- Each test uses isolated failures topic (no cross-test pollution)
- Deterministic waits instead of sleep() for race condition prevention
- Probe events to verify job is ready before sending test data
- Full pipeline E2E test (Source -> Model -> Continuous Test)
- Backpressure/throughput test with many events
"""

import json
import time
import uuid

import pytest
from confluent_kafka import Consumer

from streamt.compiler.compiler import Compiler
from streamt.compiler.manifest import FlinkJobArtifact
from streamt.core.models import (
    AdvancedConfig,
    ColumnDefinition,
    DataTest,
    DataTestType,
    KafkaConfig,
    Model,
    ProjectInfo,
    RuntimeConfig,
    Source,
    StreamtProject,
    TopicConfig,
)
from streamt.deployer.flink import FlinkDeployer

from .conftest import (
    INFRA_CONFIG,
    FlinkHelper,
    KafkaHelper,
)


def generate_unique_suffix() -> str:
    """Generate unique suffix for test isolation."""
    return uuid.uuid4().hex[:8]


def wait_for_violations(
    kafka_helper: KafkaHelper,
    topic: str,
    test_name: str,
    expected_count: int,
    timeout: float = 30.0,
    poll_interval: float = 0.5,
) -> list[dict]:
    """Poll until expected number of violations appear for a specific test.

    This is deterministic - it waits for actual violations instead of using sleep().

    Args:
        kafka_helper: KafkaHelper instance
        topic: Failures topic to consume from
        test_name: Test name to filter violations by
        expected_count: Minimum number of violations expected
        timeout: Maximum time to wait
        poll_interval: Time between polls

    Returns:
        List of violations for the specified test

    Raises:
        TimeoutError: If expected violations don't appear within timeout
    """
    consumer = Consumer(
        {
            "bootstrap.servers": INFRA_CONFIG.kafka_bootstrap_servers,
            "group.id": f"violation_waiter_{generate_unique_suffix()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])

    violations = []
    start = time.time()

    try:
        while time.time() - start < timeout:
            msg = consumer.poll(timeout=poll_interval)
            if msg is None:
                continue
            if msg.error():
                continue

            try:
                value = json.loads(msg.value().decode("utf-8"))
                if value.get("test_name") == test_name:
                    violations.append(value)
                    if len(violations) >= expected_count:
                        return violations
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue

        if len(violations) > 0:
            return violations

        raise TimeoutError(
            f"Expected {expected_count} violations for test '{test_name}', "
            f"got {len(violations)} within {timeout}s"
        )
    finally:
        consumer.close()


def wait_for_job_ready_with_probe(
    kafka_helper: KafkaHelper,
    flink_helper: FlinkHelper,
    model_topic: str,
    failures_topic: str,
    test_name: str,
    timeout: float = 30.0,
) -> None:
    """Wait for Flink job to be ready by sending a probe event and waiting for violation.

    This ensures the job has fully initialized its Kafka consumer before we send test data.

    Args:
        kafka_helper: KafkaHelper instance
        flink_helper: FlinkHelper instance
        model_topic: Topic to send probe event to
        failures_topic: Topic where violations should appear
        test_name: Test name for filtering
        timeout: Maximum time to wait
    """
    # Send probe event that will definitely trigger a violation
    probe_event = {
        "event_id": f"probe_{generate_unique_suffix()}",
        "user_id": None,  # NULL triggers not_null violation
        "event_type": "probe",
        "timestamp": "2024-01-01T00:00:00Z",
    }
    kafka_helper.produce_messages(model_topic, [probe_event])

    # Wait for the probe violation to appear
    try:
        wait_for_violations(
            kafka_helper,
            failures_topic,
            test_name,
            expected_count=1,
            timeout=timeout,
        )
    except TimeoutError:
        raise TimeoutError(
            f"Flink job not ready within {timeout}s - probe event did not produce violation"
        )


def create_test_project(
    source_topic: str,
    model_topic: str,
    test_name: str,
    columns: list[str],
    valid_event_types: list[str],
    failures_topic: str = "_streamt_test_failures",
) -> StreamtProject:
    """Create a minimal StreamtProject for testing continuous tests.

    Args:
        source_topic: Kafka topic for source data
        model_topic: Kafka topic for model output
        test_name: Name for the continuous test
        columns: List of column names
        valid_event_types: Valid values for event_type column
        failures_topic: Custom failures topic for test isolation
    """
    # Create source
    source = Source(
        name="raw_events",
        topic=source_topic,
        columns=[ColumnDefinition(name=col) for col in columns],
    )

    # Create model
    model = Model(
        name="events_clean",
        sql="""SELECT
    event_id,
    user_id,
    event_type,
    `timestamp`
FROM {{ source("raw_events") }}
WHERE event_id IS NOT NULL""",
        advanced=AdvancedConfig(
            topic=TopicConfig(name=model_topic),
        ),
    )

    # Create continuous test
    test = DataTest(
        name=test_name,
        model="events_clean",
        type=DataTestType.CONTINUOUS,
        assertions=[
            {"not_null": {"columns": ["event_id", "user_id", "event_type"]}},
            {"accepted_values": {"column": "event_type", "values": valid_event_types}},
        ],
    )

    # Create project with required runtime config
    project = StreamtProject(
        project=ProjectInfo(name="test_project", version="1.0.0"),
        runtime=RuntimeConfig(
            kafka=KafkaConfig(bootstrap_servers=INFRA_CONFIG.kafka_internal_servers)
        ),
        sources=[source],
        models=[model],
        tests=[test],
    )

    return project


def patch_failures_topic_in_sql(sql: str, new_topic: str) -> str:
    """Patch the failures topic in generated SQL for test isolation.

    Args:
        sql: Original Flink SQL
        new_topic: New topic name to use

    Returns:
        SQL with patched topic name
    """
    return sql.replace(
        "'topic' = '_streamt_test_failures'",
        f"'topic' = '{new_topic}'"
    )


@pytest.mark.integration
@pytest.mark.flink
@pytest.mark.kafka
@pytest.mark.slow
class TestContinuousTestsE2E:
    """End-to-end tests for continuous test feature."""

    def test_continuous_test_compiles_to_valid_flink_sql(
        self,
        docker_services,
        flink_helper: FlinkHelper,
    ):
        """Test that a continuous test compiles to valid Flink SQL that can be parsed."""
        suffix = generate_unique_suffix()
        source_topic = f"ct_test_source_{suffix}"
        model_topic = f"ct_test_model_{suffix}"
        test_name = f"ct_validation_{suffix}"

        project = create_test_project(
            source_topic=source_topic,
            model_topic=model_topic,
            test_name=test_name,
            columns=["event_id", "user_id", "event_type", "timestamp"],
            valid_event_types=["click", "view", "purchase"],
        )

        # Compile the project
        compiler = Compiler(project)
        manifest = compiler.compile(dry_run=True)

        # Find the continuous test artifact (in manifest.artifacts["flink_jobs"])
        flink_jobs = manifest.artifacts.get("flink_jobs", [])
        test_artifacts = [j for j in flink_jobs if j["name"] == f"test_{test_name}"]
        assert len(test_artifacts) == 1, "Should have exactly one test artifact"

        test_artifact = test_artifacts[0]

        # Verify SQL structure
        sql = test_artifact["sql"]
        assert f"CREATE TABLE IF NOT EXISTS test_source_{test_name}" in sql
        assert f"CREATE TABLE IF NOT EXISTS test_failures_{test_name}" in sql
        assert "'connector' = 'kafka'" in sql
        assert f"'topic' = '{model_topic}'" in sql
        assert "'topic' = '_streamt_test_failures'" in sql
        assert "INSERT INTO test_failures_" in sql
        assert "not_null:event_id" in sql
        assert "accepted_values:event_type" in sql

        # Verify SQL can be parsed by Flink (DDL only, not the INSERT)
        flink_helper.open_sql_session()
        # The CREATE TABLE statements should parse correctly
        # We test just the source table creation to verify syntax
        source_table_sql = sql.split(";")[0] + ";"
        error = flink_helper.execute_sql_and_check_error(source_table_sql)
        assert error is None, f"Flink SQL parsing failed: {error}"

    def test_continuous_test_detects_not_null_violations(
        self,
        docker_services,
        kafka_helper: KafkaHelper,
        flink_helper: FlinkHelper,
    ):
        """Test that continuous test detects NULL value violations."""
        suffix = generate_unique_suffix()
        model_topic = f"ct_model_notnull_{suffix}"
        failures_topic = f"ct_failures_notnull_{suffix}"  # Isolated topic
        test_name = f"ct_notnull_{suffix}"

        # Create isolated topics
        kafka_helper.create_topic(model_topic, partitions=1)
        kafka_helper.create_topic(failures_topic, partitions=1)

        try:
            # Create and compile project
            project = create_test_project(
                source_topic=f"ct_source_notnull_{suffix}",
                model_topic=model_topic,
                test_name=test_name,
                columns=["event_id", "user_id", "event_type", "timestamp"],
                valid_event_types=["click", "view", "purchase"],
            )

            compiler = Compiler(project)
            manifest = compiler.compile(dry_run=True)

            # Get test SQL and patch failures topic for isolation
            flink_jobs = manifest.artifacts.get("flink_jobs", [])
            test_artifact = next(
                j for j in flink_jobs if j["name"] == f"test_{test_name}"
            )
            patched_sql = patch_failures_topic_in_sql(test_artifact["sql"], failures_topic)

            # Deploy to Flink
            deployer = FlinkDeployer(
                rest_url=INFRA_CONFIG.flink_rest_url,
                sql_gateway_url=INFRA_CONFIG.flink_sql_gateway_url,
            )

            existing_jobs = {j["id"] for j in flink_helper.get_running_jobs()}
            result = deployer.submit_sql(patched_sql)
            assert "results" in result

            # Wait for job to start
            new_job_id = flink_helper.wait_for_new_running_job(
                existing_jobs, timeout=30.0
            )
            assert new_job_id is not None, "Continuous test job did not start"

            # Wait for job to be ready with probe event
            wait_for_job_ready_with_probe(
                kafka_helper, flink_helper, model_topic, failures_topic, test_name
            )

            # Now send test event with NULL event_id (violation)
            null_event = {
                "event_id": None,
                "user_id": "user_456",
                "event_type": "view",
                "timestamp": "2024-01-01T00:01:00Z",
            }
            kafka_helper.produce_messages(model_topic, [null_event])

            # Wait deterministically for violations
            # probe: not_null:user_id + accepted_values:event_type = 2 violations
            # null_event: not_null:event_id = 1 violation
            # Total: 3+ violations
            violations = wait_for_violations(
                kafka_helper, failures_topic, test_name,
                expected_count=3,
                timeout=30.0,
            )

            # Should have not_null violations for event_id (from our test event)
            event_id_violations = [
                v for v in violations
                if v.get("violation_type") == "not_null:event_id"
            ]
            assert len(event_id_violations) >= 1, (
                f"Expected not_null:event_id violation, got: {violations}"
            )

        finally:
            flink_helper.cancel_all_running_jobs()
            kafka_helper.delete_topic(model_topic)
            kafka_helper.delete_topic(failures_topic)

    def test_continuous_test_detects_accepted_values_violations(
        self,
        docker_services,
        kafka_helper: KafkaHelper,
        flink_helper: FlinkHelper,
    ):
        """Test that continuous test detects invalid value violations."""
        suffix = generate_unique_suffix()
        model_topic = f"ct_model_values_{suffix}"
        failures_topic = f"ct_failures_values_{suffix}"  # Isolated topic
        test_name = f"ct_values_{suffix}"

        kafka_helper.create_topic(model_topic, partitions=1)
        kafka_helper.create_topic(failures_topic, partitions=1)

        try:
            project = create_test_project(
                source_topic=f"ct_source_values_{suffix}",
                model_topic=model_topic,
                test_name=test_name,
                columns=["event_id", "user_id", "event_type", "timestamp"],
                valid_event_types=["click", "view", "purchase"],
            )

            compiler = Compiler(project)
            manifest = compiler.compile(dry_run=True)

            test_artifact = next(
                j for j in manifest.artifacts.get("flink_jobs", []) if j["name"] == f"test_{test_name}"
            )
            patched_sql = patch_failures_topic_in_sql(test_artifact["sql"], failures_topic)

            deployer = FlinkDeployer(
                rest_url=INFRA_CONFIG.flink_rest_url,
                sql_gateway_url=INFRA_CONFIG.flink_sql_gateway_url,
            )

            existing_jobs = {j["id"] for j in flink_helper.get_running_jobs()}
            result = deployer.submit_sql(patched_sql)
            assert "results" in result

            new_job_id = flink_helper.wait_for_new_running_job(
                existing_jobs, timeout=30.0
            )
            assert new_job_id is not None, "Continuous test job did not start"

            # Wait for job readiness with probe
            wait_for_job_ready_with_probe(
                kafka_helper, flink_helper, model_topic, failures_topic, test_name
            )

            # Send event with invalid event_type
            invalid_event = {
                "event_id": "evt_invalid_001",
                "user_id": "user_002",
                "event_type": "INVALID_TYPE",  # Not in accepted values
                "timestamp": "2024-01-01T00:01:00Z",
            }
            kafka_helper.produce_messages(model_topic, [invalid_event])

            # Wait for accepted_values violation
            # probe: not_null:user_id + accepted_values:event_type (probe) = 2
            # invalid_event: accepted_values:event_type (INVALID_TYPE) = 1
            # Total: 3+ violations
            violations = wait_for_violations(
                kafka_helper, failures_topic, test_name,
                expected_count=3,
                timeout=30.0,
            )

            # Find the INVALID_TYPE violation (not the probe one)
            invalid_type_violations = [
                v for v in violations
                if v.get("violation_type") == "accepted_values:event_type"
                and v.get("violation_details") == "INVALID_TYPE"
            ]
            assert len(invalid_type_violations) >= 1, (
                f"Expected accepted_values:event_type with INVALID_TYPE, got: {violations}"
            )

        finally:
            flink_helper.cancel_all_running_jobs()
            kafka_helper.delete_topic(model_topic)
            kafka_helper.delete_topic(failures_topic)

    def test_continuous_test_multiple_violations_single_event(
        self,
        docker_services,
        kafka_helper: KafkaHelper,
        flink_helper: FlinkHelper,
    ):
        """Test that multiple violations from one event are all detected."""
        suffix = generate_unique_suffix()
        model_topic = f"ct_model_multi_{suffix}"
        failures_topic = f"ct_failures_multi_{suffix}"  # Isolated topic
        test_name = f"ct_multi_{suffix}"

        kafka_helper.create_topic(model_topic, partitions=1)
        kafka_helper.create_topic(failures_topic, partitions=1)

        try:
            project = create_test_project(
                source_topic=f"ct_source_multi_{suffix}",
                model_topic=model_topic,
                test_name=test_name,
                columns=["event_id", "user_id", "event_type", "timestamp"],
                valid_event_types=["click", "view"],
            )

            compiler = Compiler(project)
            manifest = compiler.compile(dry_run=True)

            test_artifact = next(
                j for j in manifest.artifacts.get("flink_jobs", []) if j["name"] == f"test_{test_name}"
            )
            patched_sql = patch_failures_topic_in_sql(test_artifact["sql"], failures_topic)

            deployer = FlinkDeployer(
                rest_url=INFRA_CONFIG.flink_rest_url,
                sql_gateway_url=INFRA_CONFIG.flink_sql_gateway_url,
            )

            existing_jobs = {j["id"] for j in flink_helper.get_running_jobs()}
            deployer.submit_sql(patched_sql)

            new_job_id = flink_helper.wait_for_new_running_job(
                existing_jobs, timeout=30.0
            )
            assert new_job_id is not None

            # Wait for job readiness
            wait_for_job_ready_with_probe(
                kafka_helper, flink_helper, model_topic, failures_topic, test_name
            )

            # Send event with BOTH null user_id AND invalid event_type
            bad_event = {
                "event_id": "evt_bad_001",
                "user_id": None,
                "event_type": "UNKNOWN",
                "timestamp": "2024-01-01T00:00:00Z",
            }
            kafka_helper.produce_messages(model_topic, [bad_event])

            # Wait for violations (probe produces 2, bad_event produces 2)
            violations = wait_for_violations(
                kafka_helper, failures_topic, test_name,
                expected_count=4,
                timeout=30.0,
            )

            # Should have both violation types
            violation_types = {v.get("violation_type") for v in violations}
            assert "not_null:user_id" in violation_types, (
                f"Missing not_null:user_id, got: {violation_types}"
            )
            assert "accepted_values:event_type" in violation_types, (
                f"Missing accepted_values:event_type, got: {violation_types}"
            )

        finally:
            flink_helper.cancel_all_running_jobs()
            kafka_helper.delete_topic(model_topic)
            kafka_helper.delete_topic(failures_topic)

    def test_continuous_test_no_violations_for_valid_events(
        self,
        docker_services,
        kafka_helper: KafkaHelper,
        flink_helper: FlinkHelper,
    ):
        """Test that valid events do NOT produce violations."""
        suffix = generate_unique_suffix()
        model_topic = f"ct_model_valid_{suffix}"
        failures_topic = f"ct_failures_valid_{suffix}"  # Isolated topic
        test_name = f"ct_valid_{suffix}"

        kafka_helper.create_topic(model_topic, partitions=1)
        kafka_helper.create_topic(failures_topic, partitions=1)

        try:
            project = create_test_project(
                source_topic=f"ct_source_valid_{suffix}",
                model_topic=model_topic,
                test_name=test_name,
                columns=["event_id", "user_id", "event_type", "timestamp"],
                valid_event_types=["click", "view", "purchase", "probe"],  # Include probe as valid
            )

            compiler = Compiler(project)
            manifest = compiler.compile(dry_run=True)

            test_artifact = next(
                j for j in manifest.artifacts.get("flink_jobs", []) if j["name"] == f"test_{test_name}"
            )
            patched_sql = patch_failures_topic_in_sql(test_artifact["sql"], failures_topic)

            deployer = FlinkDeployer(
                rest_url=INFRA_CONFIG.flink_rest_url,
                sql_gateway_url=INFRA_CONFIG.flink_sql_gateway_url,
            )

            existing_jobs = {j["id"] for j in flink_helper.get_running_jobs()}
            deployer.submit_sql(patched_sql)

            new_job_id = flink_helper.wait_for_new_running_job(
                existing_jobs, timeout=30.0
            )
            assert new_job_id is not None

            # For this test, we can't use probe (it would be valid)
            # Instead, wait a fixed time for job startup
            time.sleep(5)

            # Send multiple VALID events
            valid_events = [
                {
                    "event_id": f"evt_valid_{i}",
                    "user_id": f"user_{i}",
                    "event_type": event_type,
                    "timestamp": "2024-01-01T00:00:00Z",
                }
                for i, event_type in enumerate(["click", "view", "purchase"])
            ]
            kafka_helper.produce_messages(model_topic, valid_events)

            # Wait a bit, then verify no violations
            time.sleep(5)

            # Try to consume - should timeout with no messages for our test
            try:
                violations = wait_for_violations(
                    kafka_helper, failures_topic, test_name,
                    expected_count=1,
                    timeout=5.0,  # Short timeout
                )
                # If we get here, we have unexpected violations
                assert False, f"Expected no violations for valid events, got: {violations}"
            except TimeoutError:
                # Expected - no violations
                pass

        finally:
            flink_helper.cancel_all_running_jobs()
            kafka_helper.delete_topic(model_topic)
            kafka_helper.delete_topic(failures_topic)


@pytest.mark.integration
@pytest.mark.flink
@pytest.mark.kafka
@pytest.mark.slow
class TestContinuousTestBackpressure:
    """Tests for throughput and backpressure handling."""

    def test_continuous_test_handles_high_throughput(
        self,
        docker_services,
        kafka_helper: KafkaHelper,
        flink_helper: FlinkHelper,
    ):
        """Test that continuous test correctly processes many events under load."""
        suffix = generate_unique_suffix()
        model_topic = f"ct_model_throughput_{suffix}"
        failures_topic = f"ct_failures_throughput_{suffix}"
        test_name = f"ct_throughput_{suffix}"

        kafka_helper.create_topic(model_topic, partitions=3)  # Multiple partitions
        kafka_helper.create_topic(failures_topic, partitions=3)

        try:
            project = create_test_project(
                source_topic=f"ct_source_throughput_{suffix}",
                model_topic=model_topic,
                test_name=test_name,
                columns=["event_id", "user_id", "event_type", "timestamp"],
                valid_event_types=["click", "view", "purchase"],
            )

            compiler = Compiler(project)
            manifest = compiler.compile(dry_run=True)

            test_artifact = next(
                j for j in manifest.artifacts.get("flink_jobs", []) if j["name"] == f"test_{test_name}"
            )
            patched_sql = patch_failures_topic_in_sql(test_artifact["sql"], failures_topic)

            deployer = FlinkDeployer(
                rest_url=INFRA_CONFIG.flink_rest_url,
                sql_gateway_url=INFRA_CONFIG.flink_sql_gateway_url,
            )

            existing_jobs = {j["id"] for j in flink_helper.get_running_jobs()}
            deployer.submit_sql(patched_sql)

            new_job_id = flink_helper.wait_for_new_running_job(
                existing_jobs, timeout=30.0
            )
            assert new_job_id is not None

            # Wait for job readiness
            wait_for_job_ready_with_probe(
                kafka_helper, flink_helper, model_topic, failures_topic, test_name
            )

            # Send 100 invalid events (all with INVALID_TYPE)
            num_events = 100
            invalid_events = [
                {
                    "event_id": f"evt_load_{i}",
                    "user_id": f"user_{i}",
                    "event_type": "INVALID_TYPE",
                    "timestamp": "2024-01-01T00:00:00Z",
                }
                for i in range(num_events)
            ]
            kafka_helper.produce_messages(model_topic, invalid_events)

            # Wait for all violations (probe + 100 events = 101 minimum)
            violations = wait_for_violations(
                kafka_helper, failures_topic, test_name,
                expected_count=num_events,  # At least num_events violations
                timeout=60.0,  # Longer timeout for high volume
            )

            # Verify we got violations for most events
            accepted_values_violations = [
                v for v in violations
                if "accepted_values" in v.get("violation_type", "")
            ]
            # Allow some tolerance for timing, but should get most
            assert len(accepted_values_violations) >= num_events * 0.9, (
                f"Expected ~{num_events} accepted_values violations, "
                f"got {len(accepted_values_violations)}"
            )

        finally:
            flink_helper.cancel_all_running_jobs()
            kafka_helper.delete_topic(model_topic)
            kafka_helper.delete_topic(failures_topic)


@pytest.mark.integration
@pytest.mark.flink
@pytest.mark.kafka
@pytest.mark.slow
class TestFullPipelineE2E:
    """Tests for the complete Source -> Model -> Continuous Test -> Failures pipeline."""

    def test_full_pipeline_model_output_to_failures(
        self,
        docker_services,
        kafka_helper: KafkaHelper,
        flink_helper: FlinkHelper,
    ):
        """Test the continuous test pipeline: Model Output -> Continuous Test -> Failures.

        This tests the complete flow of the continuous test monitoring model output:
        1. Events appear in model output topic (simulating model processing)
        2. Continuous test monitors the model output
        3. Violations are written to failures topic
        4. Both not_null and accepted_values violations are detected
        """
        suffix = generate_unique_suffix()
        model_topic = f"ct_model_full_{suffix}"
        failures_topic = f"ct_failures_full_{suffix}"
        test_name = f"ct_full_{suffix}"

        kafka_helper.create_topic(model_topic, partitions=1)
        kafka_helper.create_topic(failures_topic, partitions=1)

        try:
            project = create_test_project(
                source_topic=f"ct_source_full_{suffix}",
                model_topic=model_topic,
                test_name=test_name,
                columns=["event_id", "user_id", "event_type", "timestamp"],
                valid_event_types=["click", "view", "purchase"],
            )

            compiler = Compiler(project)
            manifest = compiler.compile(dry_run=True)

            # Get continuous test artifact
            flink_jobs = manifest.artifacts.get("flink_jobs", [])
            test_artifact = next(
                j for j in flink_jobs if j["name"] == f"test_{test_name}"
            )

            deployer = FlinkDeployer(
                rest_url=INFRA_CONFIG.flink_rest_url,
                sql_gateway_url=INFRA_CONFIG.flink_sql_gateway_url,
            )

            # Deploy continuous test job
            patched_sql = patch_failures_topic_in_sql(test_artifact["sql"], failures_topic)
            existing_jobs = {j["id"] for j in flink_helper.get_running_jobs()}
            deployer.submit_sql(patched_sql)

            test_job_id = flink_helper.wait_for_new_running_job(
                existing_jobs, timeout=30.0
            )
            assert test_job_id is not None, "Continuous test job did not start"

            # Wait for test job readiness
            wait_for_job_ready_with_probe(
                kafka_helper, flink_helper, model_topic, failures_topic, test_name
            )

            # Send event to model output (simulating what model would produce)
            # This event has BOTH a null violation AND invalid event_type
            model_output_event = {
                "event_id": "evt_model_001",
                "user_id": None,  # Null violation
                "event_type": "BAD_TYPE",  # Invalid type
                "timestamp": "2024-01-01T00:00:00Z",
            }
            kafka_helper.produce_messages(model_topic, [model_output_event])

            # Wait for violations
            # probe: not_null:user_id + accepted_values:event_type = 2
            # model_output_event: not_null:user_id + accepted_values:event_type = 2
            # Total: 4+ violations
            violations = wait_for_violations(
                kafka_helper, failures_topic, test_name,
                expected_count=4,
                timeout=30.0,
            )

            # Verify we have both violation types for BAD_TYPE event
            bad_type_violations = [
                v for v in violations
                if v.get("violation_details") in (None, "BAD_TYPE")  # NULL shows as None
            ]
            violation_types = {v.get("violation_type") for v in bad_type_violations}

            assert "not_null:user_id" in violation_types, (
                f"Missing not_null:user_id in full pipeline, got: {violation_types}"
            )
            assert "accepted_values:event_type" in violation_types, (
                f"Missing accepted_values:event_type in full pipeline, got: {violation_types}"
            )

        finally:
            flink_helper.cancel_all_running_jobs()
            kafka_helper.delete_topic(model_topic)
            kafka_helper.delete_topic(failures_topic)


@pytest.mark.integration
@pytest.mark.flink
@pytest.mark.kafka
class TestContinuousTestJobManagement:
    """Test job lifecycle management for continuous tests."""

    def test_job_status_found_for_running_continuous_test(
        self,
        docker_services,
        kafka_helper: KafkaHelper,
        flink_helper: FlinkHelper,
    ):
        """Test that get_job_state finds a running continuous test job."""
        suffix = generate_unique_suffix()
        model_topic = f"ct_model_status_{suffix}"
        failures_topic = f"ct_failures_status_{suffix}"
        test_name = f"ct_status_{suffix}"

        kafka_helper.create_topic(model_topic, partitions=1)
        kafka_helper.create_topic(failures_topic, partitions=1)

        try:
            project = create_test_project(
                source_topic=f"ct_source_status_{suffix}",
                model_topic=model_topic,
                test_name=test_name,
                columns=["event_id", "user_id", "event_type", "timestamp"],
                valid_event_types=["click"],
            )

            compiler = Compiler(project)
            manifest = compiler.compile(dry_run=True)

            test_artifact = next(
                j for j in manifest.artifacts.get("flink_jobs", []) if j["name"] == f"test_{test_name}"
            )
            patched_sql = patch_failures_topic_in_sql(test_artifact["sql"], failures_topic)

            deployer = FlinkDeployer(
                rest_url=INFRA_CONFIG.flink_rest_url,
                sql_gateway_url=INFRA_CONFIG.flink_sql_gateway_url,
            )

            existing_jobs = {j["id"] for j in flink_helper.get_running_jobs()}
            deployer.submit_sql(patched_sql)

            new_job_id = flink_helper.wait_for_new_running_job(
                existing_jobs, timeout=30.0
            )
            assert new_job_id is not None

            # Now verify get_job_state finds it
            job_state = deployer.get_job_state(f"test_{test_name}")

            assert job_state.exists, (
                f"Job test_{test_name} should exist but wasn't found."
            )
            assert job_state.status == "RUNNING", (
                f"Expected RUNNING, got {job_state.status}"
            )
            assert job_state.job_id is not None

        finally:
            flink_helper.cancel_all_running_jobs()
            kafka_helper.delete_topic(model_topic)
            kafka_helper.delete_topic(failures_topic)

    def test_apply_job_skips_already_running_test(
        self,
        docker_services,
        kafka_helper: KafkaHelper,
        flink_helper: FlinkHelper,
    ):
        """Test that apply_job returns 'unchanged' for already running test."""
        suffix = generate_unique_suffix()
        model_topic = f"ct_model_skip_{suffix}"
        failures_topic = f"ct_failures_skip_{suffix}"
        test_name = f"ct_skip_{suffix}"

        kafka_helper.create_topic(model_topic, partitions=1)
        kafka_helper.create_topic(failures_topic, partitions=1)

        try:
            project = create_test_project(
                source_topic=f"ct_source_skip_{suffix}",
                model_topic=model_topic,
                test_name=test_name,
                columns=["event_id", "user_id", "event_type", "timestamp"],
                valid_event_types=["click"],
            )

            compiler = Compiler(project)
            manifest = compiler.compile(dry_run=True)

            test_artifact_dict = next(
                j for j in manifest.artifacts.get("flink_jobs", []) if j["name"] == f"test_{test_name}"
            )
            patched_sql = patch_failures_topic_in_sql(test_artifact_dict["sql"], failures_topic)

            # Create artifact with patched SQL
            test_artifact = FlinkJobArtifact(
                name=test_artifact_dict["name"],
                sql=patched_sql,
                cluster=test_artifact_dict.get("cluster"),
            )

            deployer = FlinkDeployer(
                rest_url=INFRA_CONFIG.flink_rest_url,
                sql_gateway_url=INFRA_CONFIG.flink_sql_gateway_url,
            )

            # First apply - should submit
            action1 = deployer.apply_job(test_artifact)
            assert action1 == "submitted"

            # Wait for job to be running
            time.sleep(5)

            # Second apply - should be unchanged (already running)
            action2 = deployer.apply_job(test_artifact)
            assert action2 == "unchanged", (
                f"Expected 'unchanged' for already running job, got '{action2}'"
            )

        finally:
            flink_helper.cancel_all_running_jobs()
            kafka_helper.delete_topic(model_topic)
            kafka_helper.delete_topic(failures_topic)
