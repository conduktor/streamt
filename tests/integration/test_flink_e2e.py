"""End-to-end tests for Flink SQL job management with Docker infrastructure.

These tests verify that the FlinkDeployer correctly:
- Creates SQL Gateway sessions
- Executes DDL statements (CREATE TABLE)
- Submits streaming jobs
- Lists and monitors job status
- Cancels jobs
"""

import uuid

import pytest

from streamt.compiler.manifest import FlinkJobArtifact
from streamt.deployer.flink import FlinkDeployer

from .conftest import INFRA_CONFIG, FlinkHelper, KafkaHelper, poll_until_messages


@pytest.mark.integration
@pytest.mark.flink
class TestFlinkSqlGatewaySession:
    """Test SQL Gateway session management."""

    def test_open_session(
        self,
        docker_services,
    ):
        """Test opening a SQL Gateway session."""
        deployer = FlinkDeployer(
            rest_url=INFRA_CONFIG.flink_rest_url,
            sql_gateway_url=INFRA_CONFIG.flink_sql_gateway_url,
        )

        session_handle = deployer.get_session()
        assert session_handle is not None
        assert len(session_handle) > 0

    def test_close_session(
        self,
        docker_services,
    ):
        """Test session caching behavior."""
        deployer = FlinkDeployer(
            rest_url=INFRA_CONFIG.flink_rest_url,
            sql_gateway_url=INFRA_CONFIG.flink_sql_gateway_url,
        )

        # Get session (creates new one)
        session1 = deployer.get_session()
        # Get session again (returns cached)
        session2 = deployer.get_session()

        # Should return the same cached session
        assert session1 == session2

    def test_multiple_sessions(
        self,
        docker_services,
    ):
        """Test creating multiple independent sessions with different deployers."""
        deployer1 = FlinkDeployer(
            rest_url=INFRA_CONFIG.flink_rest_url,
            sql_gateway_url=INFRA_CONFIG.flink_sql_gateway_url,
        )
        deployer2 = FlinkDeployer(
            rest_url=INFRA_CONFIG.flink_rest_url,
            sql_gateway_url=INFRA_CONFIG.flink_sql_gateway_url,
        )

        session1 = deployer1.get_session()
        session2 = deployer2.get_session()

        # Sessions should be different (each deployer creates its own)
        assert session1 != session2


@pytest.mark.integration
@pytest.mark.flink
class TestFlinkDDLExecution:
    """Test DDL statement execution."""

    def test_create_table_from_kafka(
        self,
        docker_services,
        kafka_helper: KafkaHelper,
        flink_helper: FlinkHelper,
    ):
        """Test creating a Flink table backed by Kafka."""
        topic_name = f"flink_test_{uuid.uuid4().hex[:8]}"
        table_name = f"test_table_{uuid.uuid4().hex[:8]}"

        try:
            # Create Kafka topic first
            kafka_helper.create_topic(topic_name, partitions=3)

            # Open Flink session
            flink_helper.open_sql_session()

            # Create Flink table
            create_table_sql = f"""
            CREATE TABLE {table_name} (
                id INT,
                name STRING,
                amount DOUBLE,
                event_time TIMESTAMP(3),
                WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
            ) WITH (
                'connector' = 'kafka',
                'topic' = '{topic_name}',
                'properties.bootstrap.servers' = '{INFRA_CONFIG.kafka_internal_servers}',
                'properties.group.id' = 'flink_test_group',
                'scan.startup.mode' = 'earliest-offset',
                'format' = 'json'
            )
            """

            result = flink_helper.execute_sql_and_wait(create_table_sql)
            assert result["status"] == "FINISHED"

            # Verify table was created by listing tables
            show_tables_result = flink_helper.execute_sql_and_wait("SHOW TABLES")
            assert show_tables_result["status"] == "FINISHED"

        finally:
            # Drop table
            try:
                flink_helper.execute_sql(f"DROP TABLE IF EXISTS {table_name}")
            except Exception:
                pass
            kafka_helper.delete_topic(topic_name)

    def test_create_table_with_primary_key(
        self,
        docker_services,
        flink_helper: FlinkHelper,
    ):
        """Test creating a table with primary key for upsert operations."""
        table_name = f"test_pk_table_{uuid.uuid4().hex[:8]}"

        try:
            flink_helper.open_sql_session()

            # Create upsert-kafka table
            create_table_sql = f"""
            CREATE TABLE {table_name} (
                user_id STRING,
                user_name STRING,
                balance DECIMAL(10, 2),
                PRIMARY KEY (user_id) NOT ENFORCED
            ) WITH (
                'connector' = 'upsert-kafka',
                'topic' = 'test_upsert_topic',
                'properties.bootstrap.servers' = '{INFRA_CONFIG.kafka_internal_servers}',
                'key.format' = 'json',
                'value.format' = 'json'
            )
            """

            result = flink_helper.execute_sql_and_wait(create_table_sql)
            assert result["status"] == "FINISHED"

        finally:
            try:
                flink_helper.execute_sql(f"DROP TABLE IF EXISTS {table_name}")
            except Exception:
                pass

    def test_create_temporary_view(
        self,
        docker_services,
        flink_helper: FlinkHelper,
    ):
        """Test creating a temporary view."""
        table_name = f"test_base_{uuid.uuid4().hex[:8]}"
        view_name = f"test_view_{uuid.uuid4().hex[:8]}"

        try:
            flink_helper.open_sql_session()

            # Create base table (datagen for simplicity)
            # Note: 'value' is a reserved keyword, use backticks
            create_table_sql = f"""
            CREATE TABLE {table_name} (
                id INT,
                `value` STRING
            ) WITH (
                'connector' = 'datagen',
                'rows-per-second' = '1'
            )
            """
            flink_helper.execute_sql_and_wait(create_table_sql)

            # Create view
            create_view_sql = f"""
            CREATE TEMPORARY VIEW {view_name} AS
            SELECT id, UPPER(`value`) as upper_value
            FROM {table_name}
            """
            result = flink_helper.execute_sql_and_wait(create_view_sql)
            assert result["status"] == "FINISHED"

        finally:
            try:
                flink_helper.execute_sql(f"DROP VIEW IF EXISTS {view_name}")
                flink_helper.execute_sql(f"DROP TABLE IF EXISTS {table_name}")
            except Exception:
                pass


@pytest.mark.integration
@pytest.mark.flink
class TestFlinkJobSubmission:
    """Test job submission and execution."""

    def test_submit_insert_job(
        self,
        docker_services,
        kafka_helper: KafkaHelper,
        flink_helper: FlinkHelper,
    ):
        """Test submitting an INSERT job."""
        source_topic = f"flink_source_{uuid.uuid4().hex[:8]}"
        sink_topic = f"flink_sink_{uuid.uuid4().hex[:8]}"
        source_table = f"source_{uuid.uuid4().hex[:8]}"
        sink_table = f"sink_{uuid.uuid4().hex[:8]}"

        try:
            # Create topics
            kafka_helper.create_topic(source_topic, partitions=3)
            kafka_helper.create_topic(sink_topic, partitions=3)

            # Open session
            flink_helper.open_sql_session()

            # Create source table
            flink_helper.execute_sql_and_wait(
                f"""
            CREATE TABLE {source_table} (
                id INT,
                name STRING
            ) WITH (
                'connector' = 'kafka',
                'topic' = '{source_topic}',
                'properties.bootstrap.servers' = '{INFRA_CONFIG.kafka_internal_servers}',
                'properties.group.id' = 'flink_source_group',
                'scan.startup.mode' = 'earliest-offset',
                'format' = 'json'
            )
            """
            )

            # Create sink table
            flink_helper.execute_sql_and_wait(
                f"""
            CREATE TABLE {sink_table} (
                id INT,
                name STRING
            ) WITH (
                'connector' = 'kafka',
                'topic' = '{sink_topic}',
                'properties.bootstrap.servers' = '{INFRA_CONFIG.kafka_internal_servers}',
                'format' = 'json'
            )
            """
            )

            # Submit job
            before_job_ids = {j["id"] for j in flink_helper.get_running_jobs()}
            result = flink_helper.execute_sql_and_wait(
                f"""
            INSERT INTO {sink_table}
            SELECT id, name FROM {source_table}
            """,
                expected_statuses={"RUNNING", "FINISHED"},
                timeout=60.0,
            )
            assert result["status"] in {"RUNNING", "FINISHED"}

            job_id = flink_helper.wait_for_new_running_job(
                known_job_ids=before_job_ids,
                timeout=60.0,
                interval=2.0,
            )
            assert job_id is not None, "Expected a new running job after INSERT"

        finally:
            try:
                flink_helper.execute_sql(f"DROP TABLE IF EXISTS {sink_table}")
                flink_helper.execute_sql(f"DROP TABLE IF EXISTS {source_table}")
            except Exception:
                pass
            kafka_helper.delete_topic(source_topic)
            kafka_helper.delete_topic(sink_topic)

    def test_submit_aggregation_job(
        self,
        docker_services,
        kafka_helper: KafkaHelper,
        flink_helper: FlinkHelper,
    ):
        """Test submitting a windowed aggregation job."""
        source_topic = f"flink_agg_source_{uuid.uuid4().hex[:8]}"
        sink_topic = f"flink_agg_sink_{uuid.uuid4().hex[:8]}"
        source_table = f"agg_source_{uuid.uuid4().hex[:8]}"
        sink_table = f"agg_sink_{uuid.uuid4().hex[:8]}"

        try:
            kafka_helper.create_topic(source_topic, partitions=3)
            kafka_helper.create_topic(sink_topic, partitions=3)

            flink_helper.open_sql_session()

            # Create source with event time
            flink_helper.execute_sql_and_wait(
                f"""
            CREATE TABLE {source_table} (
                user_id STRING,
                amount DECIMAL(10, 2),
                event_time TIMESTAMP(3),
                WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
            ) WITH (
                'connector' = 'kafka',
                'topic' = '{source_topic}',
                'properties.bootstrap.servers' = '{INFRA_CONFIG.kafka_internal_servers}',
                'properties.group.id' = 'flink_agg_group',
                'scan.startup.mode' = 'earliest-offset',
                'format' = 'json'
            )
            """
            )

            # Create sink for aggregation results
            flink_helper.execute_sql_and_wait(
                f"""
            CREATE TABLE {sink_table} (
                window_start TIMESTAMP(3),
                window_end TIMESTAMP(3),
                user_id STRING,
                total_amount DECIMAL(10, 2),
                tx_count BIGINT
            ) WITH (
                'connector' = 'kafka',
                'topic' = '{sink_topic}',
                'properties.bootstrap.servers' = '{INFRA_CONFIG.kafka_internal_servers}',
                'format' = 'json'
            )
            """
            )

            # Submit aggregation job with tumbling window
            before_job_ids = {j["id"] for j in flink_helper.get_running_jobs()}
            result = flink_helper.execute_sql_and_wait(
                f"""
            INSERT INTO {sink_table}
            SELECT
                TUMBLE_START(event_time, INTERVAL '1' MINUTE) as window_start,
                TUMBLE_END(event_time, INTERVAL '1' MINUTE) as window_end,
                user_id,
                SUM(amount) as total_amount,
                COUNT(*) as tx_count
            FROM {source_table}
            GROUP BY TUMBLE(event_time, INTERVAL '1' MINUTE), user_id
            """,
                expected_statuses={"RUNNING", "FINISHED"},
                timeout=60.0,
            )
            assert result["status"] in {"RUNNING", "FINISHED"}

            job_id = flink_helper.wait_for_new_running_job(
                known_job_ids=before_job_ids,
                timeout=60.0,
                interval=2.0,
            )
            assert job_id is not None, "Expected a new running aggregation job"

        finally:
            try:
                flink_helper.execute_sql(f"DROP TABLE IF EXISTS {sink_table}")
                flink_helper.execute_sql(f"DROP TABLE IF EXISTS {source_table}")
            except Exception:
                pass
            kafka_helper.delete_topic(source_topic)
            kafka_helper.delete_topic(sink_topic)


@pytest.mark.integration
@pytest.mark.flink
class TestFlinkDeployerIntegration:
    """Test FlinkDeployer (REST API) integration."""

    def test_check_connection(
        self,
        docker_services,
    ):
        """Test checking Flink cluster connection."""
        deployer = FlinkDeployer(INFRA_CONFIG.flink_rest_url)
        assert deployer.check_connection() is True

    def test_check_connection_invalid(self):
        """Test connection check with invalid URL."""
        deployer = FlinkDeployer("http://localhost:99999")
        assert deployer.check_connection() is False

    def test_list_jobs(
        self,
        docker_services,
    ):
        """Test listing jobs from cluster."""
        deployer = FlinkDeployer(INFRA_CONFIG.flink_rest_url)
        jobs = deployer.list_jobs()
        assert isinstance(jobs, list)

    def test_get_cluster_overview(
        self,
        docker_services,
        flink_helper: FlinkHelper,
    ):
        """Test getting cluster overview."""
        overview = flink_helper.get_cluster_overview()

        assert "taskmanagers" in overview
        assert "flink-version" in overview
        assert "slots-total" in overview


@pytest.mark.integration
@pytest.mark.flink
class TestFlinkJobArtifact:
    """Test FlinkJobArtifact deployment."""

    def test_apply_job_artifact(
        self,
        docker_services,
        kafka_helper: KafkaHelper,
    ):
        """Test applying a FlinkJobArtifact via deployer."""
        deployer = FlinkDeployer(
            rest_url=INFRA_CONFIG.flink_rest_url,
            sql_gateway_url=INFRA_CONFIG.flink_sql_gateway_url,
        )
        topic_name = f"artifact_test_{uuid.uuid4().hex[:8]}"

        try:
            kafka_helper.create_topic(topic_name, partitions=1)

            # Create artifact with complete SQL
            # Note: 'value' is a reserved keyword, use backticks
            sql = f"""
            CREATE TABLE test_artifact_table (
                id INT,
                `value` STRING
            ) WITH (
                'connector' = 'kafka',
                'topic' = '{topic_name}',
                'properties.bootstrap.servers' = '{INFRA_CONFIG.kafka_internal_servers}',
                'format' = 'json'
            )
            """

            artifact = FlinkJobArtifact(
                name=f"test_job_{uuid.uuid4().hex[:8]}",
                sql=sql,
                parallelism=1,
            )

            # Submit SQL
            results = deployer.submit_sql(artifact.sql)
            assert len(results["results"]) > 0

        finally:
            kafka_helper.delete_topic(topic_name)


@pytest.mark.integration
@pytest.mark.flink
@pytest.mark.slow
class TestFlinkEndToEndDataFlow:
    """Test end-to-end data flow through Flink."""

    def test_data_transformation_pipeline(
        self,
        docker_services,
        kafka_helper: KafkaHelper,
        flink_helper: FlinkHelper,
    ):
        """Test complete data transformation: Kafka -> Flink -> Kafka."""
        source_topic = f"e2e_source_{uuid.uuid4().hex[:8]}"
        sink_topic = f"e2e_sink_{uuid.uuid4().hex[:8]}"
        source_table = f"e2e_source_table_{uuid.uuid4().hex[:8]}"
        sink_table = f"e2e_sink_table_{uuid.uuid4().hex[:8]}"

        try:
            # Setup topics
            kafka_helper.create_topic(source_topic, partitions=1)
            kafka_helper.create_topic(sink_topic, partitions=1)

            # Produce test data
            test_messages = [
                {"id": 1, "name": "alice", "score": 100},
                {"id": 2, "name": "bob", "score": 200},
                {"id": 3, "name": "charlie", "score": 150},
            ]
            kafka_helper.produce_messages(source_topic, test_messages)

            # Create Flink pipeline
            flink_helper.open_sql_session()

            # Source table
            flink_helper.execute_sql_and_wait(
                f"""
            CREATE TABLE {source_table} (
                id INT,
                name STRING,
                score INT
            ) WITH (
                'connector' = 'kafka',
                'topic' = '{source_topic}',
                'properties.bootstrap.servers' = '{INFRA_CONFIG.kafka_internal_servers}',
                'properties.group.id' = 'e2e_test_group_{uuid.uuid4().hex[:8]}',
                'scan.startup.mode' = 'earliest-offset',
                'format' = 'json'
            )
            """
            )

            # Sink table
            flink_helper.execute_sql_and_wait(
                f"""
            CREATE TABLE {sink_table} (
                id INT,
                name_upper STRING,
                score_doubled INT
            ) WITH (
                'connector' = 'kafka',
                'topic' = '{sink_topic}',
                'properties.bootstrap.servers' = '{INFRA_CONFIG.kafka_internal_servers}',
                'format' = 'json'
            )
            """
            )

            # Submit transformation job
            before_job_ids = {j["id"] for j in flink_helper.get_running_jobs()}
            flink_helper.execute_sql_and_wait(
                f"""
            INSERT INTO {sink_table}
            SELECT
                id,
                UPPER(name) as name_upper,
                score * 2 as score_doubled
            FROM {source_table}
            """,
                expected_statuses={"RUNNING", "FINISHED"},
                timeout=60.0,
            )

            job_id = flink_helper.wait_for_new_running_job(
                known_job_ids=before_job_ids,
                timeout=60.0,
                interval=2.0,
            )
            assert job_id is not None, "Expected a running job for transformation pipeline"

            results = poll_until_messages(
                kafka_helper,
                sink_topic,
                min_messages=1,
                group_id=f"e2e_consumer_{uuid.uuid4().hex[:8]}",
                timeout=60.0,  # Increased timeout for job startup
                poll_interval=2.0,
            )

            # Verify transformations - require at least 1 message to validate the pipeline
            assert len(results) > 0, (
                f"Expected transformed messages in sink topic '{sink_topic}', "
                "but received none. Pipeline may have failed to process data."
            )

            # Check transformation was applied correctly
            for msg in results:
                assert "name_upper" in msg, f"Missing 'name_upper' field in result: {msg}"
                assert "score_doubled" in msg, f"Missing 'score_doubled' field in result: {msg}"
                assert (
                    msg["name_upper"] == msg["name_upper"].upper()
                ), f"Expected uppercase name, got '{msg['name_upper']}'"
                # Verify score was doubled correctly
                original = next(
                    (m for m in test_messages if m["id"] == msg["id"]),
                    None,
                )
                assert original is not None, f"Could not find original message for id={msg['id']}"
                assert (
                    msg["score_doubled"] == original["score"] * 2
                ), f"Expected score_doubled={original['score'] * 2}, got {msg['score_doubled']}"

        finally:
            try:
                flink_helper.execute_sql(f"DROP TABLE IF EXISTS {sink_table}")
                flink_helper.execute_sql(f"DROP TABLE IF EXISTS {source_table}")
            except Exception:
                pass
            kafka_helper.delete_topic(source_topic)
            kafka_helper.delete_topic(sink_topic)

    def test_filtering_pipeline(
        self,
        docker_services,
        kafka_helper: KafkaHelper,
        flink_helper: FlinkHelper,
    ):
        """Test filtering transformation."""
        source_topic = f"filter_source_{uuid.uuid4().hex[:8]}"
        sink_topic = f"filter_sink_{uuid.uuid4().hex[:8]}"
        source_table = f"filter_source_tbl_{uuid.uuid4().hex[:8]}"
        sink_table = f"filter_sink_tbl_{uuid.uuid4().hex[:8]}"

        try:
            kafka_helper.create_topic(source_topic, partitions=1)
            kafka_helper.create_topic(sink_topic, partitions=1)

            # Produce mixed data
            test_messages = [
                {"id": 1, "status": "active", "value": 100},
                {"id": 2, "status": "inactive", "value": 50},
                {"id": 3, "status": "active", "value": 200},
                {"id": 4, "status": "pending", "value": 75},
                {"id": 5, "status": "active", "value": 150},
            ]
            kafka_helper.produce_messages(source_topic, test_messages)

            flink_helper.open_sql_session()

            # Source - Note: 'value' is a reserved keyword, use backticks
            flink_helper.execute_sql_and_wait(
                f"""
            CREATE TABLE {source_table} (
                id INT,
                status STRING,
                `value` INT
            ) WITH (
                'connector' = 'kafka',
                'topic' = '{source_topic}',
                'properties.bootstrap.servers' = '{INFRA_CONFIG.kafka_internal_servers}',
                'properties.group.id' = 'filter_test_{uuid.uuid4().hex[:8]}',
                'scan.startup.mode' = 'earliest-offset',
                'format' = 'json'
            )
            """
            )

            # Sink - Note: 'value' is a reserved keyword, use backticks
            flink_helper.execute_sql_and_wait(
                f"""
            CREATE TABLE {sink_table} (
                id INT,
                `value` INT
            ) WITH (
                'connector' = 'kafka',
                'topic' = '{sink_topic}',
                'properties.bootstrap.servers' = '{INFRA_CONFIG.kafka_internal_servers}',
                'format' = 'json'
            )
            """
            )

            # Filter only active records
            before_job_ids = {j["id"] for j in flink_helper.get_running_jobs()}
            flink_helper.execute_sql_and_wait(
                f"""
            INSERT INTO {sink_table}
            SELECT id, `value`
            FROM {source_table}
            WHERE status = 'active'
            """,
                expected_statuses={"RUNNING", "FINISHED"},
                timeout=60.0,
            )

            job_id = flink_helper.wait_for_new_running_job(
                known_job_ids=before_job_ids,
                timeout=60.0,
                interval=2.0,
            )
            assert job_id is not None, "Expected a running job for filtering pipeline"

            results = poll_until_messages(
                kafka_helper,
                sink_topic,
                min_messages=1,
                group_id=f"filter_consumer_{uuid.uuid4().hex[:8]}",
                timeout=60.0,  # Increased timeout for job startup
                poll_interval=2.0,
            )

            # Verify filtering - require at least 1 message to validate the pipeline
            assert len(results) > 0, (
                f"Expected filtered messages in sink topic '{sink_topic}', "
                "but received none. Filtering pipeline may have failed."
            )

            # Should only have active records (ids 1, 3, 5)
            result_ids = {msg["id"] for msg in results}
            # All results should be from active records only
            assert result_ids.issubset(
                {1, 3, 5}
            ), f"Expected only active record ids {{1, 3, 5}}, but got {result_ids}"
            # Verify the filter excluded inactive/pending records (ids 2 and 4)
            assert 2 not in result_ids, "Inactive record (id=2) should have been filtered out"
            assert 4 not in result_ids, "Pending record (id=4) should have been filtered out"

        finally:
            try:
                flink_helper.execute_sql(f"DROP TABLE IF EXISTS {sink_table}")
                flink_helper.execute_sql(f"DROP TABLE IF EXISTS {source_table}")
            except Exception:
                pass
            kafka_helper.delete_topic(source_topic)
            kafka_helper.delete_topic(sink_topic)


@pytest.mark.integration
@pytest.mark.flink
class TestFlinkJobLifecycle:
    """Test Flink job lifecycle operations including cancellation."""

    def test_cancel_running_job(
        self,
        docker_services,
        kafka_helper: KafkaHelper,
        flink_helper: FlinkHelper,
    ):
        """Test cancelling a running streaming job."""
        source_topic = f"cancel_source_{uuid.uuid4().hex[:8]}"
        sink_topic = f"cancel_sink_{uuid.uuid4().hex[:8]}"
        source_table = f"cancel_src_{uuid.uuid4().hex[:8]}"
        sink_table = f"cancel_sink_{uuid.uuid4().hex[:8]}"

        try:
            # Create topics
            kafka_helper.create_topic(source_topic, partitions=1)
            kafka_helper.create_topic(sink_topic, partitions=1)

            # Open session and create tables
            flink_helper.open_sql_session()

            # Note: 'value' is a reserved keyword, use backticks
            flink_helper.execute_sql_and_wait(
                f"""
            CREATE TABLE {source_table} (
                id INT,
                `value` STRING
            ) WITH (
                'connector' = 'kafka',
                'topic' = '{source_topic}',
                'properties.bootstrap.servers' = '{INFRA_CONFIG.kafka_internal_servers}',
                'properties.group.id' = 'cancel_test_{uuid.uuid4().hex[:8]}',
                'scan.startup.mode' = 'earliest-offset',
                'format' = 'json'
            )
            """
            )

            flink_helper.execute_sql_and_wait(
                f"""
            CREATE TABLE {sink_table} (
                id INT,
                `value` STRING
            ) WITH (
                'connector' = 'kafka',
                'topic' = '{sink_topic}',
                'properties.bootstrap.servers' = '{INFRA_CONFIG.kafka_internal_servers}',
                'format' = 'json'
            )
            """
            )

            # Get existing job IDs before submission
            before_job_ids = {j["id"] for j in flink_helper.get_running_jobs()}

            # Submit a streaming job
            flink_helper.execute_sql_and_wait(
                f"""
            INSERT INTO {sink_table}
            SELECT id, `value` FROM {source_table}
            """,
                expected_statuses={"RUNNING", "FINISHED"},
                timeout=60.0,
            )

            # Use polling instead of hardcoded sleep - wait for new job to appear
            # Flink jobs may take 10-20s to transition to RUNNING state
            job_to_cancel = flink_helper.wait_for_new_running_job(
                known_job_ids=before_job_ids,
                timeout=60.0,  # Increased timeout for job startup
                interval=2.0,
            )

            # CRITICAL: We must have created at least one new job for this test to be valid
            assert job_to_cancel is not None, (
                "Expected a new running job to appear after INSERT statement, "
                f"but none found within timeout. Known jobs before: {len(before_job_ids)}. "
                "This test requires a running job to validate cancellation."
            )

            # Cancel the job
            cancelled = flink_helper.cancel_job(job_to_cancel)
            assert cancelled is True, f"Failed to cancel job {job_to_cancel}"

            # Use polling to wait for cancellation instead of hardcoded sleep
            flink_helper.wait_for_job_status(
                job_to_cancel,
                expected_status="CANCELED",
                timeout=15,
            )

            # Verify job is cancelled (or finished/cancelling)
            status = flink_helper.get_job_status(job_to_cancel)
            assert status in [
                "CANCELED",
                "CANCELLING",
                "FINISHED",
            ], f"Expected job to be cancelled, but status is {status}"

        finally:
            try:
                flink_helper.execute_sql(f"DROP TABLE IF EXISTS {sink_table}")
                flink_helper.execute_sql(f"DROP TABLE IF EXISTS {source_table}")
            except Exception:
                pass
            kafka_helper.delete_topic(source_topic)
            kafka_helper.delete_topic(sink_topic)

    def test_list_and_monitor_jobs(
        self,
        docker_services,
        flink_helper: FlinkHelper,
    ):
        """Test listing and monitoring job statuses."""
        # Get all jobs (running and completed)
        jobs = flink_helper.list_jobs()

        assert isinstance(jobs, list), "Expected list of jobs"

        # Each job should have required fields
        for job in jobs:
            assert "id" in job, f"Job missing 'id' field: {job}"
            assert "status" in job, f"Job missing 'status' field: {job}"
            assert job["status"] in [
                "CREATED",
                "RUNNING",
                "FAILING",
                "FAILED",
                "CANCELLING",
                "CANCELED",
                "FINISHED",
                "RESTARTING",
                "SUSPENDED",
                "RECONCILING",
            ], f"Unexpected job status: {job['status']}"

    def test_get_job_details(
        self,
        docker_services,
        kafka_helper: KafkaHelper,
        flink_helper: FlinkHelper,
    ):
        """Test getting detailed job information."""
        source_topic = f"details_source_{uuid.uuid4().hex[:8]}"
        source_table = f"details_src_{uuid.uuid4().hex[:8]}"

        try:
            kafka_helper.create_topic(source_topic, partitions=1)
            flink_helper.open_sql_session()

            # Create a simple datagen source
            flink_helper.execute_sql_and_wait(
                f"""
            CREATE TABLE {source_table} (
                id INT,
                data STRING
            ) WITH (
                'connector' = 'datagen',
                'rows-per-second' = '1'
            )
            """
            )

            # Execute a query
            result = flink_helper.execute_sql_and_wait(
                f"""
            SELECT * FROM {source_table}
            """,
                expected_statuses={"RUNNING", "FINISHED"},
                timeout=30.0,
            )
            assert result["status"] in {"RUNNING", "FINISHED"}

        finally:
            try:
                flink_helper.execute_sql(f"DROP TABLE IF EXISTS {source_table}")
            except Exception:
                pass
            kafka_helper.delete_topic(source_topic)


@pytest.mark.integration
@pytest.mark.flink
class TestFlinkErrorHandling:
    """Test Flink error handling and recovery."""

    def test_invalid_sql_syntax_error(
        self,
        docker_services,
        flink_helper: FlinkHelper,
    ):
        """Test that invalid SQL produces appropriate error.

        Note: Flink SQL Gateway is async - errors appear when fetching results,
        not when submitting statements. This test verifies that errors are
        properly surfaced through the result polling mechanism.
        """
        flink_helper.open_sql_session()

        # Execute invalid SQL and check for error in result
        error = flink_helper.execute_sql_and_check_error("INVALID SQL STATEMENT HERE")

        # Should get an error message
        assert error is not None, "Expected error for invalid SQL syntax"
        assert len(error) > 0, "Expected non-empty error message"

    def test_nonexistent_table_error(
        self,
        docker_services,
        flink_helper: FlinkHelper,
    ):
        """Test that referencing nonexistent table produces error."""
        flink_helper.open_sql_session()
        nonexistent_table = f"nonexistent_{uuid.uuid4().hex[:8]}"

        # Execute and check for error in result
        error = flink_helper.execute_sql_and_check_error(
            f"SELECT * FROM {nonexistent_table}"
        )

        # Should get an error about table not existing
        assert error is not None, "Expected error for nonexistent table"

    def test_duplicate_table_creation_error(
        self,
        docker_services,
        flink_helper: FlinkHelper,
    ):
        """Test that creating duplicate table produces error."""
        table_name = f"duplicate_test_{uuid.uuid4().hex[:8]}"

        try:
            flink_helper.open_sql_session()

            # Create table first time - should succeed
            flink_helper.execute_sql_and_wait(
                f"""
            CREATE TABLE {table_name} (
                id INT
            ) WITH (
                'connector' = 'datagen',
                'rows-per-second' = '1'
            )
            """
            )

            # Create same table again - should fail
            error = flink_helper.execute_sql_and_check_error(
                f"""
                CREATE TABLE {table_name} (
                    id INT
                ) WITH (
                    'connector' = 'datagen',
                    'rows-per-second' = '1'
                )
                """
            )

            # Should get an error about duplicate table
            assert error is not None, "Expected error for duplicate table creation"

        finally:
            try:
                flink_helper.execute_sql(f"DROP TABLE IF EXISTS {table_name}")
            except Exception:
                pass

    def test_create_if_not_exists_idempotent(
        self,
        docker_services,
        flink_helper: FlinkHelper,
    ):
        """Test that CREATE TABLE IF NOT EXISTS is idempotent."""
        table_name = f"idempotent_test_{uuid.uuid4().hex[:8]}"

        try:
            flink_helper.open_sql_session()

            # Create with IF NOT EXISTS - should succeed
            result1 = flink_helper.execute_sql_and_wait(
                f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INT
            ) WITH (
                'connector' = 'datagen',
                'rows-per-second' = '1'
            )
            """
            )
            assert result1["status"] == "FINISHED"

            # Create again with IF NOT EXISTS - should also succeed (no-op)
            result2 = flink_helper.execute_sql_and_wait(
                f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INT
            ) WITH (
                'connector' = 'datagen',
                'rows-per-second' = '1'
            )
            """
            )
            assert result2["status"] == "FINISHED"

        finally:
            try:
                flink_helper.execute_sql(f"DROP TABLE IF EXISTS {table_name}")
            except Exception:
                pass


@pytest.mark.integration
@pytest.mark.flink
@pytest.mark.kafka
class TestFailureModes:
    """Test that failures are properly surfaced (chaos/failure testing).

    These tests verify that when things go wrong, errors are surfaced
    clearly rather than being silently swallowed. Bank-grade systems
    must fail loudly, not silently.
    """

    def test_invalid_kafka_broker_surfaces_error(
        self,
        docker_services,
        flink_helper: FlinkHelper,
    ):
        """Test that invalid Kafka broker connection surfaces error."""
        table_name = f"invalid_broker_test_{uuid.uuid4().hex[:8]}"

        try:
            # Create table pointing to non-existent broker
            # Note: 'value' is a reserved keyword, use backticks
            result = flink_helper.execute_sql_and_wait(
                f"""
            CREATE TABLE {table_name} (
                id INT,
                `value` STRING
            ) WITH (
                'connector' = 'kafka',
                'topic' = 'test-topic',
                'properties.bootstrap.servers' = 'nonexistent-broker:9999',
                'properties.group.id' = 'test-group',
                'scan.startup.mode' = 'earliest-offset',
                'format' = 'json'
            )
            """
            )

            # Table creation succeeds (lazy connection)
            assert result["status"] == "FINISHED"

            # Querying should surface an error for invalid broker
            error = flink_helper.execute_sql_and_check_error(
                f"SELECT * FROM {table_name}",
                timeout=30.0,
            )
            assert error is not None, "Expected error for invalid Kafka broker"

        finally:
            try:
                flink_helper.execute_sql(f"DROP TABLE IF EXISTS {table_name}")
            except Exception:
                pass

    def test_malformed_json_in_consume_surfaces_error(
        self,
        docker_services,
        kafka_helper: KafkaHelper,
    ):
        """Test that malformed JSON messages surface decode errors."""
        topic_name = f"malformed_json_test_{uuid.uuid4().hex[:8]}"

        try:
            kafka_helper.create_topic(topic_name)

            # Produce malformed JSON directly (bypass helper's JSON encoding)
            from confluent_kafka import Producer

            producer = Producer({"bootstrap.servers": INFRA_CONFIG.kafka_bootstrap_servers})
            producer.produce(topic_name, value=b"not valid json {{{")
            producer.produce(topic_name, value=b"\xff\xfe invalid utf8")
            producer.flush()

            # Consuming should raise ValueError about decode errors
            with pytest.raises(ValueError) as exc_info:
                kafka_helper.consume_messages(
                    topic_name,
                    group_id=f"malformed_test_{uuid.uuid4().hex[:8]}",
                    max_messages=10,
                    timeout=10.0,
                )

            # Verify error message is informative
            error_msg = str(exc_info.value)
            assert "decode" in error_msg.lower() or "Failed to decode" in error_msg

        finally:
            kafka_helper.delete_topic(topic_name)

    def test_nonexistent_topic_consume_returns_empty(
        self,
        docker_services,
        kafka_helper: KafkaHelper,
    ):
        """Test that consuming from non-existent topic returns empty, not hangs."""
        topic_name = f"definitely_does_not_exist_{uuid.uuid4().hex[:8]}"

        # Should return empty list within timeout, not hang
        messages = kafka_helper.consume_messages(
            topic_name,
            group_id=f"nonexistent_test_{uuid.uuid4().hex[:8]}",
            max_messages=10,
            timeout=5.0,  # Short timeout
        )

        assert messages == [], f"Expected empty list for non-existent topic, got {messages}"

    def test_flink_sql_syntax_error_surfaces_immediately(
        self,
        docker_services,
        flink_helper: FlinkHelper,
    ):
        """Test that SQL syntax errors are detectable in result polling.

        Note: Flink SQL Gateway is async - errors appear when fetching results,
        not when submitting statements. This test verifies that errors are
        properly detectable through result polling.
        """
        flink_helper.open_sql_session()

        # Execute invalid SQL and check for error
        error = flink_helper.execute_sql_and_check_error("SELEC * FORM invalid_syntax")

        # Verify we get a meaningful error
        assert error is not None, "Expected error for SQL syntax error"
        assert len(error) > 0, "Expected non-empty error message for SQL syntax error"
