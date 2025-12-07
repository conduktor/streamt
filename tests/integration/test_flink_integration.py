"""Integration tests for Flink deployer.

These tests require Docker infrastructure (started via docker_services fixture).
Run with: pytest tests/integration/ -v
"""

import time
import uuid

import pytest

from streamt.deployer.flink import FlinkDeployer

from .conftest import INFRA_CONFIG, KafkaHelper


@pytest.mark.integration
@pytest.mark.flink
class TestFlinkIntegration:
    """Integration tests with real Flink cluster."""

    def test_check_connection_with_real_flink(self, docker_services):
        """Should connect to real Flink cluster."""
        deployer = FlinkDeployer(rest_url=INFRA_CONFIG.flink_rest_url)
        assert deployer.check_connection() is True

    def test_list_jobs_with_real_flink(self, docker_services):
        """Should list jobs from real Flink cluster."""
        deployer = FlinkDeployer(rest_url=INFRA_CONFIG.flink_rest_url)
        jobs = deployer.list_jobs()

        # Should return a list (possibly empty)
        assert isinstance(jobs, list)

    def test_list_jobs_with_details(self, docker_services):
        """Should fetch job details including names."""
        deployer = FlinkDeployer(rest_url=INFRA_CONFIG.flink_rest_url)
        jobs = deployer.list_jobs(include_details=True)

        # If there are jobs, they should have names
        for job in jobs:
            if job.get("jid"):
                assert "name" in job or "state" in job

    def test_get_session_with_real_gateway(self, docker_services):
        """Should create session with real SQL Gateway."""
        deployer = FlinkDeployer(
            rest_url=INFRA_CONFIG.flink_rest_url,
            sql_gateway_url=INFRA_CONFIG.flink_sql_gateway_url,
        )
        session_id = deployer.get_session()

        assert session_id is not None
        assert len(session_id) > 0

    def test_submit_simple_sql(self, docker_services):
        """Should submit and execute simple SQL."""
        deployer = FlinkDeployer(
            rest_url=INFRA_CONFIG.flink_rest_url,
            sql_gateway_url=INFRA_CONFIG.flink_sql_gateway_url,
        )

        # Simple SQL that doesn't create any resources
        result = deployer.submit_sql("SELECT 1")

        assert "results" in result
        assert len(result["results"]) == 1
        assert result["results"][0]["status"] == "FINISHED"

    def test_submit_invalid_sql_returns_error(self, docker_services):
        """Should raise error for invalid SQL."""
        deployer = FlinkDeployer(
            rest_url=INFRA_CONFIG.flink_rest_url,
            sql_gateway_url=INFRA_CONFIG.flink_sql_gateway_url,
        )

        with pytest.raises(RuntimeError) as exc_info:
            deployer.submit_sql("SELECT * FROM nonexistent_table_xyz")

        assert "error" in str(exc_info.value).lower()


@pytest.mark.integration
@pytest.mark.flink
class TestFlinkJobMatching:
    """Integration tests for job name matching with real jobs."""

    def test_get_job_state_nonexistent(self, docker_services):
        """Should return exists=False for nonexistent job."""
        deployer = FlinkDeployer(rest_url=INFRA_CONFIG.flink_rest_url)
        state = deployer.get_job_state("definitely_nonexistent_job_12345")

        assert state.exists is False
        assert state.name == "definitely_nonexistent_job_12345"

    def test_get_job_state_matches_running_job(
        self,
        docker_services,
        kafka_helper: KafkaHelper,
    ):
        """Create an INSERT job and verify get_job_state finds it by pattern.

        This test is self-contained: it creates its own Flink INSERT job,
        then verifies the job name matching logic works correctly.
        """
        suffix = uuid.uuid4().hex[:8]
        source_topic = f"matching_src_{suffix}"
        sink_topic = f"matching_sink_{suffix}"

        # Use a predictable model name so we know what pattern to look for
        model_name = f"test_model_{suffix}"

        deployer = FlinkDeployer(
            rest_url=INFRA_CONFIG.flink_rest_url,
            sql_gateway_url=INFRA_CONFIG.flink_sql_gateway_url,
        )

        try:
            # Create topics
            kafka_helper.create_topic(source_topic, partitions=1)
            kafka_helper.create_topic(sink_topic, partitions=1)

            # Create source table
            deployer.submit_sql(f"""
                CREATE TABLE {model_name}_source (
                    id STRING,
                    val INT
                ) WITH (
                    'connector' = 'kafka',
                    'topic' = '{source_topic}',
                    'properties.bootstrap.servers' = '{INFRA_CONFIG.kafka_internal_servers}',
                    'scan.startup.mode' = 'earliest-offset',
                    'format' = 'json'
                )
            """)

            # Create sink table with the expected naming pattern
            # Flink will name the job: insert-into_default_catalog.default_database.{model_name}_sink
            deployer.submit_sql(f"""
                CREATE TABLE {model_name}_sink (
                    id STRING,
                    val INT
                ) WITH (
                    'connector' = 'kafka',
                    'topic' = '{sink_topic}',
                    'properties.bootstrap.servers' = '{INFRA_CONFIG.kafka_internal_servers}',
                    'format' = 'json'
                )
            """)

            # Submit INSERT job - this creates a running job with predictable name
            deployer.submit_sql(f"""
                INSERT INTO {model_name}_sink
                SELECT id, val FROM {model_name}_source
            """)

            # Wait for job to be running
            time.sleep(5)

            # Verify we can find the job using get_job_state with processor pattern
            # get_job_state("test_model_xxx_processor") should find "insert-into_..._test_model_xxx_sink"
            state = deployer.get_job_state(f"{model_name}_processor")

            assert state.exists is True, (
                f"Expected to find job for '{model_name}_processor' but got exists=False. "
                f"Jobs: {[j.get('name') for j in deployer.list_jobs(include_details=True)]}"
            )
            assert state.status == "RUNNING", f"Expected RUNNING status, got {state.status}"
            assert state.job_id is not None, "Expected job_id to be set"

            # Also verify we can find it by exact sink table match
            jobs = deployer.list_jobs(include_details=True)
            matching_jobs = [j for j in jobs if f"{model_name}_sink" in j.get("name", "")]
            assert len(matching_jobs) >= 1, (
                f"Expected at least 1 job with '{model_name}_sink' in name. "
                f"Found jobs: {[j.get('name') for j in jobs]}"
            )

        finally:
            # Cleanup: cancel any jobs we created
            try:
                state = deployer.get_job_state(f"{model_name}_processor")
                if state.exists and state.job_id:
                    deployer.cancel_job(state.job_id)
            except Exception:
                pass

            kafka_helper.delete_topic(source_topic)
            kafka_helper.delete_topic(sink_topic)
