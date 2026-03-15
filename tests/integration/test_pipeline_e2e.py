"""End-to-end pipeline tests using streamt YAML → Compile → Deploy workflow.

These tests verify complete streaming pipelines through streamt's abstractions:
- YAML model definition → Compiler → Artifacts → Deployer
- Multi-stage transformations using ref()
- Stream-to-stream joins
- Windowed aggregations
- Full manifest deployments

NOTE: Tests use streamt's YAML config and compiler, NOT raw Flink SQL.
This ensures we test streamt's actual functionality, not just Flink.
"""

import tempfile
import time
import uuid
from pathlib import Path

import pytest

from streamt.compiler.compiler import Compiler
from streamt.compiler.manifest import (
    FlinkJobArtifact,
    Manifest,
    TopicArtifact,
)
from streamt.core.parser import ProjectParser
from streamt.deployer.flink import FlinkDeployer
from streamt.deployer.kafka import KafkaDeployer

from .conftest import INFRA_CONFIG, ConnectHelper, FlinkHelper, KafkaHelper, poll_until_messages


def generate_unique_suffix() -> str:
    """Generate unique suffix for test isolation."""
    return uuid.uuid4().hex[:8]


def create_and_compile_project(yaml_content: str) -> tuple:
    """Create a project from YAML and compile it.

    Returns (manifest, tmpdir) - tmpdir must be kept alive until deployment completes.
    """
    tmpdir = tempfile.mkdtemp()
    project_file = Path(tmpdir) / "stream_project.yml"
    project_file.write_text(yaml_content)

    parser = ProjectParser(Path(tmpdir))
    project = parser.parse()
    compiler = Compiler(project)
    manifest = compiler.compile()

    return manifest, tmpdir


def deploy_manifest_topics(manifest: Manifest) -> None:
    """Deploy all topics from a manifest."""
    kafka_deployer = KafkaDeployer(INFRA_CONFIG.kafka_bootstrap_servers)
    for topic_dict in manifest.artifacts.get("topics", []):
        artifact = TopicArtifact(
            name=topic_dict["name"],
            partitions=topic_dict["partitions"],
            replication_factor=topic_dict["replication_factor"],
            config=topic_dict.get("config", {}),
        )
        kafka_deployer.apply_topic(artifact)


def deploy_manifest_flink_jobs(manifest: Manifest) -> list[str]:
    """Deploy all Flink jobs from a manifest. Returns job names."""
    flink_deployer = FlinkDeployer(
        rest_url=INFRA_CONFIG.flink_rest_url,
        sql_gateway_url=INFRA_CONFIG.flink_sql_gateway_url,
    )
    deployed_jobs = []
    for job_dict in manifest.artifacts.get("flink_jobs", []):
        artifact = FlinkJobArtifact(
            name=job_dict["name"],
            sql=job_dict["sql"],
            cluster=job_dict.get("cluster", "local"),
        )
        flink_deployer.apply_job(artifact)
        deployed_jobs.append(job_dict["name"])
    return deployed_jobs


@pytest.mark.integration
@pytest.mark.kafka
@pytest.mark.flink
@pytest.mark.slow
class TestMultiStageKafkaFlinkPipeline:
    """Test multi-stage Kafka-Flink pipelines using streamt YAML workflow."""

    def test_two_stage_transformation(
        self,
        docker_services,
        kafka_helper: KafkaHelper,
        flink_helper: FlinkHelper,
    ):
        """Test a two-stage transformation pipeline via streamt YAML.

        Stage 1: Filter high-value orders (amount >= 100)
        Stage 2: Enrich with premium flag (amount >= 200)

        Uses streamt's ref() function to chain models.
        """
        suffix = generate_unique_suffix()
        source_topic = f"orders_{suffix}"
        stage1_topic = f"high_value_orders_{suffix}"
        stage2_topic = f"enriched_orders_{suffix}"

        project_yaml = f"""
project:
  name: two-stage-pipeline-{suffix}
  version: "1.0.0"

runtime:
  kafka:
    bootstrap_servers: {INFRA_CONFIG.kafka_bootstrap_servers}
    bootstrap_servers_internal: {INFRA_CONFIG.kafka_internal_servers}
  flink:
    default: local
    clusters:
      local:
        rest_url: {INFRA_CONFIG.flink_rest_url}
        sql_gateway_url: {INFRA_CONFIG.flink_sql_gateway_url}

sources:
  - name: orders
    topic: {source_topic}
    columns:
      - name: order_id
        type: INT
      - name: category
        type: STRING
      - name: amount
        type: DOUBLE

models:
  - name: high_value_orders
    description: Filter orders with amount >= 100
    sql: |
      SELECT order_id, category, amount
      FROM {{{{ source("orders") }}}}
      WHERE amount >= 100
    topic:
      name: {stage1_topic}
      partitions: 3

  - name: enriched_orders
    description: Add premium flag for amount >= 200
    sql: |
      SELECT
        order_id,
        category,
        amount,
        CASE WHEN amount >= 200 THEN TRUE ELSE FALSE END as is_premium
      FROM {{{{ ref("high_value_orders") }}}}
    topic:
      name: {stage2_topic}
      partitions: 3
"""

        try:
            # Create source topic
            kafka_helper.create_topic(source_topic, partitions=3)

            # Compile project using streamt
            manifest, _tmpdir = create_and_compile_project(project_yaml)

            # Verify compilation produced expected artifacts
            assert "topics" in manifest.artifacts, "Manifest should have topics"
            assert "flink_jobs" in manifest.artifacts, "Manifest should have flink_jobs"
            assert len(manifest.artifacts["flink_jobs"]) == 2, "Should have 2 Flink jobs"

            # Verify DAG ordering: high_value_orders should come before enriched_orders
            job_names = [j["name"] for j in manifest.artifacts["flink_jobs"]]
            assert "high_value_orders" in job_names
            assert "enriched_orders" in job_names

            # Deploy topics from manifest
            deploy_manifest_topics(manifest)

            # Deploy Flink jobs from manifest
            deploy_manifest_flink_jobs(manifest)

            # Wait for jobs to initialize
            time.sleep(5)

            # Produce test orders
            orders = [
                {"order_id": 1, "category": "electronics", "amount": 150.0},
                {"order_id": 2, "category": "books", "amount": 25.0},  # Filtered out
                {"order_id": 3, "category": "electronics", "amount": 500.0},
                {"order_id": 4, "category": "clothing", "amount": 80.0},  # Filtered out
                {"order_id": 5, "category": "electronics", "amount": 200.0},
                {"order_id": 6, "category": "books", "amount": 15.0},  # Filtered out
            ]
            kafka_helper.produce_messages(source_topic, orders)

            # Verify stage 1 output: high-value filtering
            stage1_results = poll_until_messages(
                kafka_helper,
                stage1_topic,
                min_messages=1,
                group_id=f"verify_stage1_{suffix}",
                timeout=60.0,
                poll_interval=2.0,
            )

            assert len(stage1_results) > 0, "Stage 1 should produce filtered results"
            stage1_ids = {r["order_id"] for r in stage1_results}
            assert stage1_ids.issubset({1, 3, 5}), f"Stage 1 should only have high-value orders, got {stage1_ids}"
            for r in stage1_results:
                assert r["amount"] >= 100, f"Stage 1 filter failed: {r}"

            # Verify stage 2 output: premium enrichment
            stage2_results = poll_until_messages(
                kafka_helper,
                stage2_topic,
                min_messages=1,
                group_id=f"verify_stage2_{suffix}",
                timeout=30.0,
                poll_interval=2.0,
            )

            assert len(stage2_results) > 0, "Stage 2 should produce enriched results"
            for r in stage2_results:
                assert "is_premium" in r, f"Missing 'is_premium' field: {r}"
                if r["amount"] >= 200:
                    assert r["is_premium"] is True, f"Should be premium: {r}"
                else:
                    assert r["is_premium"] is False, f"Should not be premium: {r}"

        finally:
            flink_helper.cancel_all_running_jobs()
            kafka_helper.delete_topic(source_topic)
            kafka_helper.delete_topic(stage1_topic)
            kafka_helper.delete_topic(stage2_topic)


@pytest.mark.integration
@pytest.mark.kafka
@pytest.mark.flink
@pytest.mark.slow
class TestStreamingJoinPipeline:
    """Test streaming join pipelines using streamt YAML workflow."""

    def test_stream_to_stream_join(
        self,
        docker_services,
        kafka_helper: KafkaHelper,
        flink_helper: FlinkHelper,
    ):
        """Test joining two Kafka streams via streamt YAML model."""
        suffix = generate_unique_suffix()
        orders_topic = f"join_orders_{suffix}"
        customers_topic = f"join_customers_{suffix}"
        output_topic = f"join_output_{suffix}"

        project_yaml = f"""
project:
  name: stream-join-{suffix}
  version: "1.0.0"

runtime:
  kafka:
    bootstrap_servers: {INFRA_CONFIG.kafka_bootstrap_servers}
    bootstrap_servers_internal: {INFRA_CONFIG.kafka_internal_servers}
  flink:
    default: local
    clusters:
      local:
        rest_url: {INFRA_CONFIG.flink_rest_url}
        sql_gateway_url: {INFRA_CONFIG.flink_sql_gateway_url}

sources:
  - name: orders
    topic: {orders_topic}
    columns:
      - name: order_id
        type: STRING
      - name: customer_id
        type: STRING
      - name: amount
        type: DOUBLE

  - name: customers
    topic: {customers_topic}
    columns:
      - name: customer_id
        type: STRING
      - name: name
        type: STRING
      - name: tier
        type: STRING

models:
  - name: enriched_orders
    description: Orders enriched with customer info
    sql: |
      SELECT
        o.order_id,
        c.name as customer_name,
        c.tier as customer_tier,
        o.amount
      FROM {{{{ source("orders") }}}} o
      JOIN {{{{ source("customers") }}}} c
      ON o.customer_id = c.customer_id
    topic:
      name: {output_topic}
      partitions: 1
"""

        try:
            # Create topics
            kafka_helper.create_topic(orders_topic, partitions=1)
            kafka_helper.create_topic(customers_topic, partitions=1)

            # Compile and deploy
            manifest, _tmpdir = create_and_compile_project(project_yaml)

            assert len(manifest.artifacts.get("flink_jobs", [])) == 1, "Should have 1 join job"

            deploy_manifest_topics(manifest)
            deploy_manifest_flink_jobs(manifest)

            time.sleep(5)

            # Produce customers first (dimension data)
            customers = [
                {"customer_id": "c1", "name": "Alice", "tier": "gold"},
                {"customer_id": "c2", "name": "Bob", "tier": "silver"},
                {"customer_id": "c3", "name": "Charlie", "tier": "bronze"},
            ]
            kafka_helper.produce_messages(customers_topic, customers, key_field="customer_id")

            # Produce orders
            orders = [
                {"order_id": "o1", "customer_id": "c1", "amount": 100.0},
                {"order_id": "o2", "customer_id": "c2", "amount": 50.0},
                {"order_id": "o3", "customer_id": "c1", "amount": 200.0},
            ]
            kafka_helper.produce_messages(orders_topic, orders, key_field="order_id")

            # Wait for joined results
            results = poll_until_messages(
                kafka_helper,
                output_topic,
                min_messages=1,
                group_id=f"verify_join_{suffix}",
                timeout=60.0,
                poll_interval=2.0,
            )

            assert len(results) > 0, "Should have joined results"
            for r in results:
                assert "order_id" in r, f"Missing order_id: {r}"
                assert "customer_name" in r, f"Missing customer_name: {r}"
                assert "customer_tier" in r, f"Missing customer_tier: {r}"
                assert "amount" in r, f"Missing amount: {r}"
                assert r["customer_tier"] in {"gold", "silver", "bronze"}, f"Invalid tier: {r}"

        finally:
            flink_helper.cancel_all_running_jobs()
            kafka_helper.delete_topic(orders_topic)
            kafka_helper.delete_topic(customers_topic)
            kafka_helper.delete_topic(output_topic)


@pytest.mark.integration
@pytest.mark.kafka
@pytest.mark.flink
@pytest.mark.slow
class TestWindowedAggregationPipeline:
    """Test windowed aggregation pipelines using streamt YAML workflow."""

    def test_tumbling_window_aggregation(
        self,
        docker_services,
        kafka_helper: KafkaHelper,
        flink_helper: FlinkHelper,
    ):
        """Test tumbling window aggregation via streamt YAML model."""
        suffix = generate_unique_suffix()
        source_topic = f"window_source_{suffix}"
        sink_topic = f"window_sink_{suffix}"

        project_yaml = f"""
project:
  name: window-aggregation-{suffix}
  version: "1.0.0"

runtime:
  kafka:
    bootstrap_servers: {INFRA_CONFIG.kafka_bootstrap_servers}
    bootstrap_servers_internal: {INFRA_CONFIG.kafka_internal_servers}
  flink:
    default: local
    clusters:
      local:
        rest_url: {INFRA_CONFIG.flink_rest_url}
        sql_gateway_url: {INFRA_CONFIG.flink_sql_gateway_url}

sources:
  - name: page_views
    topic: {source_topic}
    columns:
      - name: user_id
        type: STRING
      - name: page
        type: STRING
      - name: event_time
        type: TIMESTAMP(3)
    event_time:
      column: event_time
      watermark:
        strategy: bounded_out_of_orderness
        max_out_of_orderness_ms: 5000

models:
  - name: page_view_counts
    description: Count page views per user in 10-second tumbling windows
    sql: |
      SELECT
        TUMBLE_START(event_time, INTERVAL '10' SECOND) as window_start,
        TUMBLE_END(event_time, INTERVAL '10' SECOND) as window_end,
        user_id,
        COUNT(*) as page_views
      FROM {{{{ source("page_views") }}}}
      GROUP BY
        TUMBLE(event_time, INTERVAL '10' SECOND),
        user_id
    topic:
      name: {sink_topic}
      partitions: 1
"""

        try:
            kafka_helper.create_topic(source_topic, partitions=1)

            # Compile and deploy
            manifest, _tmpdir = create_and_compile_project(project_yaml)

            # Verify event_time config was compiled
            job_sql = manifest.artifacts["flink_jobs"][0]["sql"]
            assert "WATERMARK" in job_sql, "Should have WATERMARK in compiled SQL"

            deploy_manifest_topics(manifest)
            deploy_manifest_flink_jobs(manifest)

            time.sleep(5)

            # Produce events in a completed window (30 seconds ago)
            window_time = int(time.time() * 1000) - 30000
            events = [
                {"user_id": "u1", "page": "/home", "event_time": window_time},
                {"user_id": "u1", "page": "/products", "event_time": window_time + 1000},
                {"user_id": "u2", "page": "/home", "event_time": window_time + 2000},
                {"user_id": "u1", "page": "/cart", "event_time": window_time + 3000},
            ]

            # Convert to Flink timestamp format
            for event in events:
                ts = event["event_time"]
                event["event_time"] = time.strftime("%Y-%m-%d %H:%M:%S.000", time.gmtime(ts / 1000))

            kafka_helper.produce_messages(source_topic, events)

            # Advance watermark with future event
            future_time = int(time.time() * 1000) + 60000
            watermark_event = {
                "user_id": "watermark_trigger",
                "page": "/trigger",
                "event_time": time.strftime("%Y-%m-%d %H:%M:%S.000", time.gmtime(future_time / 1000)),
            }
            kafka_helper.produce_messages(source_topic, [watermark_event])

            time.sleep(10)  # Wait for window to close

            # Verify aggregation results
            results = poll_until_messages(
                kafka_helper,
                sink_topic,
                min_messages=1,
                group_id=f"verify_window_{suffix}",
                timeout=60.0,
                poll_interval=2.0,
            )

            assert len(results) > 0, "Should have window aggregation results"

            # Filter out watermark trigger
            actual_results = [r for r in results if r.get("user_id") != "watermark_trigger"]
            assert len(actual_results) > 0, "Should have results from test users"

            for r in actual_results:
                assert "window_start" in r, f"Missing window_start: {r}"
                assert "window_end" in r, f"Missing window_end: {r}"
                assert "user_id" in r, f"Missing user_id: {r}"
                assert "page_views" in r, f"Missing page_views: {r}"
                assert isinstance(r["page_views"], int), f"Invalid page_views type: {r}"
                assert r["page_views"] > 0, f"Invalid page_views value: {r}"

        finally:
            flink_helper.cancel_all_running_jobs()
            kafka_helper.delete_topic(source_topic)
            kafka_helper.delete_topic(sink_topic)


@pytest.mark.integration
@pytest.mark.kafka
@pytest.mark.connect
@pytest.mark.flink
@pytest.mark.slow
class TestFullStackPipeline:
    """Test complete pipelines using all components via streamt."""

    def test_connect_kafka_flink_pipeline(
        self,
        docker_services,
        kafka_helper: KafkaHelper,
        connect_helper: ConnectHelper,
        flink_helper: FlinkHelper,
    ):
        """Test: Datagen -> Kafka -> Flink Transform -> Kafka via streamt.

        Uses Connect for data generation, then processes through streamt model.
        """
        suffix = generate_unique_suffix()
        source_topic = f"fullstack_source_{suffix}"
        sink_topic = f"fullstack_sink_{suffix}"
        connector_name = f"fullstack_datagen_{suffix}"

        project_yaml = f"""
project:
  name: fullstack-pipeline-{suffix}
  version: "1.0.0"

runtime:
  kafka:
    bootstrap_servers: {INFRA_CONFIG.kafka_bootstrap_servers}
    bootstrap_servers_internal: {INFRA_CONFIG.kafka_internal_servers}
  flink:
    default: local
    clusters:
      local:
        rest_url: {INFRA_CONFIG.flink_rest_url}
        sql_gateway_url: {INFRA_CONFIG.flink_sql_gateway_url}

sources:
  - name: users
    topic: {source_topic}
    columns:
      - name: userid
        type: STRING
      - name: regionid
        type: STRING
      - name: gender
        type: STRING

models:
  - name: users_transformed
    description: Transform user data - uppercase region
    sql: |
      SELECT userid, UPPER(regionid) as region_upper
      FROM {{{{ source("users") }}}}
    topic:
      name: {sink_topic}
      partitions: 1
"""

        try:
            # Create source topic and start datagen connector
            kafka_helper.create_topic(source_topic, partitions=1)

            config = {
                "connector.class": "io.confluent.kafka.connect.datagen.DatagenConnector",
                "kafka.topic": source_topic,
                "quickstart": "users",
                "key.converter": "org.apache.kafka.connect.storage.StringConverter",
                "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                "value.converter.schemas.enable": "false",
                "max.interval": "500",
                "iterations": "100",
                "tasks.max": "1",
            }
            connect_helper.create_connector(connector_name, config)
            connect_helper.wait_for_connector_running(connector_name, timeout=30)

            time.sleep(5)  # Let datagen produce some data

            # Compile and deploy streamt model
            manifest, _tmpdir = create_and_compile_project(project_yaml)
            deploy_manifest_topics(manifest)
            deploy_manifest_flink_jobs(manifest)

            time.sleep(15)  # Multi-system pipeline needs time

            # Verify transformed results
            results = poll_until_messages(
                kafka_helper,
                sink_topic,
                min_messages=1,
                group_id=f"fullstack_verify_{suffix}",
                timeout=90.0,
                poll_interval=2.0,
            )

            assert len(results) > 0, "Should have transformed results"
            for r in results:
                assert "userid" in r, f"Missing userid: {r}"
                assert "region_upper" in r, f"Missing region_upper: {r}"
                if r["region_upper"] is not None:
                    assert r["region_upper"] == r["region_upper"].upper(), f"Should be uppercase: {r}"

        finally:
            flink_helper.cancel_all_running_jobs()
            try:
                connect_helper.delete_connector(connector_name)
            except Exception:
                pass
            kafka_helper.delete_topic(source_topic)
            kafka_helper.delete_topic(sink_topic)


@pytest.mark.integration
@pytest.mark.kafka
class TestDeployersIntegration:
    """Test using multiple deployers together."""

    def test_deploy_topic_artifacts(
        self,
        docker_services,
    ):
        """Test deploying multiple topic artifacts via KafkaDeployer."""
        deployer = KafkaDeployer(INFRA_CONFIG.kafka_bootstrap_servers)
        prefix = generate_unique_suffix()

        topics = [
            TopicArtifact(
                name=f"orders_{prefix}",
                partitions=6,
                replication_factor=1,
                config={"retention.ms": "86400000"},
            ),
            TopicArtifact(
                name=f"order_items_{prefix}",
                partitions=6,
                replication_factor=1,
                config={"retention.ms": "86400000"},
            ),
            TopicArtifact(
                name=f"order_totals_{prefix}",
                partitions=3,
                replication_factor=1,
                config={"cleanup.policy": "compact"},
            ),
        ]

        try:
            # Deploy all topics
            results = []
            for topic in topics:
                result = deployer.apply_topic(topic)
                results.append(result)

            assert all(r == "created" for r in results)

            # Verify all topics exist with correct config
            for topic in topics:
                state = deployer.get_topic_state(topic.name)
                assert state.exists
                assert state.partitions == topic.partitions

            # Re-apply should be idempotent
            results = []
            for topic in topics:
                result = deployer.apply_topic(topic)
                results.append(result)

            assert all(r == "unchanged" for r in results)

        finally:
            for topic in topics:
                try:
                    deployer.delete_topic(topic.name)
                except Exception:
                    pass

    def test_manifest_deployment(
        self,
        docker_services,
    ):
        """Test deploying a complete manifest (streamt artifact container)."""
        prefix = generate_unique_suffix()

        # Create a manifest with multiple artifacts
        manifest = Manifest(
            version="1.0.0",
            project_name=f"test_project_{prefix}",
            sources=[],
            models=[],
            tests=[],
            exposures=[],
            dag={},
            artifacts={
                "topics": [
                    {
                        "name": f"source_events_{prefix}",
                        "partitions": 6,
                        "replication_factor": 1,
                        "config": {},
                    },
                    {
                        "name": f"processed_events_{prefix}",
                        "partitions": 6,
                        "replication_factor": 1,
                        "config": {},
                    },
                ],
            },
        )

        kafka_deployer = KafkaDeployer(INFRA_CONFIG.kafka_bootstrap_servers)

        try:
            # Deploy all topics from manifest
            topic_artifacts = manifest.artifacts.get("topics", [])
            for topic_dict in topic_artifacts:
                artifact = TopicArtifact(
                    name=topic_dict["name"],
                    partitions=topic_dict["partitions"],
                    replication_factor=topic_dict["replication_factor"],
                    config=topic_dict.get("config", {}),
                )
                result = kafka_deployer.apply_topic(artifact)
                assert result == "created"

            # Verify deployment
            for topic_dict in topic_artifacts:
                state = kafka_deployer.get_topic_state(topic_dict["name"])
                assert state.exists

        finally:
            for topic_dict in manifest.artifacts.get("topics", []):
                try:
                    kafka_deployer.delete_topic(topic_dict["name"])
                except Exception:
                    pass


@pytest.mark.integration
@pytest.mark.kafka
@pytest.mark.flink
class TestDeployerPlanningMode:
    """Test planning capabilities across deployers."""

    def test_plan_mode_topics(
        self,
        docker_services,
    ):
        """Test planning topic changes without applying."""
        deployer = KafkaDeployer(INFRA_CONFIG.kafka_bootstrap_servers)
        topic_name = f"plan_test_{generate_unique_suffix()}"

        try:
            # Plan for non-existent topic
            artifact = TopicArtifact(
                name=topic_name,
                partitions=3,
                replication_factor=1,
            )

            plan = deployer.plan_topic(artifact)
            assert plan.action == "create"

            # Create the topic
            deployer.apply_topic(artifact)

            # Plan for update
            updated = TopicArtifact(
                name=topic_name,
                partitions=6,
                replication_factor=1,
                config={"retention.ms": "86400000"},
            )

            plan = deployer.plan_topic(updated)
            assert plan.action == "update"
            assert "partitions" in plan.changes
            assert "config.retention.ms" in plan.changes

            # Apply update and plan with same config
            deployer.apply_topic(updated)
            plan = deployer.plan_topic(updated)
            assert plan.action == "none"

        finally:
            try:
                deployer.delete_topic(topic_name)
            except Exception:
                pass

    def test_compute_diff(
        self,
        docker_services,
    ):
        """Test computing diffs between current and desired state."""
        deployer = KafkaDeployer(INFRA_CONFIG.kafka_bootstrap_servers)
        topic_name = f"diff_test_{generate_unique_suffix()}"

        try:
            # Create initial topic
            initial = TopicArtifact(
                name=topic_name,
                partitions=3,
                replication_factor=1,
                config={"retention.ms": "86400000"},
            )
            deployer.apply_topic(initial)

            # Compute diff with changes
            updated = TopicArtifact(
                name=topic_name,
                partitions=6,
                replication_factor=1,
                config={
                    "retention.ms": "604800000",
                    "cleanup.policy": "compact",
                },
            )

            diff = deployer.compute_diff(updated)

            assert "partitions" in diff
            assert "config.retention.ms" in diff
            assert "config.cleanup.policy" in diff

        finally:
            try:
                deployer.delete_topic(topic_name)
            except Exception:
                pass
