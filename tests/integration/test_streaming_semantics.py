"""End-to-end tests for streaming semantics via streamt YAML workflow.

These tests verify critical streaming behaviors through streamt's abstractions:
1. Windowed aggregation semantics (tumbling, hopping windows)
2. Watermark configuration and SQL generation
3. State TTL configuration
4. Event time processing

NOTE: Tests use streamt's YAML → Compiler → Deployer workflow.
Pure Kafka infrastructure tests have been moved to tests/infrastructure/.
"""

import tempfile
import time
import uuid
from pathlib import Path

import pytest
import yaml

from streamt.compiler import Compiler
from streamt.compiler.manifest import FlinkJobArtifact, TopicArtifact
from streamt.core.parser import ProjectParser
from streamt.deployer.flink import FlinkDeployer
from streamt.deployer.kafka import KafkaDeployer

from .conftest import INFRA_CONFIG, FlinkHelper, KafkaHelper, poll_until_messages


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


def deploy_manifest_topics(manifest) -> None:
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


def deploy_manifest_flink_jobs(manifest) -> list[str]:
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
@pytest.mark.flink
class TestWindowedAggregationSemantics:
    """Test windowed aggregation semantics via streamt YAML workflow."""

    def test_tumbling_window_aggregation(
        self, docker_services, flink_helper: FlinkHelper, kafka_helper: KafkaHelper
    ):
        """TC-STREAMING-001: Tumbling window should aggregate events correctly.

        This test verifies streamt's windowed aggregation via YAML config:
        - Events within the same window are aggregated together
        - Window results are emitted after watermark advances past window end
        - Multiple windows produce separate results

        Uses streamt: YAML → Compiler → FlinkDeployer → verify results
        """
        suffix = generate_unique_suffix()
        source_topic = f"test_tumble_src_{suffix}"
        output_topic = f"test_tumble_out_{suffix}"

        project_yaml = f"""
project:
  name: tumbling-window-test-{suffix}
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
  - name: events
    topic: {source_topic}
    columns:
      - name: event_id
        type: STRING
      - name: category
        type: STRING
      - name: amount
        type: DOUBLE
      - name: event_time
        type: TIMESTAMP(3)
    event_time:
      column: event_time
      watermark:
        strategy: bounded_out_of_orderness
        max_out_of_orderness_ms: 2000

models:
  - name: category_window_stats
    description: Aggregate events per category in 10-second tumbling windows
    sql: |
      SELECT
        category,
        TUMBLE_START(event_time, INTERVAL '10' SECOND) as window_start,
        TUMBLE_END(event_time, INTERVAL '10' SECOND) as window_end,
        COUNT(*) as event_count,
        SUM(amount) as total_amount
      FROM {{{{ source("events") }}}}
      GROUP BY category, TUMBLE(event_time, INTERVAL '10' SECOND)
    advanced:
      topic:
        name: {output_topic}
        partitions: 1
"""

        try:
            kafka_helper.create_topic(source_topic, partitions=1)

            # Compile and deploy via streamt
            manifest, tmpdir = create_and_compile_project(project_yaml)

            # Verify compilation generated correct SQL with watermark
            job_sql = manifest.artifacts["flink_jobs"][0]["sql"]
            assert "WATERMARK" in job_sql, "Compiled SQL should have WATERMARK clause"
            assert "TUMBLE" in job_sql, "Compiled SQL should have TUMBLE aggregation"

            deploy_manifest_topics(manifest)
            deploy_manifest_flink_jobs(manifest)

            time.sleep(3)

            # Create events all in the SAME 10-second window
            current_epoch_s = int(time.time())
            window_base = ((current_epoch_s - 30) // 10) * 10 + 2

            events = [
                {
                    "event_id": f"evt_{i}",
                    "category": "electronics",
                    "amount": 100.0 + i * 10,
                    "event_time": time.strftime("%Y-%m-%d %H:%M:%S.000", time.gmtime(window_base + i)),
                }
                for i in range(5)
            ]

            kafka_helper.produce_messages(source_topic, events)

            # Advance watermark by sending event in the future
            future_time = int(time.time()) + 120
            watermark_event = {
                "event_id": "watermark_advance",
                "category": "other",
                "amount": 1.0,
                "event_time": time.strftime("%Y-%m-%d %H:%M:%S.000", time.gmtime(future_time)),
            }
            kafka_helper.produce_messages(source_topic, [watermark_event])

            time.sleep(10)

            # Wait for window result
            messages = poll_until_messages(
                kafka_helper,
                output_topic,
                min_messages=1,
                timeout=60.0,
                group_id=f"test_consumer_{suffix}",
            )

            # Verify aggregation - find the electronics window result
            electronics_result = None
            for msg in messages:
                if msg.get("category") == "electronics":
                    electronics_result = msg
                    break

            assert electronics_result is not None, f"Should have electronics window result. Got: {messages}"
            assert electronics_result["event_count"] == 5, f"Expected 5 events, got {electronics_result['event_count']}"
            expected_sum = sum(100.0 + i * 10 for i in range(5))
            assert abs(electronics_result["total_amount"] - expected_sum) < 0.01, \
                f"Expected sum {expected_sum}, got {electronics_result['total_amount']}"

        finally:
            flink_helper.cancel_all_running_jobs()
            try:
                kafka_helper.delete_topic(source_topic)
                kafka_helper.delete_topic(output_topic)
            except Exception:
                pass


@pytest.mark.integration
@pytest.mark.flink
class TestWatermarkBehavior:
    """Test watermark and out-of-order event handling via streamt."""

    def _create_project(self, tmpdir: Path, config: dict):
        """Create a project and return parsed project."""
        with open(tmpdir / "stream_project.yml", "w") as f:
            yaml.dump(config, f)
        parser = ProjectParser(tmpdir)
        return parser.parse()

    def test_watermark_configuration_generates_correct_sql(self, docker_services):
        """TC-STREAMING-002: Watermark config should generate correct Flink SQL.

        This test validates that streamt correctly generates Flink SQL with:
        - Proper WATERMARK clause in source table definition
        - Correct delay interval based on max_out_of_orderness_ms config
        """
        suffix = generate_unique_suffix()
        source_topic = f"test_wm_src_{suffix}"
        output_topic = f"test_wm_out_{suffix}"

        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test-watermark-sql", "version": "1.0.0"},
                "runtime": {
                    "kafka": {
                        "bootstrap_servers": "localhost:9092",
                        "bootstrap_servers_internal": "kafka:29092",
                    },
                },
                "sources": [
                    {
                        "name": "events",
                        "topic": source_topic,
                        "columns": [
                            {"name": "event_id"},
                            {"name": "value", "type": "INT"},
                            {"name": "event_time", "type": "TIMESTAMP(3)"},
                        ],
                        "event_time": {
                            "column": "event_time",
                            "watermark": {
                                "strategy": "bounded_out_of_orderness",
                                "max_out_of_orderness_ms": 5000,  # 5 seconds
                            },
                        },
                    }
                ],
                "models": [
                    {
                        "name": output_topic,
                        "sql": """
                            SELECT
                                event_id,
                                value,
                                event_time
                            FROM {{ source("events") }}
                        """,
                    }
                ],
            }
            project = self._create_project(Path(tmpdir), config)
            output_dir = Path(tmpdir) / "generated"
            compiler = Compiler(project, output_dir)
            compiler.compile(dry_run=False)

            sql_path = output_dir / "flink" / f"{output_topic}.sql"
            sql_content = sql_path.read_text()

            # Verify watermark clause is present
            assert "WATERMARK FOR `event_time`" in sql_content, \
                f"Should have WATERMARK clause. SQL: {sql_content}"

            # Verify 5-second delay (5000ms = 5 seconds)
            assert "INTERVAL '5' SECOND" in sql_content, \
                f"Should have 5-second watermark delay. SQL: {sql_content}"

            # Verify event_time column is TIMESTAMP(3)
            assert "`event_time` TIMESTAMP(3)" in sql_content, \
                f"event_time should be TIMESTAMP(3). SQL: {sql_content}"

    @pytest.mark.xfail(reason="Compiler doesn't yet support monotonously_increasing watermark strategy")
    def test_monotonously_increasing_watermark_strategy(self, docker_services):
        """TC-STREAMING-003: Monotonously increasing watermark should use row-time directly.

        For perfectly ordered streams, watermark = event_time (no delay).
        """
        suffix = generate_unique_suffix()
        source_topic = f"test_mono_src_{suffix}"
        output_topic = f"test_mono_out_{suffix}"

        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test-mono-watermark", "version": "1.0.0"},
                "runtime": {
                    "kafka": {
                        "bootstrap_servers": "localhost:9092",
                        "bootstrap_servers_internal": "kafka:29092",
                    },
                },
                "sources": [
                    {
                        "name": "events",
                        "topic": source_topic,
                        "columns": [
                            {"name": "event_id"},
                            {"name": "event_time", "type": "TIMESTAMP(3)"},
                        ],
                        "event_time": {
                            "column": "event_time",
                            "watermark": {
                                "strategy": "monotonously_increasing",
                            },
                        },
                    }
                ],
                "models": [
                    {
                        "name": output_topic,
                        "sql": "SELECT * FROM {{ source(\"events\") }}",
                    }
                ],
            }
            project = self._create_project(Path(tmpdir), config)
            output_dir = Path(tmpdir) / "generated"
            compiler = Compiler(project, output_dir)
            compiler.compile(dry_run=False)

            sql_path = output_dir / "flink" / f"{output_topic}.sql"
            sql_content = sql_path.read_text()

            # Monotonously increasing should not have INTERVAL subtraction
            assert "WATERMARK FOR `event_time`" in sql_content
            # Check for direct assignment (no subtraction)
            assert "AS `event_time`" in sql_content or "- INTERVAL" not in sql_content.split("WATERMARK")[1].split(")")[0]


@pytest.mark.integration
@pytest.mark.flink
class TestStateTTL:
    """Test state TTL configuration and behavior via streamt."""

    def _create_project(self, tmpdir: Path, config: dict):
        """Create a project and return parsed project."""
        with open(tmpdir / "stream_project.yml", "w") as f:
            yaml.dump(config, f)
        parser = ProjectParser(tmpdir)
        return parser.parse()

    def test_state_ttl_generates_correct_config(
        self, docker_services, flink_helper: FlinkHelper
    ):
        """TC-STREAMING-004: State TTL should generate correct Flink SET statement.

        State TTL is critical for:
        - Preventing unbounded state growth in joins
        - Cleaning up old keys in aggregations
        - Managing memory in long-running jobs
        """
        suffix = generate_unique_suffix()
        source_topic = f"test_ttl_src_{suffix}"
        output_topic = f"test_ttl_out_{suffix}"

        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test-ttl", "version": "1.0.0"},
                "runtime": {
                    "kafka": {
                        "bootstrap_servers": INFRA_CONFIG.kafka_bootstrap_servers,
                        "bootstrap_servers_internal": "kafka:29092",
                    },
                    "flink": {
                        "default": "local",
                        "clusters": {
                            "local": {
                                "type": "rest",
                                "rest_url": INFRA_CONFIG.flink_rest_url,
                            }
                        },
                    },
                },
                "sources": [
                    {
                        "name": "events",
                        "topic": source_topic,
                        "columns": [
                            {"name": "user_id"},
                            {"name": "value"},
                        ],
                    }
                ],
                "models": [
                    {
                        "name": output_topic,
                        "advanced": {
                            "flink": {
                                "state_ttl_ms": 3600000,  # 1 hour
                            }
                        },
                        "sql": """
                            SELECT
                                user_id,
                                COUNT(*) as event_count
                            FROM {{ source("events") }}
                            GROUP BY user_id
                        """,
                    }
                ],
            }
            project = self._create_project(Path(tmpdir), config)
            output_dir = Path(tmpdir) / "generated"
            compiler = Compiler(project, output_dir)
            compiler.compile(dry_run=False)

            sql_path = output_dir / "flink" / f"{output_topic}.sql"
            sql_content = sql_path.read_text()

            # Verify SET statement for state TTL
            assert "SET 'table.exec.state.ttl'" in sql_content, \
                "Should have state TTL SET statement"
            # TTL can be formatted as "1 h", "3600 s", "3600000 ms", etc.
            assert "'1 h'" in sql_content or "'3600 s'" in sql_content or "'3600000 ms'" in sql_content or "'3600000ms'" in sql_content, \
                f"Should have 1 hour TTL value. SQL: {sql_content}"


@pytest.mark.integration
@pytest.mark.flink
class TestIdempotentProcessing:
    """Test idempotent processing patterns via streamt."""

    @pytest.mark.xfail(reason="Kafka sink doesn't support changelog (update/delete) from ROW_NUMBER")
    def test_deduplication_model(
        self, docker_services, flink_helper: FlinkHelper, kafka_helper: KafkaHelper
    ):
        """TC-STREAMING-005: Deduplication model should remove duplicate events.

        Uses streamt model with DISTINCT or ROW_NUMBER deduplication pattern.
        """
        suffix = generate_unique_suffix()
        source_topic = f"test_dedup_src_{suffix}"
        output_topic = f"test_dedup_out_{suffix}"

        project_yaml = f"""
project:
  name: dedup-test-{suffix}
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
  - name: raw_events
    topic: {source_topic}
    columns:
      - name: event_id
        type: STRING
      - name: user_id
        type: STRING
      - name: action
        type: STRING
      - name: proc_time
        type: TIMESTAMP(3)
        proctime: true

models:
  - name: deduped_events
    description: Deduplicate events by event_id using ROW_NUMBER
    sql: |
      SELECT event_id, user_id, action
      FROM (
        SELECT
          event_id, user_id, action,
          ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY proc_time) as rn
        FROM {{{{ source("raw_events") }}}}
      )
      WHERE rn = 1
    advanced:
      topic:
        name: {output_topic}
        partitions: 1
"""

        try:
            kafka_helper.create_topic(source_topic, partitions=1)

            # Compile and deploy via streamt
            manifest, tmpdir = create_and_compile_project(project_yaml)

            # Verify compilation produced deduplication SQL
            job_sql = manifest.artifacts["flink_jobs"][0]["sql"]
            assert "ROW_NUMBER()" in job_sql, "Compiled SQL should have ROW_NUMBER for dedup"

            deploy_manifest_topics(manifest)
            deploy_manifest_flink_jobs(manifest)

            time.sleep(5)

            # Send duplicate events
            events = [
                {"event_id": "evt_1", "user_id": "user_a", "action": "click"},
                {"event_id": "evt_1", "user_id": "user_a", "action": "click"},  # Duplicate
                {"event_id": "evt_2", "user_id": "user_b", "action": "view"},
                {"event_id": "evt_1", "user_id": "user_a", "action": "click"},  # Another duplicate
                {"event_id": "evt_3", "user_id": "user_a", "action": "purchase"},
            ]
            kafka_helper.produce_messages(source_topic, events)

            # Wait for deduplication processing
            time.sleep(10)

            # Verify deduplication: should have 3 unique event_ids
            results = poll_until_messages(
                kafka_helper,
                output_topic,
                min_messages=3,
                timeout=60.0,
                group_id=f"dedup_verify_{suffix}",
            )

            event_ids = {r["event_id"] for r in results}
            assert event_ids == {"evt_1", "evt_2", "evt_3"}, \
                f"Should have exactly 3 unique events, got {event_ids}"

            # Count occurrences - each event_id should appear only once
            event_id_counts = {}
            for r in results:
                eid = r["event_id"]
                event_id_counts[eid] = event_id_counts.get(eid, 0) + 1

            for eid, count in event_id_counts.items():
                assert count == 1, f"Event {eid} should appear once, got {count}"

        finally:
            flink_helper.cancel_all_running_jobs()
            try:
                kafka_helper.delete_topic(source_topic)
                kafka_helper.delete_topic(output_topic)
            except Exception:
                pass


@pytest.mark.integration
@pytest.mark.flink
class TestEventTimeVsProcessingTime:
    """Test event time vs processing time semantics via streamt."""

    def test_event_time_watermark_compilation(self, docker_services):
        """TC-STREAMING-006: Event time config should compile to Flink WATERMARK.

        Validates the full chain: YAML event_time config → Compiler → Flink SQL
        """
        suffix = generate_unique_suffix()

        with tempfile.TemporaryDirectory() as tmpdir:
            project_yaml = f"""
project:
  name: event-time-test-{suffix}
  version: "1.0.0"

runtime:
  kafka:
    bootstrap_servers: localhost:9092
    bootstrap_servers_internal: kafka:29092

sources:
  - name: events
    topic: events_{suffix}
    columns:
      - name: event_id
        type: STRING
      - name: ts
        type: TIMESTAMP(3)
    event_time:
      column: ts
      watermark:
        strategy: bounded_out_of_orderness
        max_out_of_orderness_ms: 10000

models:
  - name: windowed_counts
    sql: |
      SELECT
        TUMBLE_START(ts, INTERVAL '1' MINUTE) as window_start,
        COUNT(*) as cnt
      FROM {{{{ source("events") }}}}
      GROUP BY TUMBLE(ts, INTERVAL '1' MINUTE)
"""
            project_file = Path(tmpdir) / "stream_project.yml"
            project_file.write_text(project_yaml)

            parser = ProjectParser(Path(tmpdir))
            project = parser.parse()
            output_dir = Path(tmpdir) / "generated"
            compiler = Compiler(project, output_dir)
            compiler.compile(dry_run=False)

            sql_path = output_dir / "flink" / "windowed_counts.sql"
            sql_content = sql_path.read_text()

            # Verify event time configuration
            assert "WATERMARK FOR `ts`" in sql_content, \
                "Should have WATERMARK clause for event time column"
            assert "INTERVAL '10' SECOND" in sql_content, \
                "Should have 10-second out-of-orderness (10000ms)"

    @pytest.mark.xfail(reason="Compiler doesn't yet generate PROCTIME() for proctime columns")
    def test_processing_time_model(
        self, docker_services, flink_helper: FlinkHelper, kafka_helper: KafkaHelper
    ):
        """TC-STREAMING-007: Processing time model should use PROCTIME().

        Validates that streamt can generate processing-time based transformations.
        """
        suffix = generate_unique_suffix()
        source_topic = f"proctime_src_{suffix}"
        output_topic = f"proctime_out_{suffix}"

        project_yaml = f"""
project:
  name: proctime-test-{suffix}
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
  - name: events
    topic: {source_topic}
    columns:
      - name: event_id
        type: STRING
      - name: value
        type: INT
      - name: proc_time
        type: TIMESTAMP(3)
        proctime: true

models:
  - name: recent_events
    description: Filter events based on processing time window
    sql: |
      SELECT event_id, value
      FROM {{{{ source("events") }}}}
    advanced:
      topic:
        name: {output_topic}
        partitions: 1
"""

        try:
            kafka_helper.create_topic(source_topic, partitions=1)

            # Compile via streamt
            manifest, tmpdir = create_and_compile_project(project_yaml)

            # Verify PROCTIME was generated
            job_sql = manifest.artifacts["flink_jobs"][0]["sql"]
            assert "PROCTIME()" in job_sql or "AS PROCTIME()" in job_sql, \
                f"Compiled SQL should have PROCTIME(). SQL: {job_sql}"

            deploy_manifest_topics(manifest)
            deploy_manifest_flink_jobs(manifest)

            time.sleep(5)

            # Produce and verify data flows
            events = [
                {"event_id": "e1", "value": 100},
                {"event_id": "e2", "value": 200},
            ]
            kafka_helper.produce_messages(source_topic, events)

            results = poll_until_messages(
                kafka_helper,
                output_topic,
                min_messages=1,
                timeout=30.0,
                group_id=f"proctime_verify_{suffix}",
            )

            assert len(results) > 0, "Should have output from processing-time model"

        finally:
            flink_helper.cancel_all_running_jobs()
            try:
                kafka_helper.delete_topic(source_topic)
                kafka_helper.delete_topic(output_topic)
            except Exception:
                pass
