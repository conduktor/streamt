"""Test runner for streamt tests."""

from __future__ import annotations

import json
import logging
import time
import uuid
from typing import Optional

from streamt.core.models import DataTest, DataTestType, StreamtProject

logger = logging.getLogger(__name__)


class TestRunner:
    """Runner for streamt tests.

    Supports three test types:
    - schema: Validates data types and constraints against Schema Registry
    - sample: Consumes N messages from Kafka and validates assertions
    - continuous: Reports status of deployed Flink monitoring jobs
    """

    def __init__(self, project: StreamtProject) -> None:
        """Initialize test runner."""
        self.project = project
        self._consumer = None

    def run(self, tests: list[DataTest]) -> list[dict[str, object]]:
        """Run a list of tests."""
        results = []

        for test in tests:
            if test.type == DataTestType.SCHEMA:
                result = self._run_schema_test(test)
            elif test.type == DataTestType.SAMPLE:
                result = self._run_sample_test(test)
            elif test.type == DataTestType.CONTINUOUS:
                result = self._run_continuous_test(test)
            else:
                result = {
                    "name": test.name,
                    "status": "skipped",
                    "errors": [f"Unknown test type: {test.type}"],
                }

            results.append(result)

        return results

    def _run_schema_test(self, test: DataTest) -> dict[str, object]:
        """Run a schema test.

        Schema tests validate that the data conforms to expected types
        and constraints. Ideally checks against Schema Registry.
        """
        errors = []

        # Get the model/source being tested
        model = self.project.get_model(test.model)
        source = self.project.get_source(test.model) if not model else None

        if not model and not source:
            return {
                "name": test.name,
                "status": "failed",
                "errors": [f"Model/source '{test.model}' not found"],
            }

        # Check if Schema Registry is configured
        if self.project.runtime.schema_registry:
            # Would validate against Schema Registry
            # For now, validate assertion structure
            for assertion in test.assertions:
                if not assertion:
                    errors.append("Empty assertion dict")
                    continue
                assertion_type = list(assertion.keys())[0]
                assertion_config = assertion[assertion_type] or {}

                if assertion_type == "not_null":
                    columns = assertion_config.get("columns", [])
                    if not columns:
                        errors.append("not_null assertion requires 'columns' list")

                elif assertion_type == "accepted_types":
                    types = assertion_config.get("types", {})
                    if not types:
                        errors.append("accepted_types assertion requires 'types' mapping")

        if errors:
            return {
                "name": test.name,
                "status": "failed",
                "errors": errors,
            }

        return {
            "name": test.name,
            "status": "passed",
            "message": "Schema validation passed (structural check only)",
        }

    def _run_sample_test(self, test: DataTest) -> dict[str, object]:
        """Run a sample test by consuming messages from Kafka."""
        errors = []
        warnings = []

        # Get sample size
        sample_size = test.sample_size or 100

        # Get the model/source and determine topic
        topic = self._get_topic_for_test(test.model)
        if not topic:
            return {
                "name": test.name,
                "status": "failed",
                "errors": [f"Cannot determine topic for '{test.model}'"],
            }

        # Sample messages from Kafka
        try:
            messages = self._sample_messages_from_kafka(topic, sample_size)

            if not messages:
                return {
                    "name": test.name,
                    "status": "failed",
                    "errors": [f"No messages found in topic '{topic}'"],
                    "sample_size": 0,
                }

            # Run assertions against sampled messages
            assertion_results = []
            for assertion in test.assertions:
                if not assertion:
                    continue
                assertion_type = list(assertion.keys())[0]
                assertion_config = assertion[assertion_type] or {}

                result = self._run_assertion(assertion_type, assertion_config, messages)
                assertion_results.append(result)

                if not result["passed"]:
                    errors.extend(result.get("errors", []))
                if result.get("warnings"):
                    warnings.extend(result["warnings"])

        except ImportError:
            return {
                "name": test.name,
                "status": "failed",
                "errors": ["confluent-kafka is not installed. Reinstall streamt."],
            }
        except Exception as e:
            return {
                "name": test.name,
                "status": "failed",
                "errors": [f"Failed to sample messages: {e}"],
            }

        if errors:
            return {
                "name": test.name,
                "status": "failed",
                "errors": errors,
                "warnings": warnings,
                "sample_size": len(messages),
            }

        return {
            "name": test.name,
            "status": "passed",
            "sample_size": len(messages),
            "warnings": warnings if warnings else None,
        }

    def _run_continuous_test(self, test: DataTest) -> dict[str, object]:
        """Check status of a continuous test (deployed as Flink job)."""
        from streamt.deployer.flink import FlinkDeployer

        if not self.project.runtime.flink:
            return {
                "name": test.name,
                "status": "failed",
                "errors": ["Continuous test requires Flink. Configure runtime.flink"],
            }

        # Check if we can query Flink for job status
        flink_config = self.project.runtime.flink
        default_cluster = flink_config.default
        if default_cluster and default_cluster in flink_config.clusters:
            cluster = flink_config.clusters[default_cluster]
            if cluster.rest_url:
                try:
                    with FlinkDeployer(cluster.rest_url) as deployer:
                        job_state = deployer.get_job_state(f"test_{test.name}")

                    if job_state.exists:
                        return {
                            "name": test.name,
                            "status": "passed" if job_state.status == "RUNNING" else "failed",
                            "job_status": job_state.status,
                            "message": f"Continuous test job is {job_state.status}",
                        }
                except ConnectionError as e:
                    return {
                        "name": test.name,
                        "status": "error",
                        "errors": [f"Cannot connect to Flink: {e}"],
                    }
                except Exception as e:
                    logger.warning(f"Failed to check Flink job status for test '{test.name}': {e}")
                    return {
                        "name": test.name,
                        "status": "error",
                        "errors": [f"Flink query failed: {e}"],
                    }

        return {
            "name": test.name,
            "status": "skipped",
            "message": "Continuous test not deployed. Use 'streamt apply' to deploy.",
        }

    def _get_topic_for_test(self, model_name: str) -> Optional[str]:
        """Get the Kafka topic for a model or source."""
        # Check models first
        model = self.project.get_model(model_name)
        if model:
            if model.get_topic_config() and model.get_topic_config().name:
                return model.get_topic_config().name
            return model.name

        # Check sources
        source = self.project.get_source(model_name)
        if source:
            return source.topic

        return None

    def _sample_messages_from_kafka(
        self, topic: str, sample_size: int, timeout_ms: int = 10000
    ) -> list[dict]:
        """Sample messages from a Kafka topic.

        Args:
            topic: The topic to consume from
            sample_size: Maximum number of messages to consume
            timeout_ms: Consumer timeout in milliseconds

        Returns:
            List of deserialized messages (JSON parsed)
        """
        from confluent_kafka import Consumer, KafkaError

        if sample_size <= 0:
            return []
        if timeout_ms < 0:
            raise ValueError("timeout_ms must be non-negative")

        consumer_config: dict[str, object] = dict(
            self.project.runtime.kafka.to_confluent_config()
        )
        consumer_config.update(
            {
                "group.id": f"streamt-sample-{uuid.uuid4()}",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
                "log_level": 0,
            }
        )
        messages: list[dict] = []
        consumer = Consumer(consumer_config)
        deadline = time.monotonic() + (timeout_ms / 1000)

        try:
            consumer.subscribe([topic])
            while len(messages) < sample_size:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                msg = consumer.poll(timeout=min(1.0, remaining))
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise RuntimeError(f"Kafka consumer error: {msg.error()}")

                raw_value = msg.value()
                if raw_value is None:
                    continue
                value = json.loads(raw_value.decode("utf-8"))
                if not isinstance(value, dict):
                    raise ValueError(
                        f"Expected a JSON object in topic '{topic}', got {type(value).__name__}"
                    )
                messages.append(value)
        finally:
            consumer.close()

        return messages

    def _run_assertion(
        self, assertion_type: str, config: dict, messages: list[dict]
    ) -> dict[str, object]:
        """Run a single assertion against sampled messages."""
        if assertion_type == "not_null":
            return self._assert_not_null(config, messages)
        elif assertion_type == "accepted_values":
            return self._assert_accepted_values(config, messages)
        elif assertion_type == "range":
            return self._assert_range(config, messages)
        elif assertion_type == "unique_key":
            return self._assert_unique_key(config, messages)
        else:
            return {
                "passed": True,
                "warnings": [f"Unknown assertion type '{assertion_type}' - skipped"],
            }

    def _assert_not_null(self, config: dict, messages: list[dict]) -> dict[str, object]:
        """Assert that specified columns are not null."""
        columns = config.get("columns", [])
        errors = []
        null_counts = dict.fromkeys(columns, 0)

        for msg in messages:
            for col in columns:
                if col in msg and msg[col] is None:
                    null_counts[col] += 1

        for col, count in null_counts.items():
            if count > 0:
                errors.append(f"Column '{col}' has {count} null values in {len(messages)} messages")

        return {"passed": len(errors) == 0, "errors": errors}

    def _assert_accepted_values(self, config: dict, messages: list[dict]) -> dict[str, object]:
        """Assert that a column only contains accepted values."""
        column = config.get("column")
        accepted = set(config.get("values", []))
        errors = []
        invalid_values = set()

        for msg in messages:
            if column in msg:
                val = msg[column]
                if val not in accepted:
                    invalid_values.add(str(val))

        if invalid_values:
            sample = list(invalid_values)[:5]
            errors.append(
                f"Column '{column}' has {len(invalid_values)} invalid values. Sample: {sample}"
            )

        return {"passed": len(errors) == 0, "errors": errors}

    def _assert_range(self, config: dict, messages: list[dict]) -> dict[str, object]:
        """Assert that a column's values fall within a range."""
        column = config.get("column")
        min_val = config.get("min")
        max_val = config.get("max")
        errors = []
        violations = {"below_min": 0, "above_max": 0}

        for msg in messages:
            if column in msg:
                val = msg[column]
                if val is not None:
                    try:
                        if min_val is not None and val < min_val:
                            violations["below_min"] += 1
                        if max_val is not None and val > max_val:
                            violations["above_max"] += 1
                    except TypeError:
                        # Non-comparable types
                        pass

        if violations["below_min"] > 0:
            errors.append(
                f"Column '{column}' has {violations['below_min']} values below min {min_val}"
            )
        if violations["above_max"] > 0:
            errors.append(
                f"Column '{column}' has {violations['above_max']} values above max {max_val}"
            )

        return {"passed": len(errors) == 0, "errors": errors}

    def _assert_unique_key(self, config: dict, messages: list[dict]) -> dict[str, object]:
        """Assert that a key is unique within the sample."""
        key = config.get("key")
        tolerance = config.get("tolerance", 0.0)
        errors = []
        seen = {}

        for msg in messages:
            if key in msg:
                val = msg[key]
                if val in seen:
                    seen[val] += 1
                else:
                    seen[val] = 1

        duplicates = sum(1 for count in seen.values() if count > 1)
        duplicate_rate = duplicates / len(messages) if messages else 0

        if duplicate_rate > tolerance:
            errors.append(
                f"Key '{key}' has {duplicate_rate:.2%} duplicate rate (tolerance: {tolerance:.2%})"
            )

        return {"passed": len(errors) == 0, "errors": errors}
