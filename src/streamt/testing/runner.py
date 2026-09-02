"""Test runner for streamt tests."""

from __future__ import annotations

import json
import logging
import time
import uuid
from dataclasses import dataclass
from typing import TypedDict

from pydantic import BaseModel, ValidationError

from streamt.core.models import (
    AcceptedTypesAssertion,
    AcceptedValuesAssertion,
    CustomSqlAssertion,
    DataTest,
    DataTestType,
    DistributionAssertion,
    ForeignKeyAssertion,
    MaxLagAssertion,
    NotNullAssertion,
    RangeAssertion,
    StreamtProject,
    ThroughputAssertion,
    UniqueKeyAssertion,
)

logger = logging.getLogger(__name__)

JsonRecord = dict[str, object]


class AssertionResult(TypedDict, total=False):
    """Structured result returned by a sample assertion."""

    passed: bool
    errors: list[str]
    warnings: list[str]


@dataclass(frozen=True)
class _ParsedAssertion:
    """A shape-checked assertion and its validated configuration."""

    assertion_type: str
    config: BaseModel | dict[str, object]


_ASSERTION_CONFIG_MODELS: dict[str, type[BaseModel]] = {
    "not_null": NotNullAssertion,
    "unique_key": UniqueKeyAssertion,
    "accepted_values": AcceptedValuesAssertion,
    "accepted_types": AcceptedTypesAssertion,
    "range": RangeAssertion,
    "max_lag": MaxLagAssertion,
    "throughput": ThroughputAssertion,
    "distribution": DistributionAssertion,
    "foreign_key": ForeignKeyAssertion,
    "custom_sql": CustomSqlAssertion,
}


def resolve_sample_test_topic(project: StreamtProject, target_name: str) -> str | None:
    """Resolve the exact Kafka topic consumed by a sample test target."""
    model = project.get_model(target_name)
    if model:
        topic_config = model.get_topic_config()
        if topic_config and topic_config.name:
            return topic_config.name
        return model.name

    source = project.get_source(target_name)
    if source:
        return source.topic

    return None


def _format_assertion_error(index: int, assertion_type: str, exc: ValidationError) -> str:
    """Format assertion validation failures with stable collection paths."""
    details = []
    for error in exc.errors():
        location = " → ".join(str(part) for part in error["loc"])
        path = f"assertions → {index} → {assertion_type}"
        if location:
            path = f"{path} → {location}"
        details.append(f"field '{path}': {error['msg']}")
    return "; ".join(details)


def _parse_assertions(assertions: object) -> tuple[list[_ParsedAssertion], list[str]]:
    """Validate assertion container/config shapes without trusting model construction."""
    if not isinstance(assertions, list):
        return [], ["field 'assertions' must be a list"]

    parsed: list[_ParsedAssertion] = []
    errors: list[str] = []
    for index, assertion in enumerate(assertions):
        if not isinstance(assertion, dict) or len(assertion) != 1:
            errors.append(
                f"field 'assertions → {index}' must contain exactly one assertion type"
            )
            continue

        assertion_type, raw_config = next(iter(assertion.items()))
        if not isinstance(assertion_type, str):
            errors.append(f"field 'assertions → {index}' must use a string assertion type")
            continue

        config_model = _ASSERTION_CONFIG_MODELS.get(assertion_type)
        if config_model is not None:
            try:
                config: BaseModel | dict[str, object] = config_model.model_validate(raw_config)
            except ValidationError as exc:
                errors.append(_format_assertion_error(index, assertion_type, exc))
                continue
        elif isinstance(raw_config, dict):
            config = {
                key: value for key, value in raw_config.items() if isinstance(key, str)
            }
            if len(config) != len(raw_config):
                errors.append(
                    f"field 'assertions → {index} → {assertion_type}' "
                    "must use string keys"
                )
                continue
        else:
            errors.append(
                f"field 'assertions → {index} → {assertion_type}' must be a mapping"
            )
            continue

        parsed.append(_ParsedAssertion(assertion_type=assertion_type, config=config))

    return parsed, errors


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
        results: list[dict[str, object]] = []

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
        errors: list[str] = []

        # Get the model/source being tested
        model = self.project.get_model(test.model)
        source = self.project.get_source(test.model) if not model else None

        if not model and not source:
            return {
                "name": test.name,
                "status": "failed",
                "errors": [f"Model/source '{test.model}' not found"],
            }

        parsed_assertions, assertion_errors = _parse_assertions(test.assertions)
        if assertion_errors:
            return {
                "name": test.name,
                "status": "failed",
                "errors": assertion_errors,
            }

        # Check if Schema Registry is configured
        if self.project.runtime.schema_registry:
            # Would validate against Schema Registry
            # For now, validate assertion structure
            for assertion in parsed_assertions:
                if assertion.assertion_type == "not_null" and isinstance(
                    assertion.config, NotNullAssertion
                ):
                    if not assertion.config.columns:
                        errors.append("not_null assertion requires 'columns' list")

                elif assertion.assertion_type == "accepted_types" and isinstance(
                    assertion.config, AcceptedTypesAssertion
                ):
                    if not assertion.config.types:
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
        errors: list[str] = []
        warnings: list[str] = []

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

        parsed_assertions, assertion_errors = _parse_assertions(test.assertions)
        if assertion_errors:
            return {
                "name": test.name,
                "status": "failed",
                "errors": assertion_errors,
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
            assertion_results: list[AssertionResult] = []
            for assertion in parsed_assertions:
                result = self._run_assertion(assertion, messages)
                assertion_results.append(result)

                if not result.get("passed", False):
                    errors.extend(result.get("errors", []))
                warnings.extend(result.get("warnings", []))

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

    def _get_topic_for_test(self, model_name: str) -> str | None:
        """Get the Kafka topic for a model or source."""
        return resolve_sample_test_topic(self.project, model_name)

    def _sample_messages_from_kafka(
        self, topic: str, sample_size: int, timeout_ms: int = 10000
    ) -> list[JsonRecord]:
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
        messages: list[JsonRecord] = []
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
                message_error = msg.error()
                if message_error is not None:
                    if message_error.code() == KafkaError._PARTITION_EOF:
                        continue
                    raise RuntimeError(f"Kafka consumer error: {message_error}")

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
        self, assertion: _ParsedAssertion, messages: list[JsonRecord]
    ) -> AssertionResult:
        """Run a single assertion against sampled messages."""
        if assertion.assertion_type == "not_null" and isinstance(
            assertion.config, NotNullAssertion
        ):
            return self._assert_not_null(assertion.config, messages)
        if assertion.assertion_type == "accepted_values" and isinstance(
            assertion.config, AcceptedValuesAssertion
        ):
            return self._assert_accepted_values(assertion.config, messages)
        if assertion.assertion_type == "range" and isinstance(
            assertion.config, RangeAssertion
        ):
            return self._assert_range(assertion.config, messages)
        if assertion.assertion_type == "unique_key" and isinstance(
            assertion.config, UniqueKeyAssertion
        ):
            return self._assert_unique_key(assertion.config, messages)
        return {
            "passed": True,
            "warnings": [
                f"Unknown assertion type '{assertion.assertion_type}' - skipped"
            ],
        }

    def _assert_not_null(
        self, config: NotNullAssertion, messages: list[JsonRecord]
    ) -> AssertionResult:
        """Assert that specified columns are not null."""
        errors: list[str] = []
        null_counts: dict[str, int] = dict.fromkeys(config.columns, 0)

        for message in messages:
            for column in config.columns:
                if column in message and message[column] is None:
                    null_counts[column] += 1

        for column, count in null_counts.items():
            if count > 0:
                errors.append(
                    f"Column '{column}' has {count} null values in {len(messages)} messages"
                )

        return {"passed": len(errors) == 0, "errors": errors}

    def _assert_accepted_values(
        self, config: AcceptedValuesAssertion, messages: list[JsonRecord]
    ) -> AssertionResult:
        """Assert that a column only contains accepted values."""
        errors: list[str] = []
        invalid_values: set[str] = set()

        for message in messages:
            if config.column in message:
                value = message[config.column]
                if not any(value == accepted for accepted in config.values):
                    invalid_values.add(str(value))

        if invalid_values:
            sample = list(invalid_values)[:5]
            errors.append(
                f"Column '{config.column}' has {len(invalid_values)} invalid values. "
                f"Sample: {sample}"
            )

        return {"passed": len(errors) == 0, "errors": errors}

    def _assert_range(
        self, config: RangeAssertion, messages: list[JsonRecord]
    ) -> AssertionResult:
        """Assert that a column's values fall within a range."""
        errors: list[str] = []
        violations: dict[str, int] = {"below_min": 0, "above_max": 0}

        for message in messages:
            if config.column in message:
                value = message[config.column]
                if isinstance(value, (int, float)):
                    if config.min is not None and value < config.min:
                        violations["below_min"] += 1
                    if config.max is not None and value > config.max:
                        violations["above_max"] += 1

        if violations["below_min"] > 0:
            errors.append(
                f"Column '{config.column}' has {violations['below_min']} values "
                f"below min {config.min}"
            )
        if violations["above_max"] > 0:
            errors.append(
                f"Column '{config.column}' has {violations['above_max']} values "
                f"above max {config.max}"
            )

        return {"passed": len(errors) == 0, "errors": errors}

    def _assert_unique_key(
        self, config: UniqueKeyAssertion, messages: list[JsonRecord]
    ) -> AssertionResult:
        """Assert that a key is unique within the sample."""
        tolerance = config.tolerance or 0.0
        errors: list[str] = []
        seen: dict[object, int] = {}

        for message in messages:
            if config.key in message:
                value = message[config.key]
                try:
                    seen[value] = seen.get(value, 0) + 1
                except TypeError:
                    errors.append(
                        f"Key '{config.key}' contains a non-scalar value that cannot be "
                        "checked for uniqueness"
                    )

        duplicates = sum(1 for count in seen.values() if count > 1)
        duplicate_rate = duplicates / len(messages) if messages else 0

        if duplicate_rate > tolerance:
            errors.append(
                f"Key '{config.key}' has {duplicate_rate:.2%} duplicate rate "
                f"(tolerance: {tolerance:.2%})"
            )

        return {"passed": len(errors) == 0, "errors": errors}
