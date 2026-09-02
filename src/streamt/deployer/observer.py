"""Runtime observer — maps manifest artifacts to live metrics from Kafka and Flink."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal, Optional

from streamt.core.redaction import redact_sensitive_text

if TYPE_CHECKING:
    from streamt.compiler.manifest import Manifest
    from streamt.deployer.flink import FlinkDeployer
    from streamt.deployer.kafka import ConsumerGroupLag, KafkaDeployer

logger = logging.getLogger(__name__)


@dataclass
class ConsumerStatus:
    group_id: str
    lag: int
    state: Optional[str] = None  # "Stable", "Empty", "Dead" (if available)


@dataclass(frozen=True)
class ConsumerObservationFailure:
    scope: str
    code: str
    message: str


@dataclass(frozen=True)
class ConsumerObservationEvidence:
    """Completeness of the consumer-group evidence for one topic."""

    status: Literal["verified", "partial", "unavailable"]
    source: str = "kafka_consumer_groups"
    reason: Optional[str] = None
    failures: list[ConsumerObservationFailure] = field(default_factory=list)


@dataclass
class FlinkMetrics:
    job_id: str
    state: str  # RUNNING, FAILED, RESTARTING, CANCELED, FINISHED
    records_in_per_second: Optional[float] = None
    is_backpressured: Optional[bool] = None


@dataclass
class ModelObservation:
    model_name: str
    topic: Optional[str]
    consumers: list[ConsumerStatus] = field(default_factory=list)
    consumer_evidence: Optional[ConsumerObservationEvidence] = None
    flink: Optional[FlinkMetrics] = None

    @property
    def total_lag(self) -> int:
        return sum(c.lag for c in self.consumers)

    @property
    def health(self) -> str:
        """ok | warning | degraded | unknown"""
        if self.flink and self.flink.state not in ("RUNNING", None):
            return "degraded"
        if self.flink and self.flink.is_backpressured:
            return "warning"
        if (
            self.topic is not None
            and self.consumer_evidence is not None
            and self.consumer_evidence.status != "verified"
        ):
            return "unknown"
        if self.consumers and self.total_lag > 100_000:
            return "warning"
        if self.flink is None and self.topic is None:
            return "unknown"
        return "ok"


class Observer:
    """Fetches live runtime signals for all models declared in a manifest."""

    def __init__(
        self,
        manifest: Manifest,
        kafka_deployer: Optional[KafkaDeployer],
        flink_deployer: Optional[FlinkDeployer],
    ) -> None:
        self._manifest = manifest
        self._kafka = kafka_deployer
        self._flink = flink_deployer

    def observe(self) -> list[ModelObservation]:
        """Return one ModelObservation per Flink job or topic artifact in the manifest."""
        observations: list[ModelObservation] = []

        topic_by_model, flink_jobs_by_model = self._resource_indexes()

        all_models = set(topic_by_model) | set(flink_jobs_by_model)

        # Pre-fetch consumer groups once (expensive to enumerate per-topic otherwise)
        all_groups, listing_evidence = self._consumer_groups()

        for model_name in sorted(all_models):
            topic = topic_by_model.get(model_name)
            flink_metrics = None

            consumers: list[ConsumerStatus] = []
            consumer_evidence = None
            if topic:
                consumers, consumer_evidence = self._fetch_consumers(
                    topic, all_groups, listing_evidence
                )
            if model_name in flink_jobs_by_model:
                flink_metrics = self._fetch_flink_metrics(flink_jobs_by_model[model_name])

            observations.append(
                ModelObservation(
                    model_name=model_name,
                    topic=topic,
                    consumers=consumers,
                    consumer_evidence=consumer_evidence,
                    flink=flink_metrics,
                )
            )

        return observations

    def _resource_indexes(self) -> tuple[dict[str, str], dict[str, str]]:
        """Map manifest model names to their compiled topic and Flink job names."""
        topic_artifacts = {
            str(t["name"]): t
            for t in self._manifest.artifacts.get("topics", [])
            if isinstance(t, dict) and t.get("name")
        }
        flink_artifacts = {
            str(j["name"]): j
            for j in self._manifest.artifacts.get("flink_jobs", [])
            if isinstance(j, dict) and j.get("name")
        }

        topic_by_model: dict[str, str] = {}
        flink_by_model: dict[str, str] = {}
        models = [m for m in self._manifest.models if isinstance(m, dict) and m.get("name")]

        for model in models:
            model_name = str(model["name"])
            topic_config = model.get("topic")
            configured_topic = (
                topic_config.get("name") if isinstance(topic_config, dict) else None
            )
            topic_name = str(configured_topic or model_name)
            if topic_name in topic_artifacts:
                topic_by_model[model_name] = topic_name

            for job_name in (model_name, f"{model_name}_processor"):
                if job_name in flink_artifacts:
                    flink_by_model[model_name] = job_name
                    break

        # Ownership metadata provides a fallback for manifests whose artifact
        # names do not follow the legacy naming convention. setdefault keeps a
        # model's primary topic/job from being replaced by a DLQ or test job.
        for topic_name, artifact in topic_artifacts.items():
            ownership = artifact.get("ownership")
            owner = (
                ownership.get("name")
                if isinstance(ownership, dict) and ownership.get("type") == "model"
                else artifact.get("model") or artifact.get("owner_model")
            )
            if owner:
                topic_by_model.setdefault(str(owner), topic_name)
        for job_name, artifact in flink_artifacts.items():
            ownership = artifact.get("ownership")
            owner = (
                ownership.get("name")
                if isinstance(ownership, dict) and ownership.get("type") == "model"
                else artifact.get("model") or artifact.get("owner_model")
            )
            if owner:
                flink_by_model.setdefault(str(owner), job_name)

        return topic_by_model, flink_by_model

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _consumer_groups(
        self,
    ) -> tuple[list[str], Optional[ConsumerObservationEvidence]]:
        """List group identities and preserve failures as explicit evidence."""
        if not self._kafka:
            return [], ConsumerObservationEvidence(
                status="unavailable",
                reason="kafka_not_configured",
            )

        try:
            raw_groups = self._kafka.get_consumer_groups()
        except Exception as error:
            safe_error = redact_sensitive_text(error)
            logger.debug("Could not list consumer groups: %s", safe_error)
            return [], ConsumerObservationEvidence(
                status="unavailable",
                reason="consumer_group_listing_failed",
                failures=[
                    ConsumerObservationFailure(
                        scope="consumer_group_list",
                        code="consumer_group_listing_failed",
                        message=safe_error,
                    )
                ],
            )

        if not isinstance(raw_groups, list):
            return [], ConsumerObservationEvidence(
                status="unavailable",
                reason="invalid_consumer_group_response",
                failures=[
                    ConsumerObservationFailure(
                        scope="consumer_group_list",
                        code="invalid_consumer_group_response",
                        message="Kafka returned a non-list consumer group response.",
                    )
                ],
            )

        groups: set[str] = set()
        failures: list[ConsumerObservationFailure] = []
        for group_id in raw_groups:
            if isinstance(group_id, str) and group_id:
                groups.add(group_id)
            else:
                failures.append(
                    ConsumerObservationFailure(
                        scope="consumer_group_list",
                        code="invalid_consumer_group_identity",
                        message="Kafka returned a non-string consumer group identity.",
                    )
                )

        if failures:
            return sorted(groups), ConsumerObservationEvidence(
                status="partial",
                reason="invalid_consumer_group_identity",
                failures=failures,
            )
        return sorted(groups), None

    def _fetch_consumers(
        self,
        topic: str,
        all_groups: list[str],
        listing_evidence: Optional[ConsumerObservationEvidence],
    ) -> tuple[list[ConsumerStatus], ConsumerObservationEvidence]:
        """Return consumers and explicit evidence completeness for a topic."""
        if not self._kafka:
            return [], listing_evidence or ConsumerObservationEvidence(
                status="unavailable",
                reason="kafka_not_configured",
            )
        if listing_evidence and listing_evidence.status == "unavailable":
            return [], listing_evidence

        result: list[ConsumerStatus] = []
        failures = list(listing_evidence.failures) if listing_evidence else []
        lag_query_failed = False
        for group_id in all_groups:
            try:
                lag_info: Optional[ConsumerGroupLag] = self._kafka.get_consumer_group_lag(
                    group_id, topic
                )
                if lag_info is not None:
                    result.append(
                        ConsumerStatus(
                            group_id=group_id,
                            lag=lag_info.total_lag,
                            state=lag_info.state,
                        )
                    )
            except Exception as error:
                lag_query_failed = True
                safe_group = redact_sensitive_text(group_id)
                safe_error = redact_sensitive_text(error)
                logger.debug(
                    "Could not query group %s for topic %s: %s",
                    safe_group,
                    redact_sensitive_text(topic),
                    safe_error,
                )
                failures.append(
                    ConsumerObservationFailure(
                        scope=f"consumer_group/{safe_group}",
                        code="consumer_group_lag_failed",
                        message=safe_error,
                    )
                )

        if failures:
            return result, ConsumerObservationEvidence(
                status="partial",
                reason=(
                    "consumer_group_lag_failed"
                    if lag_query_failed
                    else listing_evidence.reason if listing_evidence else None
                ),
                failures=failures,
            )
        return result, ConsumerObservationEvidence(status="verified")

    def _fetch_flink_metrics(self, job_name: str) -> Optional[FlinkMetrics]:
        """Fetch job state and optional throughput metrics from the Flink REST API."""
        if not self._flink:
            return None

        try:
            job_state = self._flink.get_job_state(job_name)
            if not job_state.exists:
                return None

            metrics = FlinkMetrics(
                job_id=job_state.job_id or "", state=job_state.status or "UNKNOWN"
            )

            # Fetch throughput + backpressure from Flink Metrics API
            if job_state.job_id:
                try:
                    raw = self._flink._request(
                        "GET",
                        f"/jobs/{job_state.job_id}/metrics",
                        params={"get": "numRecordsInPerSecond,isBackPressured"},
                    )
                    if isinstance(raw, list):
                        for entry in raw:
                            mid = entry.get("id", "")
                            val = entry.get("value")
                            if mid == "numRecordsInPerSecond" and val is not None:
                                try:
                                    metrics.records_in_per_second = float(val)
                                except (TypeError, ValueError):
                                    pass
                            elif mid == "isBackPressured" and val is not None:
                                metrics.is_backpressured = str(val).lower() in ("true", "1")
                except Exception as e:
                    logger.debug("Could not fetch Flink metrics for %s: %s", job_name, e)

            return metrics

        except Exception as e:
            logger.debug("Could not fetch Flink job state for %s: %s", job_name, e)
            return None
