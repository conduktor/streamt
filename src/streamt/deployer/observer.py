"""Runtime observer — maps manifest artifacts to live metrics from Kafka and Flink."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Optional

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

        # Build index: model_name → topic_name (from topic artifacts)
        topic_by_model: dict[str, str] = {}
        for t in self._manifest.artifacts.get("topics", []):
            name = t.get("name") if isinstance(t, dict) else getattr(t, "name", None)
            if name:
                topic_by_model[name] = name

        # Build index: model_name → flink job name (from flink_jobs artifacts)
        flink_jobs_by_model: dict[str, str] = {}
        for j in self._manifest.artifacts.get("flink_jobs", []):
            name = j.get("name") if isinstance(j, dict) else getattr(j, "name", None)
            if name:
                flink_jobs_by_model[name] = name

        all_models = set(topic_by_model) | set(flink_jobs_by_model)

        # Pre-fetch consumer groups once (expensive to enumerate per-topic otherwise)
        all_groups: list[str] = []
        if self._kafka:
            try:
                all_groups = self._kafka.get_consumer_groups()
            except Exception as e:
                logger.warning("Could not list consumer groups: %s", e)

        for model_name in sorted(all_models):
            topic = topic_by_model.get(model_name)
            flink_metrics = None

            consumers = self._fetch_consumers(topic, all_groups) if topic else []
            if model_name in flink_jobs_by_model:
                flink_metrics = self._fetch_flink_metrics(model_name)

            observations.append(
                ModelObservation(
                    model_name=model_name,
                    topic=topic,
                    consumers=consumers,
                    flink=flink_metrics,
                )
            )

        return observations

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _fetch_consumers(self, topic: str, all_groups: list[str]) -> list[ConsumerStatus]:
        """Return consumer groups that have committed offsets on the given topic."""
        if not self._kafka or not all_groups:
            return []

        result: list[ConsumerStatus] = []
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
            except Exception as e:
                logger.debug("Skipping group %s for topic %s: %s", group_id, topic, e)

        return result

    def _fetch_flink_metrics(self, model_name: str) -> Optional[FlinkMetrics]:
        """Fetch job state and optional throughput metrics from the Flink REST API."""
        if not self._flink:
            return None

        try:
            job_state = self._flink.get_job_state(f"{model_name}_processor")
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
                    logger.debug("Could not fetch Flink metrics for %s: %s", model_name, e)

            return metrics

        except Exception as e:
            logger.debug("Could not fetch Flink job state for %s: %s", model_name, e)
            return None
