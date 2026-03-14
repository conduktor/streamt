# TODO

Unimplemented features, planned work, and known gaps.

## Production Readiness

- [ ] Flink savepoint handling — graceful upgrades without data loss
- [ ] HTTP response validation — check status before `.json()` calls in deployers
- [ ] Input validation — Pydantic validators for URLs, topic names, bootstrap servers

## Core Features

- [ ] Advanced test assertions — `unique_key`, `foreign_key`, `distribution`, `max_lag`, `throughput` (require windowing/aggregation). Documented in `docs/concepts/tests.md`
- [ ] Test failure handlers — `on_failure` actions: alert (Slack/PagerDuty), pause model, route to DLQ, block deployment
- [ ] Global credentials/connections — define Snowflake, S3, etc. once and reference everywhere
- [ ] `streamt build` — generate deployable artifacts for debugging, auditing, air-gapped deployments
- [ ] `streamt diff` — show diff between local and deployed state
- [ ] `streamt status --health` — health checks with thresholds

## Flink Options (Parsed But Not Applied)

- [ ] `state_backend` — parsed from YAML but not applied to Flink jobs (`docs/reference/flink-options.md:113`)

## Flink Options (Not Yet Implemented)

Documented in `docs/reference/flink-options.md`:

- [ ] Advanced checkpointing — `timeout_ms`, `min_pause_ms`, `max_concurrent`, `mode`, `externalized`, `unaligned`, `incremental`
- [ ] State backend advanced — `state.ttl_ms`, `state.rocksdb.*`, `state.incremental_cleanup.*`
- [ ] Restart strategy — `fixed_delay`, `failure_rate`, `exponential_delay`
- [ ] Resource configuration — task manager memory/CPU/slots, job manager memory/CPU
- [ ] Savepoint management — `savepoint.enabled`, `savepoint.path`, `savepoint.on_upgrade`, `savepoint.on_cancel`
- [ ] Custom watermark expression — `event_time.watermark.expression`
- [ ] Kubernetes Flink operator deployment

## Governance Rules

Documented in `docs/reference/governance.md`:

- [ ] `max_replication_factor` (line 44, commented out)
- [ ] `forbidden_suffixes` (line 53, commented out)

## Data Governance (Future)

- [ ] Security policies — field-level encryption, `allowed_roles`, purpose-based access control, per-consumer column masking
- [ ] Exposure SLOs — `max_end_to_end_latency_ms`, `max_error_rate`, `freshness` contracts
- [ ] Exposure access control — `allowed_roles`, `purpose` metadata on exposures
- [ ] Data residency — region constraints (`region: EU`, `allowed_clusters`, `forbidden_sinks`)
- [ ] Schema versioning — v1/v2 model versions with compatibility checks and migration paths

## Operational

- [ ] Prometheus/OpenTelemetry integration — metrics and alerting
- [ ] Kubernetes Flink operator support — native K8s deployment
- [ ] CI/CD GitHub Actions templates — automation for deploy pipelines
- [ ] Curated connector library — tested configs for Postgres, Snowflake, S3

## Vision

- [ ] Semantic layer / Streaming API — declarative consumption API ("give me flux X with max 30s delay")
- [ ] Model templates / packages — reusable pipeline patterns (CDC-to-enriched, sessionization)
- [ ] External app support — register blackbox applications (Java, Go) with input/output models for lineage
- [ ] High-level intent mode — "I want X" and streamt builds the entire pipeline
- [ ] KStreams runtime — `materialized: kstreams` for users without Flink
- [ ] RisingWave runtime — streaming SQL database alternative to Flink
- [ ] Materialize runtime — incremental view maintenance

## Deferred

- [ ] VS Code extension
- [ ] Additional streaming substrates (Pulsar, Kinesis)
- [ ] Cloud/SaaS version

## Known Gaps in Docs

- [ ] `docs/concepts/streaming-fundamentals.md:72` — "Coming Soon" section for `event_time:` config in sources
- [ ] `docs/reference/yaml-schema.md:700` — "PLANNED" block for advanced Flink options
- [ ] `docs/reference/cli.md:890` — "Coming Soon" section referencing deleted roadmap link
