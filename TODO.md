# TODO

Unimplemented features and planned work.

## Production Readiness

- [ ] Flink savepoint handling — graceful upgrades without data loss; trigger savepoints on upgrade, restore from savepoint
- [x] HTTP response validation — Flink polling checks HTTP status before `.json()` calls
- [x] HTTP response validation — all deployers already call raise_for_status()
- [x] Input validation — URL scheme validation on SR, Flink, Connect, Gateway configs

## Core Features

- [ ] Test assertions — `foreign_key`, `max_lag`, `throughput`, `distribution`:
  ```yaml
  # Referential integrity across streams
  - foreign_key:
      column: customer_id
      ref_model: customers
      ref_key: id
      window: "1 HOUR"
      match_rate: 0.99

  # Event time lag monitoring (continuous only)
  - max_lag:
      column: event_timestamp
      max_seconds: 300

  # Message throughput bounds (continuous only)
  - throughput:
      min_per_second: 10
      max_per_second: 1000

  # Value distribution across buckets
  - distribution:
      column: amount
      buckets:
        - { min: 0, max: 100, expected_ratio: 0.4, tolerance: 0.1 }
        - { min: 100, max: 1000, expected_ratio: 0.5, tolerance: 0.1 }
        - { min: 1000, max: 10000, expected_ratio: 0.1, tolerance: 0.05 }
  ```
- [ ] Test failure handlers — `on_failure` actions: alert (Slack/PagerDuty), pause model, route to DLQ, block deployment
- [x] Global credentials/connections — define once in `connections:`, reference via `connection:` in sinks
- [x] `streamt build` — compile + package artifacts with manifest and checksums
- [x] `streamt diff` — standalone diff between local and deployed state
- [x] `streamt status --health` — exit 1 if any resource MISSING or DRIFT

## Flink Options

`state_backend` applied as `SET 'state.backend'`. Checkpointing, restart strategy, custom watermark, changelog mode all implemented.

Not yet implemented:

- [x] State backend advanced — RocksDB tuning (`block_cache_size_mb`, `write_buffer_size_mb`, `predefined_options`)
- [x] Resource configuration — task manager memory/CPU/slots, job manager memory/CPU
- [ ] Savepoint management — `savepoint.enabled`, `savepoint.path`, `savepoint.on_upgrade` (trigger_and_restore), `savepoint.on_cancel`
- [ ] Kubernetes Flink operator — deploy via K8s CRDs instead of REST API; namespace, service account, image, pod template config
- [ ] Docker cluster type — local Docker deployment

## Governance Rules

- [x] `max_replication_factor` — maximum replication factor enforcement
- [x] `forbidden_suffixes` — disallowed topic name suffixes (complement to existing `forbidden_prefixes`)

## Data Governance

- [ ] Security policies — field-level encryption, `allowed_roles`, purpose-based access control, per-consumer column masking
- [x] Exposure SLOs — `max_end_to_end_latency_ms`, `max_error_rate`, `freshness_minutes` with validation
- [x] Exposure access control — `allowed_roles`, `purpose` metadata on exposures
- [x] Data residency — `region` on models/sources, `allowed_regions` governance rule
- [x] Schema versioning — `version` on models, duplicate/gap detection in validator

## Operational

- [ ] Prometheus/OpenTelemetry integration — metrics and alerting
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
