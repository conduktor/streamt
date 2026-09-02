# Integration support

This page distinguishes working integration paths from configuration that is
only planned. "Direct apply" means streamt currently owns the API calls. Local
ownership state is persisted for development use. PostgreSQL schema version 2
is supported for ordinary plan/apply/adopt and explicit reviewed recovery when
all release boundaries below are met. Production-safe Flink stateful upgrades
are not enabled.

| System | Compile | Observe or discover | Direct plan/apply | Current boundary |
| --- | --- | --- | --- | --- |
| Apache Kafka / compatible brokers | Yes | Topics, consumer groups, lag, no-clobber source import | Topics | Topic changes include canonical downstream graph and consumer evidence. Local ownership state and single-topic adoption are supported; deletion by absence and shared-CI locking are disabled. |
| Confluent Schema Registry / compatible API | Yes | Subjects, versions, references, compatibility, single-subject adoption | Register source schemas | Registry-incompatible updates are classified schema-breaking and blocked. Compatible updates remain risky until downstream column/contract impact exists. Adoption is state-only and fail-closed; subject deletion is disabled. |
| Apache Flink REST + SQL Gateway | Yes | Job status and metrics | Submit new jobs; plan existing updates | Existing updates and resubmissions are classified state-migration-requiring and blocked until a savepoint-safe or explicitly stateless workflow exists. Adoption is not supported: current status cannot prove SQL or managed execution settings. |
| Kafka Connect REST | Yes | Strict connector managed content and single-connector adoption | Sink connectors | Adoption is state-only: an omitted or explicitly matching default cluster is accepted; a non-default cluster fails closed. The claim binds its alias, versioned normalized-endpoint fingerprint, and exact connector name. Two encoded resource reads bracket confirmation for a new claim; an identical claim returns after one read with no confirmation or write. Output exposes only whole-config checksums and sanitized changed-key categories/directions. Legacy `backend: kafka-connect` state fails closed. Connector profiles remain deliberately generic. |
| Conduktor Gateway | Yes | Strict backend/vCluster-scoped AliasTopic and Interceptor aggregate | Virtual-topic interceptor rules and explicit reviewed rule removal | Removal requires an exact lifecycle tombstone, matching managed ownership, one complete live aggregate, reviewed-plan version 4 action evidence, and destructive authorization. Removing a model or omitting a rule never requests deletion; broad discovery and deletion by absence are unsupported. The AliasTopic and Interceptor list reads are sequential rather than provider-atomic, so an external writer remains a TOCTOU boundary and ambiguous or third-state evidence fails closed. Gateway adoption remains unsupported; its planned initial slice is limited to exact alias-only rules with no interceptors. Console catalog publication is separate. |
| Local deployment state | No | Status and explicit reviewed recovery | Ordinary local plan/apply/adopt | Single-host file locking, durable intent/progress, and crash-safe recovery history are supported. It provides no cross-host exclusion, shared-runner fencing, or HA durability. |
| PostgreSQL deployment state | No | Bounded v1/v2 status, confirmed v1 initialization, non-reserving lock diagnostics, confirmed v1-to-v2 migration, and reviewed recovery | Ordinary plan/apply/adopt and recovery through the exact v2 writer | Requires `streamt[postgres]`, `writer_dsn_env`, an exact v2 catalog/ACL, and a direct standalone primary. The owner/admin credential is never a runtime fallback. Version 1 remains administrative only. PostgreSQL 14 and 18 run real-server, command, ACL, mutation, recovery, and process-concurrency gates. All poolers and every HA/failover topology are unsupported. |
| AsyncAPI | AsyncAPI 3.1 export | No | No | Export is validated offline against the pinned official 3.1 JSON Schema plus local-reference semantics. It describes declared Kafka channels and contracts without live-broker or serializer claims. |
| Open Data Contract Standard (ODCS) | ODCS 3.1.0 project-wide schema export | No | No | One parsed project becomes one offline-validated contract containing every declared source and model. Quality, SLA, team, role, server, import, catalog publication, runtime enrichment, and per-model documents are not supported. |
| OpenLineage | OpenLineage 1.53.0 static `DatasetEvent`/`JobEvent` export and opt-in finite `test` `RunEvent` pairs | No | No | Static metadata and command events are validated offline against pinned official schemas plus local invariants. Explicit bounded File/HTTP transports are supported. `apply` and deployed Flink, Gateway, and Connect processes emit no telemetry. |

## Not supported as deployment backends

Docker is supported for local development infrastructure, not as a streamt
deployment backend. Kubernetes, the Flink Kubernetes Operator, Strimzi,
Terraform/OpenTofu, and Confluent Cloud Flink Statements are not accepted as
working targets today.

## Integration priorities

The next integrations are ordered by how much safety or interoperability they
unlock:

1. Continue adoption beyond Kafka topics, Schema Registry subjects, and the
   exact default-cluster Kafka Connect slice in the
   [fail-closed provider order](../plans/2026-09-02-extended-resource-adoption.md):
   normalized Gateway aggregate and alias-only adoption are next. Flink remains
   gated on artifact evidence and lifecycle semantics. Add export/import
   workflows for deployment ownership state separately.
2. Stateful external backends: Terraform/OpenTofu for cloud resources, Strimzi
   output, and Flink Kubernetes Operator resources.
3. Confluent Cloud Flink Statements as an explicit backend rather than a
   REST-shaped configuration claim.
4. OpenLineage durable apply-command telemetry, Conduktor Console metadata
   publication, and portable catalog exports.
5. Prometheus/OpenTelemetry evidence plus Alertmanager or generic webhook
   actions for runtime policy evaluation.

See [product direction](../specs/product-direction.md) and `ROADMAP.md` in the
repository root for sequencing and release gates.

## PostgreSQL release boundary

The supported topology is one direct connection endpoint for one standalone
primary. Do not put PgBouncer, Pgpool-II, a cloud database proxy, or any other
pooler between streamt and PostgreSQL, regardless of pooling mode. streamt can
reject replicas and many identity/catalog errors, but cannot reliably prove
that a DSN bypasses every pooler. Endpoint ownership is therefore an operator
requirement. Promotion, automatic failover, multi-primary, and every other HA
topology are outside the supported boundary, including synchronously replicated
clusters.

Before using PostgreSQL for shared work, activate and test schema-and-data
backups, restore-based rollback, the reviewed recovery runbook, and monitoring
for blocked operations and state-backend failures. See the
[PostgreSQL deployment-state guide](../guides/postgres-deployment-state.md) and
[release notes](release-notes.md).
