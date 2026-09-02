# Integration support

This page distinguishes working integration paths from configuration that is
only planned. "Direct apply" means streamt currently owns the API calls. Local
ownership state is persisted for development use. Explicit reviewed recovery
is available for local state and through a recovery-only PostgreSQL v2 writer,
but ordinary remote plan/apply/adopt and production-safe stateful upgrades are
not enabled.

| System | Compile | Observe or discover | Direct plan/apply | Current boundary |
| --- | --- | --- | --- | --- |
| Apache Kafka / compatible brokers | Yes | Topics, consumer groups, lag, no-clobber source import | Topics | Topic changes include canonical downstream graph and consumer evidence. Local ownership state and single-topic adoption are supported; deletion by absence and shared-CI locking are disabled. |
| Confluent Schema Registry / compatible API | Yes | Subjects, versions, references, compatibility, single-subject adoption | Register source schemas | Registry-incompatible updates are classified schema-breaking and blocked. Compatible updates remain risky until downstream column/contract impact exists. Adoption is state-only and fail-closed; subject deletion is disabled. |
| Apache Flink REST + SQL Gateway | Yes | Job status and metrics | Submit new jobs; plan existing updates | Existing updates and resubmissions are classified state-migration-requiring and blocked until a savepoint-safe or explicitly stateless workflow exists. |
| Kafka Connect REST | Yes | Connector state | Sink connectors | Connector profiles remain deliberately generic. |
| Conduktor Gateway | Yes | Rule state | Virtual-topic interceptor rules | Console catalog publication is a separate planned integration. |
| Local deployment state | No | Status and explicit reviewed recovery | Ordinary local plan/apply/adopt | Single-host file locking, durable intent/progress, and crash-safe recovery history are supported. It provides no cross-host exclusion, shared-runner fencing, or HA durability. |
| PostgreSQL deployment state | No | Bounded v1/v2 status, confirmed v1 initialization, non-reserving lock diagnostics, explicit confirmed v1-to-v2 migration, and reviewed recovery through the exact v2 writer | Recovery only | Requires the optional `postgres` package extra. `state recovery-plan` and `state recover` expose minimum explicit recovery without enabling ordinary authority. PostgreSQL 14 and 18 run real-server, ACL, mutation-protocol, recovery, and process-concurrency gates. Ordinary plan/apply/adopt selection remains disabled pending the final topology/HA and release gates. |
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

1. Complete ordinary PostgreSQL ownership-state enablement for supported
   topologies, then extend adoption beyond Kafka topics and Schema Registry
   subjects.
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
