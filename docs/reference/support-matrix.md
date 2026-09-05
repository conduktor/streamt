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
| Kafka Streams fixed Docker runner | Closed typed projection/filter SQL subset | Exact owned container, image, topic identity, status and progress checks | Create managed jobs; no-op repeat apply | Explicit `executor: kafka_streams`, immutable locally built image, local Unix-socket Docker daemon and bridge network only. One raw-JSON input/output; no joins, aggregation, windows or arbitrary JAR/image scheduling. Existing job replacement, deletion and pending-operation recovery remain blocked. No implicit image pull, offset reset or external resource adoption. |
| Kafka Connect REST | Yes | Strict connector managed content and single-connector adoption | Sink connectors and explicit reviewed removal of one exact managed Connector | Adoption is state-only: an omitted or explicitly matching default cluster is accepted; a non-default cluster fails closed. Removal instead requires an exact `lifecycle.connector_removals` tombstone naming the default alias, PostgreSQL-v2 managed ownership, and a fresh online plan-format-5 reviewed apply. Direct, dry-run, offline, targeted, selected, local-state, adopted, legacy, ambiguous, or checksum-mismatched removal fails closed. Model/manifest absence is inert. Mutation uses one exact non-retrying DELETE plus bounded absence proof; uncertainty enters reviewed control-v3 recovery without retrying DELETE. The PostgreSQL lock excludes streamt writers only, not external Connect writers. No topic, records, offsets, schema, external-system, credential, or plugin deletion is claimed. Connector profiles remain deliberately generic. |
| Conduktor Gateway | Yes | Strict backend/vCluster-scoped AliasTopic and Interceptor aggregate; single-rule alias-only adoption | Virtual-topic interceptor rules and explicit reviewed rule removal | Alias-only adoption is state-only and requires one exact adopted artifact, a present canonical `main` AliasTopic, and zero desired or selected owned Interceptors. Two complete observations bracket confirmation; an identical claim uses one. Full interceptor adoption remains unsupported. Removal requires an exact lifecycle tombstone, matching managed ownership, reviewed-plan version 5 action evidence, and destructive authorization. Removing a model or omitting a rule never requests deletion; broad discovery and deletion by absence are unsupported. The two list reads are sequential rather than provider-atomic, so an external writer remains a TOCTOU boundary and ambiguous or third-state evidence fails closed. Console catalog publication is separate. |
| Local deployment state | No | Status and explicit reviewed recovery | Ordinary local plan/apply/adopt | Single-host file locking, durable intent/progress, and crash-safe recovery history are supported. It provides no cross-host exclusion, shared-runner fencing, or HA durability. |
| PostgreSQL deployment state | No | Bounded v1/v2 status, confirmed v1 initialization, non-reserving lock diagnostics, confirmed v1-to-v2 migration, and reviewed recovery | Ordinary plan/apply/adopt, exact reviewed Connector removal, and recovery through the exact v2 writer | Requires `streamt[postgres]`, `writer_dsn_env`, an exact v2 catalog/ACL, and a direct standalone primary. The owner/admin credential is never a runtime fallback. Version 1 remains administrative only. PostgreSQL 14 and 18 run real-server, command, ACL, mutation, recovery, installed-wheel, real-Connect, and independent-process contention gates. All poolers and every HA/failover topology are unsupported. |
| AsyncAPI | AsyncAPI 3.1 export | No | No | Export is validated offline against the pinned official 3.1 JSON Schema plus local-reference semantics. It describes declared Kafka channels and contracts without live-broker or serializer claims. |
| Open Data Contract Standard (ODCS) | ODCS 3.1.0 project-wide schema export | No | No | One parsed project becomes one offline-validated contract containing every declared source and model. Quality, SLA, team, role, server, import, catalog publication, runtime enrichment, and per-model documents are not supported. |
| Strimzi KafkaTopic GitOps | Deterministic Strimzi 1.2.0 `KafkaTopic` YAML for managed compiled topics | No | No | `streamt export strimzi` is an offline artifact export validated against the pinned CRD and a real disposable reconciliation/replay gate. It omits external topics with a warning and rejects adopted topics. streamt does not contact Kubernetes, apply resources, install or operate Strimzi, manage credentials, prune, delete, or persist Kubernetes ownership state. |
| Backstage Software Catalog | Backstage v1.54.2 core `System`, `Resource`, and `Component` export | No | No | Deterministic multi-document YAML is validated offline from one dry-run compile. Exact direct dependencies, explicit owners, cluster refs, and contract state are supported. The wheel/sdist schema gate, isolated-wheel smoke, and `@backstage/catalog-model@1.10.0` parity gate cover the packaged path. Backstage API publication, live catalog synchronization, Conduktor Console publication, and deployed-runtime claims are unsupported. DataHub metadata-file export is a separate command and identity contract. |
| DataHub | DataHub v1.7.0 simplified-MCP metadata-file export | No | No | Deterministic canonical JSON is generated from one dry-run compile and validated without a runtime DataHub dependency. It maps one DataFlow, native Dataset URNs, actual DataJobs, and direct Dataset lineage; contract state is only a custom property. Python 3.10-3.12 installed-wheel/SDK gates validate the offline boundary. A separate exact v1.7.0 quickstart gate ingests both Kafka identity variants twice, reads back all five emitted aspect types, and verifies five direct Dataset relationships per variant. Production streamt has no GMS call, publisher, live synchronization, state, or deletion. |
| OpenLineage | OpenLineage 1.53.0 static `DatasetEvent`/`JobEvent` export and opt-in finite `test` and durable `apply` `RunEvent` pairs | No | Opt-in apply-command telemetry | Static metadata and command events are validated offline against pinned official schemas plus local invariants. An apply run reuses its durable operation UUID and START timestamp, then reports COMPLETE only after ownership state commits and the operation marker clears. Explicit bounded File/HTTP transports are best effort; delivery cannot change deployment or recovery truth. Events describe the finite streamt command only—deployed Flink, Gateway, Connect, and Kafka resource lifecycles remain unobserved. |

## Not supported as deployment backends

Docker is supported only for the fixed local Kafka Streams runner described
above, not as a general application scheduler. Kubernetes and Strimzi direct operations, the Flink
Kubernetes Operator, Terraform/OpenTofu, and Confluent Cloud Flink Statements
are not accepted as working deployment targets today. The supported Strimzi
boundary is the offline `KafkaTopic` artifact described above, not deployment.

## Integration priorities

The current cycle focuses on coherent topology development and updates. The
[topology/runtime plan](../plans/2026-09-05-topology-runtime-execution.md)
sets the order:

1. Improve selected import and the external/managed application model.
2. Prove installed creation for users with Kafka but no Flink through the bounded
   Kafka Streams runner, with the same ownership and validation workflow.
3. Verify a minimal source-to-output example and a supported update lifecycle.
4. Reuse that workflow in Git/CI, then add a sink if the example needs it.

Strimzi expansion, new cloud backends, and additional catalog publication are
not scheduled. Existing tested integrations remain supported within the table's
limits. Custom applications can be documented using exposure inputs and outputs;
broader managed application deployment is not implied. External declarations are
not live-diffed by planning. `status --include-external` opts into their observation;
managed safety reads and deployment-state access remain enabled. See the
[ownership contract](../specs/deployment-safety-and-ownership.md#external-declaration-behavior).

The historical [Kafka Streams proof](../specs/kafka-streams-execution-proof.md)
remains separate from the maintained runner. Standalone Java clean-restart tests
do not establish that product replacement or interrupted-operation recovery is
safe. Those transitions remain blocked until their own acceptance gates pass.
The maintained runner has real local plaintext Kafka acceptance; authenticated
TLS/SASL brokers, transactional crash recovery and multipartition lifecycle
acceptance are not yet verified. Local credential/TLS validation is not proof of
an authenticated deployment. See the [starting guide](../getting-started/kafka-streams.md).

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
