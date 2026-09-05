---
title: Release Notes
description: Current public release boundaries and operator actions
---

# Release notes

## 0.1.0a1 release candidate

This first public alpha is not published yet. The entries below describe the
candidate currently being prepared and remain subject to the exact-SHA release
gate in the
[first public alpha release plan](../plans/2026-09-03-first-alpha-release.md).

### Developer workflow and external declarations

The scaffold now passes `validate --strict`, uses explicit column projections,
and explains its Flink runtime prerequisite and the external input topic. Its
offline commands do not prove that a processor is running.

Custom application exposures now connect declared source/model inputs and
outputs in every relationship direction. Invalid references, graph name
collisions, and feedback cycles fail local validation. This does not add code
builds or application scheduling.

Import JSON now reports provenance, schema lookup outcomes, inferred-column
limits, and unavailable application SQL/code. Repeat import and no-clobber
behavior are unchanged. Ordinary planning keeps external artifacts as unobserved
no-ops. Status defaults to managed observations; use `--include-external` to check
external dependencies explicitly. Missing managed runtime configuration is an
error, and ownership, collision, removal, and recovery checks remain required.

### Bounded Kafka Streams creation

`executor: kafka_streams` now compiles the supported raw-JSON projection/filter
subset into a versioned runner plan. `streamt runtime build` builds the packaged
Java sources on the selected local Docker daemon and returns an immutable image
ID. `init --executor kafka_streams --runner-image ...` creates a managed input,
a transformation, and a metadata-only custom consumer declaration. See the
[starting guide](../getting-started/kafka-streams.md).

Managed creation, status, reviewed plans, selection, ownership state, and no-op
repeat apply share the existing workflow. Application identity remains stable
across SQL changes. Existing jobs cannot yet be replaced or recovered through
this backend; unsupported transitions remain blocked. A failed creation retains
its durable pending operation and does not reset offsets or delete its topics.
This is a local development backend, not a production lifecycle guarantee.

Declared custom-consumer columns and known type families now fail local
validation when incompatible. Kafka Streams inferred output types participate
in those checks, and physical topic cycles cannot hide behind source aliases.
Backstage, DataHub, and OpenLineage static exports retain the new process kind
and its dependencies without exposing runtime configuration.

Topic planning now distinguishes explicit topic overrides from inherited broker
settings. Removing an explicit override sends Kafka's DELETE operation instead
of setting the literal text `None`. Repeated unchanged plans converge. The
minimum `confluent-kafka` dependency is now 2.13.2, matching the exercised
topic-identity and consumer-offset Admin APIs.

### Offline Strimzi 1.2.0 KafkaTopic export

`streamt export strimzi` now emits deterministic, offline-validated
`kafka.strimzi.io/v1` `KafkaTopic` YAML for managed compiled topic artifacts.
Callers provide the exact namespace and Strimzi Kafka cluster label. Valid
Kafka names that are Kubernetes DNS-1123 labels remain direct; other valid
names receive a deterministic full-SHA-256 resource identity while preserving
the exact Kafka name in `spec.topicName`. Topic configuration values are
normalized to the pinned Strimzi contract, and optional file output uses an
atomic replacement.

External topics are deliberately omitted with
`W120_STRIMZI_EXTERNAL_TOPIC_OMITTED`; adopted topics fail closed because an
offline artifact cannot prove safe Topic Operator ownership. Other artifact
kinds produce the bounded `W121_STRIMZI_ARTIFACTS_OMITTED` warning. Mapping,
validation, serialization, and output failures use the secret-neutral
`E509_STRIMZI_INVALID` boundary.

Source, wheel, and direct-sdist exports are byte-identical on Python 3.10
through 3.14. A digest-pinned test-only lane installs the built wheel and proves
server-side admission, reconciliation, exact Kubernetes and Kafka read-back,
and idempotent replay against disposable Strimzi 1.2.0 and Kafka 4.3.1. Decisive
push [run 33909664040, job 101143523418](https://github.com/conduktor/streamt/actions/runs/33909664040/job/101143523418)
passed that normal-mode gate after runtime-image discovery in
[pilot run 33908141332](https://github.com/conduktor/streamt/actions/runs/33908141332).

The production command still ends at the offline YAML bytes. It does not
contact Kubernetes, apply or delete resources, install or operate Strimzi,
manage credentials, configure a GitOps controller, detect drift, or persist
Kubernetes ownership state. See the
[Strimzi KafkaTopic export](strimzi-kafkatopic.md) for the complete supported
boundary.

### Offline DataHub v1.7.0 metadata-file export

`streamt docs datahub` now exports deterministic simplified DataHub Metadata
Change Proposals from one offline dry-run compile. One project becomes a
DataFlow, sources and physical topic outputs become Datasets with native
DataHub identities, and actual Flink, Gateway, and Connect processes become
DataJobs with exact direct Dataset lineage. Process-free topics do not invent
jobs, and sink jobs do not invent destination datasets.

Callers supply an exact stable catalog ID and uppercase DataHub FabricType. A
Kafka platform instance is optional and must match the identity used by
DataHub Kafka ingestion. Gateway virtual topics require an explicit DataHub
platform ID and instance; streamt does not invent or bootstrap a Conduktor
Gateway platform. Contract state is represented only by the
`streamt.contract.status` custom property. The export does not copy an ODCS
document or claim a native DataHub DataContract, schema, or assertion.

Text output is canonical UTF-8 JSON; global JSON output uses the ordinary
streamt envelope; and `--output-file` validates and builds all bytes before an
atomic replacement. Invalid input, identity collisions, mapping, validation,
serialization, and file failures use `E508_DATAHUB_INVALID` without a partial
artifact. Sink destinations, exposures, tags, and owners remain deliberately
omitted with `W115_DATAHUB_SINK_OUTPUT_OMITTED` through
`W118_DATAHUB_OWNER_OMITTED`.

Production streamt has no DataHub dependency or optional extra. Python
3.10-3.12 release lanes run the installed wheel without the SDK, compare exact
source and wheel bytes, and then validate both Kafka-instance variants in a
separate environment containing exact `acryl-datahub==1.7.0`. The SDK wrapper
and metadata-file reader run on every lane; the official file-source CLI
dry-run runs on Python 3.11 with telemetry and network access disabled. CI run
`33775922977` passed this complete release gate.

A separate exact-server acceptance gate now installs the same built wheel and
`acryl-datahub==1.7.0`, then uses two fresh pinned five-service quickstart
projects. The Kafka-instance artifact writes 15/15 proposals across two
ingestions; the no-Kafka-instance artifact writes 12/12. Both preserve every
emitted aspect, return the expected five direct Dataset relationships after
each ingestion, and leave no project container, network, or volume behind.
Complete CI run
[`33798567142`](https://github.com/conduktor/streamt/actions/runs/33798567142)
passed this gate with loopback-only GMS publication and bounded evidence.

The production command boundary still ends at the offline metadata file:
streamt does not contact GMS, publish, read, reconcile, delete, or verify
platform or entity existence. Generated files intentionally disclose catalog
identities, physical names, descriptions, contract state, and topology, so
review their destination permissions. See
[DataHub catalog export](datahub-catalog.md) for the exact boundary.

### Offline Backstage Software Catalog export

`streamt docs backstage` now exports deterministic Backstage `System`,
`Resource`, and `Component` core entities from one offline dry-run compile.
Sources and physical model outputs become Resources; Flink and Gateway models
produce Components plus their outputs; sink models produce Components without
invented destination Resources. Relationships use exact direct compiler
dependencies, explicit cluster references, and explicit owner resolution.

Catalog ID, lowercase namespace, default owner reference, and Component
lifecycle are required semantic inputs. Declared streamt owner labels require
an exact strict-JSON owner map. Kafka and Gateway cluster Resource references
are conditionally required by the emitted dataset types. Invalid inputs,
ambiguous identities, missing ownership, or invalid entities fail with
`E507_BACKSTAGE_INVALID`. Sink destinations and exposures remain intentionally
omitted with `W113_BACKSTAGE_SINK_OUTPUT_OMITTED` and
`W114_BACKSTAGE_EXPOSURE_OMITTED`.

Text output is canonical Backstage YAML; global JSON output uses the ordinary
streamt envelope; `--output-file` performs an atomic YAML replacement. The
exporter does not construct state or provider clients and does not perform
network or subprocess access. The package includes the pinned Backstage
v1.54.2 seven-schema closure. Release gates inspect its exact bytes in wheel
and source distributions, exercise a repository-free installed-wheel export,
and independently validate representative YAML with
`@backstage/catalog-model@1.10.0` and release-test-only `yaml@2.8.1`. Node.js is
not a runtime dependency.

This is a static design-metadata export, not evidence of deployment, health, or
runtime lifecycle. It does not publish to Backstage. The supported offline
DataHub metadata-file export is a separate command with a separate identity
contract; neither export enables catalog synchronization. Conduktor Console
metadata publication also remains unsupported and requires its own contract.
Generated files intentionally disclose catalog metadata—including logical and
physical names, owners, clusters, descriptions, tags, and dependencies—so
review their contents and destination permissions before sharing them. See
[Backstage Software Catalog export](backstage-catalog.md) for the supported
boundary.

### Durable OpenLineage apply telemetry

`streamt apply --emit-openlineage` can now emit a validated OpenLineage 1.53.0
START/COMPLETE, START/FAIL, or START/ABORT pair through an explicitly configured
bounded File or HTTP transport. The run ID is the exact durable deployment
operation UUID, and START uses the same timestamp as the persisted operation
intent. Zero-action applies still create a durable operation and emit a normal
START/COMPLETE pair. OpenLineage environment variables alone never enable
emission.

All ordinary parsing, review, confirmation, safety, planning, dry-run, and final
state-drift gates run before OpenLineage preflight and durable begin. START is
attempted after the operation intent is durable and before progress or provider
mutation. COMPLETE is attempted only after ownership state commits and the
operation marker clears; runtime or uncertain-commit failures use FAIL, and an
interruption after START uses ABORT. A verified authority-release failure after
commit retains COMPLETE while the command preserves its existing committed
error result.

Delivery remains deliberately best effort. START, terminal, and transport-close
failures add only `W112_OPENLINEAGE_EMIT_FAILED`; they cannot change provider
mutation, rollback, state, recovery markers, or exit status. Apply events contain
no datasets, actions, reviewed plans, artifacts, provider identities, runtime
configuration, or exception text. They describe the finite streamt control-plane
command, not the lifecycle of a submitted Flink job or any deployed Gateway,
Kafka Connect, or Kafka resource.

The release gate verifies every pinned OpenLineage schema resource by decoded
size and checksum in both the wheel and source distribution. A repository-free
installed-wheel smoke test performs a real durable local apply through the File
transport and proves that START's run ID matches the in-progress operation
record before mutation, followed by COMPLETE and committed ownership state.
Real PostgreSQL 14 and 18 composition gates cover direct and reviewed success,
recovery-required failure, and transport-failure independence for the version-2
state backend.

### Exact alias-only Gateway adoption

`streamt adopt --kind gateway_rule` can now claim one compiled adopted
Conduktor Gateway rule without changing the provider. The initial boundary is
deliberately alias-only: the desired artifact and selected live aggregate must
have zero rule-owned Interceptors, the exact AliasTopic must already exist in
the bound backend and effective vCluster, and its physical cluster must
normalize to canonical `main`.

A new claim reads the AliasTopic and Interceptor collections once for review
and repeats the same ordered pair after exact confirmation. Review output
contains only the logical resource ID, effective vCluster, endpoint
fingerprint, alias, canonical physical cluster, mapping checksums, aggregate
fingerprints, artifact checksum, and pending-change categories. Physical topic
names, the endpoint, credentials, and interceptor configuration are not
emitted. An identical managed or adopted state claim returns after one complete
two-GET observation without confirmation or a state write.

Adoption records a version-1 Gateway action with exact reviewed current and
desired aggregate surfaces, so uncertain local or PostgreSQL v2 state commits
can use the normal reviewed recovery workflow. Source, PostgreSQL 14/18,
isolated-wheel, and Conduktor Gateway 3.15 gates prove state-only behavior and
zero mutation requests. Full adoption of rules with Interceptors remains
unsupported.

### Explicit reviewed Gateway rule removal

Projects can now request removal of one exact managed Gateway rule with a
`lifecycle.gateway_rule_removals` tombstone. The tombstone copies the prior
compiler-level rule name, AliasTopic mapping, and interceptor configuration from
the generated manifest; removing a model or omitting its rule never implies
deletion. See the [YAML reference](yaml-schema.md#explicit-gateway-rule-removals)
for the accepted declaration.

Removal uses a fresh online reviewed-plan workflow:

```bash
streamt plan --env prod --out gateway-removal.plan.json
streamt apply --env prod --plan gateway-removal.plan.json --confirm-env prod --force
```

Reviewed-plan format version 5 binds the exact ordered action and secret-neutral
Gateway current/desired aggregate evidence; its Connector evidence field is
null. Versions 1 through 4 must be regenerated. Direct apply, direct dry-run,
offline plan files, and targeted or selected apply cannot authorize a
tombstone. The actual delete requires `--force` unless environment policy
already allows destructive operations.

Provider-free preflight first binds the logical owner, provider rule name,
AliasTopic, backend, prior artifact checksum, and managed ownership. Planning
then observes one complete Gateway aggregate and deletion removes only its exact
owned Interceptors and AliasTopic. The AliasTopic and Interceptor collection
reads are sequential, not provider-atomic; external writers remain a TOCTOU
boundary and any ambiguous or third-state evidence fails closed. Broad
discovery and deletion by manifest absence remain unsupported; Gateway
adoption is a separate explicit alias-only workflow.

### Exact Kafka Connect adoption

`streamt adopt --kind connector` can now claim one compiled adopted Connector
without changing it. An omitted cluster or an explicit cluster equal to the
configured default is accepted; an explicit non-default cluster fails closed.
The claim binds the effective cluster alias, versioned normalized-endpoint
fingerprint, and exact connector name. A new claim reads only the percent-
encoded connector resource once for review and once after confirmation; an
identical claim returns after the first read without confirmation or a state
write. The command does not call Connect list, status, task, or mutation
endpoints.

Connector review and durable evidence are secret-neutral: they contain
checksums for the whole configuration and sanitized changed-key
categories/directions, never raw configuration or per-value fingerprints.
Reviewed recovery uses the same strict managed-content observation. Legacy
records whose backend is exactly `kafka-connect` remain
unbound and fail closed instead of inheriting authority over the configured
endpoint.

The command inherits the existing local and PostgreSQL v2 state-operation,
locking, compare-and-swap, remote-state policy, and idempotency behavior. Flink
adoption remains deferred. This boundary is covered by source tests, an
isolated-wheel command smoke test, a real Connect observer test, and the
existing PostgreSQL 14/18 release gates.

### Explicit reviewed Kafka Connect Connector removal

Projects can now request deletion of one exact previously managed Connector
with `lifecycle.connector_removals`. Each strict tombstone contains only the
prior logical owner, exact connector name, and explicit Connect cluster alias;
the alias must be the configured default. It contains no connector class,
topics, raw configuration, endpoint, backend identity, credentials, or
ownership payload. Removing a sink model, omitting a Connector artifact,
selecting another model, or observing absence never creates deletion authority.

The supported command path is deliberately narrow:

```bash
streamt plan --env prod --out connector-removal.plan.json
streamt apply --env prod --plan connector-removal.plan.json \
  --confirm-env prod --force
```

Every removal requires PostgreSQL deployment-state schema version 2, its exact
least-privilege writer/catalog/ACL and direct-primary authority, and a complete
fresh online reviewed plan. Direct apply, direct or reviewed dry-run, offline
planning, `--target`, `--select`, local state, PostgreSQL v1, non-default
cluster routing, and legacy or adopted ownership cannot authorize deletion.
`--force` supplies only destructive authorization and cannot bypass another
gate. An exact already-absent tombstone with no prior record is a visible no-op
that makes no provider mutation and does not advance the ownership serial.

Provider-free preflight binds all desired, tombstone, state, backend, and
provider-locator identities before Connect access. An action requires one exact
managed prior record plus a complete present resource GET whose reconstructed
artifact checksum matches that record. Reviewed-plan format 5 binds the ordered
delete to secret-neutral current-present and desired-absent fingerprints.
Operation control and recovery plans use version 3; legacy versions cannot
represent or authorize Connector deletion.

Apply holds the PostgreSQL address advisory lock through fresh re-planning,
durable progress, provider mutation, and atomic state/control/history commit.
The managed mutation rechecks the exact current observation, performs one
direct non-retrying DELETE requiring an empty HTTP 204 response, and polls the
same encoded resource path for bounded exact-absence proof. PostgreSQL excludes
other streamt writers at the state address, but Connect has no conditional
delete transaction, so a manual or other non-streamt writer remains a TOCTOU
boundary.

A changed preimage, DELETE-time 404, redirect, nonempty or non-204 response,
transport failure, or missing absent postcondition becomes
`E428_CONNECTOR_REMOVAL_DRIFT`. streamt stops later actions, retains ownership,
records recovery-required state, and neither retries DELETE nor attempts an
automatic rollback. Reviewed recovery may use the durable action without a
retained tombstone: one exact absent observation proves the completed candidate
for `observed`, while only the exact present prior surface can satisfy
`rolled_back`. Local recovery, a third state, a wrong binding, a competing
claim, or malformed/partial evidence fails closed.

The distribution gate builds both sdist and wheel, then an isolated
installed-wheel executable runs the public plan, destructive refusal, apply,
DELETE-time uncertainty, and tombstone-independent recovery against PostgreSQL
14 and 18 without importing the checkout. A separate pinned Kafka Connect
7.5.0 gate runs the public reviewed workflow with plain and percent-encoded
names against PostgreSQL 14 and 18, proves the unrelated Connector's returned
document stays byte-for-byte equal, and proves Kafka topics and Schema Registry
subjects remain unchanged. Across these gates, sentinel checks cover CLI
output, reviewed/recovery files, PostgreSQL current/control/history rows, and
provider logs for raw config, endpoints, response bodies, DSNs, credentials,
roles, schemas, and checkout paths.

This release adds only Connector-object deletion. It does not delete Kafka
topics or records, consumer offsets, Schema Registry subjects, external
systems, credentials, or connector plugins. Topic and schema deletion and
Flink cancellation remain unsupported separate contracts. See
[YAML reference](yaml-schema.md#explicit-kafka-connect-connector-removals),
[CLI reference](cli.md#remove-one-exact-kafka-connect-connector),
[recovery runbook](../guides/state-recovery.md), and the
[support matrix](support-matrix.md).

### PostgreSQL v2 deployment state

PostgreSQL schema version 2 is now selectable for online `plan`, direct and
reviewed `apply`, `adopt`, `state recovery-plan`, and `state recover`.
Install `streamt[postgres]` and configure `postgres.writer_dsn_env`. Each of
these commands resolves only that credential and reproves the exact stored
writer identity, catalog, ACL, and direct-primary session. The owner/admin
`postgres.dsn_env` is never a fallback. PostgreSQL version 1 remains
administrative only.

Support is intentionally narrow: one direct endpoint to one standalone
primary. Every pooler/proxy and every HA, replication, promotion, or failover
topology is unsupported, including session pooling and synchronous replicas.
streamt cannot reliably detect that an endpoint bypasses a pooler, so operators
must own that guarantee.

Before activation, operators must schedule and test schema/data backups and
restores, document restore-based rollback, rehearse reviewed recovery, and
monitor blocked operations plus state-backend failures. There is no automatic
fallback, in-place v2 downgrade, transparent failover, or automatic recovery.
Do not downgrade streamt or restore/downgrade the catalog while any address has
an `in_progress` or `recovery_required` operation marker; preserve the marker
and complete the reviewed incident workflow first.

Administrative status remains intentionally separate. On an exact v2 catalog,
`state status` reports `ordinary_state_authority:
supported_for_v2_writer`; this is a capability label and does not probe
`writer_dsn_env`. `state lock-status` reports `ordinary_state_authority:
not_verified`, makes only a transient non-reserving probe, and cannot authorize
a later command.

See [PostgreSQL deployment state](../guides/postgres-deployment-state.md),
[deployment-state recovery](../guides/state-recovery.md), and the
[support matrix](support-matrix.md) before enabling the backend.
