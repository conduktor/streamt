---
title: Release Notes
description: Current public release boundaries and operator actions
---

# Release notes

## Unreleased — durable OpenLineage apply telemetry

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

## Unreleased — exact alias-only Gateway adoption

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

## Unreleased — explicit reviewed Gateway rule removal

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

Reviewed-plan format version 4 binds the exact ordered action and secret-neutral
Gateway current/desired aggregate evidence. Versions 1 through 3 must be
regenerated. Direct apply, direct dry-run, offline plan files, and targeted or
selected apply cannot authorize a tombstone. The actual delete requires
`--force` unless environment policy already allows destructive operations.

Provider-free preflight first binds the logical owner, provider rule name,
AliasTopic, backend, prior artifact checksum, and managed ownership. Planning
then observes one complete Gateway aggregate and deletion removes only its exact
owned Interceptors and AliasTopic. The AliasTopic and Interceptor collection
reads are sequential, not provider-atomic; external writers remain a TOCTOU
boundary and any ambiguous or third-state evidence fails closed. Broad
discovery and deletion by manifest absence remain unsupported; Gateway
adoption is a separate explicit alias-only workflow.

## Unreleased — exact Kafka Connect adoption

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

## Unreleased — PostgreSQL v2 deployment state

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
