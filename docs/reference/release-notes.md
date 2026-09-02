---
title: Release Notes
description: Current public release boundaries and operator actions
---

# Release notes

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
locking, compare-and-swap, remote-state policy, and idempotency behavior.
Gateway adoption remains planned, and Flink adoption remains deferred. This
boundary is covered by source tests, an isolated-wheel command smoke test, a
real Connect observer test, and the existing PostgreSQL 14/18 release gates.

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
