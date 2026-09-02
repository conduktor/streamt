---
title: Release Notes
description: Current public release boundaries and operator actions
---

# Release notes

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
