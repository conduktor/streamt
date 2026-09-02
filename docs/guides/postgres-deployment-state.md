# PostgreSQL deployment state

This runbook upgrades an exact streamt PostgreSQL deployment-state store from
schema version 1 to version 2 and activates the supported ordinary command
boundary. Version 2 binds an externally created, least-privilege writer role.
Online `plan`, `apply`, `adopt`, and reviewed recovery can use that exact role
on a direct standalone primary. Version 1 remains administrative only.

## Before you begin

Install the optional driver and use a direct endpoint for one standalone
primary:

```bash
pip install 'streamt[postgres]'
```

Do not use any pooler or proxy, including a session pooler. The migration and
ordinary operations hold session advisory locks, and streamt cannot reliably
detect that an endpoint bypasses every pooler. Do not use a cluster endpoint,
replica, promoted standby, automatic failover, multi-primary, or any other HA
topology.

Use separate administrative and runtime identities:

- `postgres.dsn_env` resolves to the existing schema-owner DSN for this
  administrative migration.
- `postgres.writer_role_env` resolves to the PostgreSQL role name to bind. It
  is a role identifier, not a DSN or password.
- `postgres.writer_dsn_env` resolves to a DSN whose login is that exact bound
  role. It is used by online plan/apply/adopt and recovery, but not migration.

The writer must already exist. streamt never creates, alters, drops, infers, or
silently rebinds it. A representative DBA command is:

```sql
CREATE ROLE streamt_state_writer
  LOGIN
  NOSUPERUSER
  NOCREATEDB
  NOCREATEROLE
  NOREPLICATION
  NOBYPASSRLS
  NOINHERIT;
```

The role must be distinct from the schema owner, own no state object, and have
no membership edge in either direction. Do not pre-grant state-schema access:
the source must still match the exact v1 catalog. The migration transaction
revokes the target role's state-schema ACL and installs the exact v2 ACL.

## Activate backup, rollback, recovery, and monitoring

Take a schema-and-data backup with the owner credential, retain the server
version and topology information, and test restoration to a separate database.
For example:

```bash
pg_dump "$STREAMT_STATE_POSTGRES_DSN" \
  --schema=streamt \
  --format=custom \
  --file=streamt-state-before-v2.dump
```

Back up the writer's role definition through the normal cluster-level DBA
process as well. `pg_dump` does not include cluster roles. Version 2 stores the
portable role name, never its cluster-local OID.

Before the first ordinary command, make all four operational controls active:

1. Schedule complete schema-and-data backups and alert on backup age/failure.
2. Test restore into a separate database and document restore-based rollback;
   streamt has no in-place v2 downgrade or automatic database rollback.
3. Rehearse the [reviewed recovery workflow](state-recovery.md) and its
   independent-review channel.
4. Monitor `state status`, database availability/durability, and command errors,
   with immediate alerts for `E419`, `E423`, `E425`, and `E426`.

Do not treat `state status` label `supported_for_v2_writer` or a successful
`state lock-status` call as writer-credential health. Status does not resolve
`writer_dsn_env`; the lock probe reports `not_verified` and reserves nothing.

Then inspect the source and capture its immutable store ID:

```bash
streamt -o json state status -p . -e prod
streamt state lock-status -p . -e prod
```

The source must be an exact version-1 store. Every registered address must have
semantically valid ownership/history and clear operation control. A visible
`in_progress` or `recovery_required` marker is an incident that blocks
migration. PostgreSQL recovery is deliberately v2-only, so preserve a blocked
v1 store and escalate to the backed-up incident/disaster-recovery procedure;
never clear metadata manually to force migration. An `available`
`lock-status` result is only instantaneous and reserves nothing—the migration
acquires its own locks.

## Configure the administrative invocation

Add the optional role-variable name to the PostgreSQL provider:

```yaml
deployment_state:
  backend: postgres
  namespace: platform
  lock_timeout_seconds: 30
  postgres:
    dsn_env: STREAMT_STATE_POSTGRES_DSN
    schema: streamt
    writer_role_env: STREAMT_STATE_POSTGRES_WRITER_ROLE
    writer_dsn_env: STREAMT_STATE_POSTGRES_WRITER_DSN
```

Set the named variables in the operator environment:

```bash
export STREAMT_STATE_POSTGRES_DSN='postgresql://schema-owner@primary.example/state'
export STREAMT_STATE_POSTGRES_WRITER_ROLE='streamt_state_writer'
export STREAMT_STATE_POSTGRES_WRITER_DSN='postgresql://streamt_state_writer@primary.example/state'
```

The real process environment takes precedence over `.env.<environment>`, which
takes precedence over `.env`. Configuration retains only the variable names.
The resolved DSNs, database logins, writer role, schema name, and role OID are
not written to plans or normal text/JSON output.

Version-1 init and version-1/version-2 status and lock diagnostics do not
require or resolve `writer_role_env`. On an existing exact v2 store, owner-only
`state init` may register another empty address. Initializing a new store still
creates version 1. Migration resolves the owner `dsn_env` and
`writer_role_env`, but does not resolve `writer_dsn_env`. Online
plan/apply/adopt and both recovery commands do the reverse: they resolve only
`writer_dsn_env` and never use the owner/admin credential as a fallback.

## Run the migration

Supply the exact canonical UUID reported by `state status` and the exact role
value resolved through `writer_role_env`:

```bash
streamt state migrate-postgres-v2 -p . -e prod \
  --confirm-store-id 8d04f3f7-0000-4000-8000-000000000000 \
  --confirm-writer-role streamt_state_writer
```

Both confirmations are required. A missing or malformed value fails before
project parsing or provider construction. A well-formed but incorrect value is
rejected before catalog mutation. Values are never echoed in error output.

The command:

1. acquires the schema-initialization lock and every registered address lock in
   deterministic order under one bounded deadline;
2. rereads the complete v1 source and requires every operation control to be
   clear;
3. validates every current-state, state-history, and operation-history row,
   including serial/checksum and operation-sequence relationships;
4. changes metadata and constraints, appends the v2 migration ledger row, and
   installs the exact writer ACL in one serializable transaction;
5. verifies the complete postimage through a new direct-primary connection;
6. releases every address lock in reverse order and then the schema lock before
   reporting success.

Current state, address mappings, operation control, and both histories are
preserved byte-for-byte. Repeating the command with the same confirmed store
and writer is an idempotent `already_migrated` result. A partial v2 catalog,
different writer, semantic history error, ACL drift, busy address, or active
control fails closed; the command does not repair or rebind it.

The structured result data is limited to:

```json
{
  "backend": "postgres",
  "outcome": "migrated",
  "store_id": "8d04f3f7-0000-4000-8000-000000000000",
  "schema_version": 2,
  "ordinary_state_authority": "supported_for_v2_writer",
  "mutation_status": "catalog_ready"
}
```

`outcome` is `migrated` or `already_migrated`. `catalog_ready` and
`supported_for_v2_writer` describe catalog capability. Migration does not
resolve the writer DSN, so neither value is a credential or endpoint probe.

## Exact writer ACL

Every writer grant is direct, non-grantable, and issued by the common schema
owner. `PUBLIC` has no access.

| Object | Required writer privilege |
| --- | --- |
| Schema | `USAGE` |
| All seven tables | table-level `SELECT` |
| `current_state` | column `INSERT` on `namespace`, `project`, `environment`, `revision`, `state_serial`, `state_checksum`, `state_json`, `updated_at` |
| `current_state` | column `UPDATE` on `revision`, `state_serial`, `state_checksum`, `state_json`, `updated_at` |
| `operation_control` | column `UPDATE` on `revision`, `status`, `control_json`, `updated_at` |
| `state_history` | column `INSERT` on `namespace`, `project`, `environment`, `revision`, `state_serial`, `state_checksum`, `state_json`, `operation_id`, `recorded_at` |
| `operation_history` | column `INSERT` on `namespace`, `project`, `environment`, `operation_id`, `event_index`, `event_kind`, `control_json`, `recorded_at` |

There are no v2 sequences or identity objects. Table-level `INSERT`/`UPDATE`,
key-column updates, `DELETE`, `TRUNCATE`, `REFERENCES`, `TRIGGER`, `MAINTAIN`,
schema `CREATE`, default privileges, grant options, ownership, role membership,
metadata/migration writes, address registration, and history rewrites are all
forbidden. Missing, extra, wrong-level, wrong-grantor, grantable, default, or
`PUBLIC` privileges make the catalog incompatible.

## Verify and interpret failures

After success, verify through the owner or a conforming read-only status role:

```bash
streamt -o json state status -p . -e prod
streamt state lock-status -p . -e prod
```

Exact v2 structured status reports schema version `2`, `mutation_status:
catalog_ready`, and `ordinary_state_authority: supported_for_v2_writer`.
The label is derived from catalog version and does not resolve or authenticate
`writer_dsn_env`; the writer name is intentionally absent from both forms.
`state lock-status` reports `ordinary_state_authority: not_verified` because it
uses the administrative/status credential and makes no writer-authority claim.

| Code | Meaning and operator action |
| --- | --- |
| `E411_STATE_INVALID` | Confirmation, source, role, catalog, control, history, or ACL is incompatible. Correct the external cause; do not edit streamt metadata or history as a repair. |
| `E420_STATE_BACKEND_UNAVAILABLE` | Configuration, optional dependency, credential, endpoint, or database access is unavailable. Correct it and rerun preflight. |
| `E422_STATE_LOCK_TIMEOUT` | The bounded schema/address lock deadline expired. Resolve the holder, confirm operation control remains clear, then retry the identical confirmed command. Never force-unlock a PostgreSQL session advisory lock. |
| `E425_STATE_UNKNOWN_OUTCOME` | Commit outcome could not be classified. Do not blindly replay. Preserve evidence and inspect `state status`, the migration ledger, active backend session, and backup before deciding whether the exact invocation is safe. |
| `E426_STATE_RELEASE_FAILED_AFTER_COMMIT` | The v2 postimage was verified but lock release was not; structured data reports `committed: true`. Treat the migration as committed, investigate the session/endpoint, and do not replay it as an uncommitted write. |

A precommit failure rolls back metadata, ledger, and ACL together. There is no
in-place downgrade. After a verified v2 commit, do not drop the writer column,
delete the ledger row, hand-edit ACLs, or update the stored writer name. There
is also no automatic repair or rebind command. Restore the tested pre-migration
backup into a controlled target if rollback is required, or recreate the exact
stored role/ACL under a reviewed DBA procedure.

Never downgrade streamt or restore/downgrade the catalog while any registered
address has an `in_progress` or `recovery_required` marker. Preserve the active
catalog and marker, finish the reviewed incident/recovery procedure, verify
clear control, and only then execute the separately reviewed restore-based
rollback.

## Use the v2 writer

After migration, online `plan`, direct or reviewed `apply`, `adopt`,
`state recovery-plan`, and `state recover` resolve only the DSN named by
`postgres.writer_dsn_env`. The owner/admin `postgres.dsn_env` is not used and
is never a fallback. The two DSN environment-variable names must be different,
and the writer connection must authenticate as the exact role bound by
migration. Keep the owner credential out of ordinary deployment jobs.

Every ordinary operation revalidates schema version 2, the complete catalog
and ACL, exact `session_user`/`current_user`, direct-primary status, and lock
ownership at its safety boundaries. A v1 store, status-reader DSN, owner DSN,
ACL drift, replica, or missing writer value fails before runtime mutation and
never falls back to local or empty state.

Recovery revalidates the complete v2 catalog, writer identity and ACL, direct
primary topology, state/control preimage, and reviewed target evidence under
one address lock. It atomically appends recovery history, writes the reviewed
ownership revision when needed, and clears operation control. It never repeats
runtime mutations.

Follow the [deployment-state recovery runbook](state-recovery.md) for the
two-command workflow, exact confirmations, supported observation boundaries,
backup requirements, and indeterminate-outcome handling. Present Flink jobs
and nonempty or present-prior Gateway rules cannot currently be reconstructed
exactly and fail closed.

## Topology and HA boundary

Support is limited to one direct endpoint for one standalone primary. Every
state transition must be durable on that primary before acknowledgement. All
poolers and proxies are unsupported in every mode, and pooler absence is an
operator-verified prerequisite because it cannot be detected reliably from a
PostgreSQL session. All replication, promotion, failover, cluster-writer,
multi-primary, and other HA topologies are unsupported, including synchronous
replication. A topology change requires freezing commands and moving through a
tested backup/restore and fresh-preflight procedure; it is not transparent
failover.
