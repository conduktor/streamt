# Remote deployment state and operation locking

## Status

Partially implemented safety contract. The provider-neutral snapshot/operation
boundary, local version 1 provider, canonical action identities, and hardened
local apply/adopt flows are implemented. PostgreSQL has optional administrative
commands for strict v1/v2 status, confirmed version-1 initialization,
non-reserving lock diagnostics, and explicit confirmed version-1-to-version-2
migration. The v2 catalog binds an exact externally created least-privilege
writer, and the backend protocol is exercised privately through that role on
PostgreSQL 14 and 18. Explicit reviewed recovery is implemented for local state
and through a recovery-only PostgreSQL v2 writer. PostgreSQL is still not
selectable as ordinary state authority. Local JSON remains the only ordinary
provider and compatibility default for single-user development.

The remaining implementation must follow
`docs/plans/2026-09-02-postgres-slice5-foundation.md`. Nothing in the future
contract sections below enables ordinary PostgreSQL selection: the factory
stays disabled until the remaining topology/HA evidence where claimed and final
release gates pass in the enablement commit.

## Scope

This specification covers the ownership state used by `plan`, `apply`, and
`adopt`. It does not make backend mutations transactional. Kafka, Schema
Registry, Flink, Kafka Connect, and Gateway remain independent systems, so a
process can fail after changing one system and before updating ownership state.
The protocol below makes that condition visible and blocks further mutation
until it is reconciled.

Remote state has four jobs:

1. Provide one authoritative ownership snapshot for a project environment.
2. Reject stale writers with compare-and-swap semantics.
3. Serialize mutating operations across machines.
4. Record unfinished operations durably before any runtime mutation.

A shared filesystem plus `flock` is not a remote backend. Object storage CAS
without an operation lock and durable recovery marker is also insufficient.

## Invariants

1. State is selected explicitly. Failure to read a configured remote backend
   never falls back to local state or an empty snapshot.
2. The stable state address includes namespace, project, and environment. A
   snapshot whose embedded identity differs is rejected.
3. Every mutation transition compares one workflow-bound operation snapshot:
   the opaque state and control revisions plus the logical state serial and
   checksum. A remote snapshot is transactionally consistent. The replacement
   ownership serial is unchanged when ownership does not change or exactly the
   prior serial plus one when it does.
4. `apply` and `adopt` hold one exclusive operation lock from the authoritative
   state/control read through completion or recovery recording, and release it
   before reporting final success.
5. An operation intent is committed before the first runtime mutation. A
   remaining intent blocks every later mutating command.
6. Lock loss, renewal failure, unknown commit outcome, or state persistence
   failure is fail-closed. No further runtime action is started and ownership
   state is not advanced as though the operation succeeded.
7. A reviewed plan binds to the backend instance, state address, ownership
   serial, and canonical state checksum used during planning.
8. State is never rewound in place. Recovery creates a new monotonic revision
   and, when ownership content changes, a new higher serial.
9. Credentials, connection strings, lock tokens, and provider responses that
   may contain credentials never enter state, reviewed plans, structured
   output, logs, or exception text.
10. The local backend retains its current version 1 JSON representation,
    environment-specific path, atomic replacement, and warning. Refactoring it
    behind an interface must not silently change those behaviors.
11. Remote finalization atomically commits ownership when changed, control
    clearing, state history when changed, and operation history under one
    transactional authority.
12. Durable action/progress/history identity is the canonical logical resource
    URI; a provider-facing runtime or display name is separate metadata.
13. PostgreSQL ordinary operations require a direct, session-affine primary.
    Production failover support additionally requires synchronous durability
    on every node eligible for promotion for all ownership and operation
    transitions.

## State identity

The canonical address is a structured value, not a user-assembled path:

```text
streamt-state://<namespace>/<project>/<environment>
```

`namespace` separates independent organizations or installations. `project`
and `environment` come from the parsed project, not free-form command flags.
Each segment is non-empty, slash-free, and validated before it reaches a
provider API.

Every initialized remote store has a random immutable `store_id`. Moving a
project to a different database with an identical serial must still make an old
reviewed plan stale. A state observation therefore contains ownership-only
evidence:

```text
store_id            immutable backend-instance UUID
address             namespace + project + environment
state               strict LocalState-compatible ownership payload
state_serial        state.serial
state_checksum      sha256 of canonical state JSON
state_revision      opaque ownership compare-and-swap token
```

The opaque `state_revision` is meaningful only to its backend and must not be
used as the logical serial. The canonical checksum covers the complete strict
ownership payload, including resources, but excludes provider and operation
metadata.

A `ControlObservation` contains the same store/address binding, a separate
opaque `control_revision`, and the `clear`, `in_progress`, or
`recovery_required` record. An `OperationSnapshot` contains a state observation
and control observation bound at one locked workflow boundary. The local
provider reads its adjacent files under the same operation lock without
claiming cross-file atomicity; a remote provider must read both from one
consistent backend snapshot. The address and store are derived through the
state observation, and control must match the exact address. Neither a
state-only observation nor independently committed remote state/control reads
can authorize mutation.

Absent ownership at a registered address is represented by an explicit
`ABSENT` state revision and an in-memory serial-zero state with the requested
project and environment. Before the first operation it pairs with clear
control; after a first-operation interruption it can pair with `in_progress` or
`recovery_required`, which must remain readable as the durable blocker. It is
not equivalent to a read error. An absent address or missing control row is an
invalid/uninitialized remote store condition, not absent ownership.

## Backend interface

The additive provider-neutral foundation uses a typed boundary equivalent to
the current names below. `OperationSnapshot` stores the two observations; its
address and store identity are derived through `state`, and construction
requires the control address to match. The excerpt shows the
`OperationSnapshot` mutation path; legacy local compatibility overloads and
delegates are intentionally omitted.

```python
@dataclass(frozen=True)
class OperationSnapshot:
    state: StateObservation
    control: ControlObservation


class DeploymentStateOperation(Protocol):
    def observe(self) -> OperationSnapshot: ...
    def ensure_ready(
        self,
        observation: OperationSnapshot,
    ) -> None: ...
    def check_lock(self) -> None: ...
    def begin_operation(
        self,
        observation: OperationSnapshot,
        intent: OperationIntent,
    ) -> OperationSnapshot: ...
    def record_progress(
        self,
        observation: OperationSnapshot,
        progress: OperationProgress,
    ) -> OperationSnapshot: ...
    def mark_recovery_required(
        self,
        observation: OperationSnapshot,
        recovery: RecoveryRecord,
    ) -> OperationSnapshot: ...
    def commit_operation(
        self,
        observation: OperationSnapshot,
        replacement: LocalState | None,
    ) -> OperationSnapshot: ...
    def clear_before_mutation(
        self,
        observation: OperationSnapshot,
    ) -> OperationSnapshot: ...


class DeploymentStateBackend(Protocol):
    def describe(self) -> StateStoreIdentity: ...
    def read(self, address: StateAddress) -> StateObservation: ...
    def read_control(self, address: StateAddress) -> ControlObservation: ...
    def operation(
        self,
        address: StateAddress,
    ) -> ContextManager[DeploymentStateOperation]: ...
```

The public application layer must not branch on provider-specific ETags,
database transaction IDs, paths, or lock handles. Provider exceptions are
translated into stable, distinct errors for unavailable, invalid state, state
conflict, lock timeout, lock lost, recovery required, unknown outcome, and
release failure after a verified commit.

The local provider implements `describe`, a state/control binding under its
operation lock, revision-and-serial CAS, and the exclusive
intent/progress/recovery operation surface. `apply` and `adopt` retain the
pre-existing adjacent-file lock for the operation's complete lifetime, while
`plan` performs lock-free read-only state and safe operation-status reads.
Ownership remains version 1 JSON; version 1 control metadata lives in a
separate atomically replaced sidecar.

Every conforming `OperationSnapshot` transition compares the state and control
observations and lock ownership. `begin_operation` also compares the prior
logical serial/checksum and requires clear control. `clear_before_mutation` is
legal only when progress proves that no runtime action started. For PostgreSQL,
`commit_operation` atomically writes ownership when needed, clears control, and
appends state and operation history in one database transaction. A remote
backend that cannot enforce these predicates and atomic finalization does not
conform. A generic `save()` method is not part of the remote interface because
it invites unchecked overwrites.

The richer remote surface described in this specification is a target contract,
not a claim that the additive foundation already contains public
`read_snapshot`, history, fail, or abort methods. A PostgreSQL adapter may add a
backend `read_snapshot(address)` for consistent lock-free plan/status reads and
private helpers for append-only history. Application mutation continues through
`observe`, `mark_recovery_required`, `clear_before_mutation`, and
`commit_operation`; the remote adapter supplies the stronger atomic semantics
behind those existing operations.

The local provider may preserve its conservative separate-file write ordering
under the existing operation lock. Compatibility delegates may remain private
to that adapter. The ordinary application path still consumes one
workflow-bound snapshot and the operation surface so provider differences
cannot weaken command sequencing.

Read-only online `plan` does not lock. The local compatibility path may read
state and safe control status separately because it cannot authorize mutation;
the target PostgreSQL path uses one consistent remote snapshot. Offline
planning does not construct or read any state backend.

## Compare-and-swap and reviewed plans

Logical serial and backend revisions solve different problems:

- The serial is portable review evidence and advances once when owned resource
  content changes.
- The state and control revisions independently detect every relevant backend
  record change, including operation metadata changes that do not change
  ownership.
- The state checksum detects same-serial content replacement, accidental
  restoration, and a plan/apply switch between snapshots.

Reviewed-plan format version 3 adds this strict state reference:

```json
{
  "state": {
    "backend": "postgres",
    "store_id": "8d04f3f7-...",
    "address": "streamt-state://platform/payments/prod",
    "serial": 12,
    "checksum": "sha256:..."
  }
}
```

Provider revisions and lock tokens are not portable review data and are not
written to the plan. At apply time streamt acquires the lock, reads a consistent
snapshot, and requires the state reference to match before live replanning. It
then directly rereads the complete snapshot after live planning. Both reviewed
and direct apply reject any intervening state or control change and use only
that final snapshot for `begin_operation` CAS. Offline plan files use `state:
null`; they remain ineligible to authorize mutation.

Changing this envelope required a plan-format version bump. Versions 1 and 2
are rejected with regeneration guidance rather than interpreted with weaker
remote-state semantics.

## Planned-action identity

Planner output uses a typed action containing a canonical logical `resource_id`
string, a separate runtime/display label, and an action string. The record is
equivalent to:

```python
@dataclass(frozen=True)
class PlannedAction:
    resource_id: str
    runtime_label: str
    action: str
```

The durable `resource_id` serializes as:

```text
streamt://<project>/<environment>/<kind>/<logical-name>
```

Reviewed action comparison, operation intent, ordered progress, failure
records, recovery, and history use this canonical identity. Runtime deployer
callbacks may use the separate provider-facing label, but a label such as
`topic:orders.v1` is not durable identity evidence. Construction rejects
non-canonical resource URI syntax. Planner/operation construction, where the
full ordered set and state address are available, rejects duplicates and
project/environment values that do not match that address.

## Lock and operation protocol

### Lock requirements

Locks are exclusive per canonical state address. The acquisition request has a
random operation ID, a redacted actor label, and a bounded wait timeout.

The first remote backend should use PostgreSQL session advisory locks plus a
state row, rather than inventing an expiring client-clock lease. Session lock
release on connection loss avoids unsafe clock-skew takeover. A collision-safe
mapping from full state address to advisory-lock key must be stored and checked;
a hash collision may serialize unrelated projects but must never select another
project's state row.

The owning connection is checked before every runtime mutation and before every
state transition. A future expiring-lease provider must additionally provide:

- A backend-time expiry decision, not client-clock-only takeover.
- Renewal well before expiry.
- A fencing mechanism that prevents an expired holder from committing state.
- A durable recovery marker that a successor sees before it can mutate.

Force-unlock is not an ordinary apply flag. PostgreSQL session locks are
released by ending the owning database session. An unfinished operation marker
survives that release and still requires explicit recovery.

### Apply sequence

1. Parse and strict-validate project, environment, state configuration, and the
   reviewed plan without constructing runtime deployers.
2. Acquire the exclusive state-address lock within the configured timeout.
3. Read one `OperationSnapshot` under the lock. Reject identity, format, store,
   serial, checksum, plan, or recovery-marker mismatch.
4. Observe live infrastructure and rebuild typed canonical planned actions.
   Reject plan drift, ownership requirements, and safety blockers. Reviewed
   apply compares the complete regenerated plan; direct apply retains the
   initial snapshot as drift evidence.
5. Directly reread the complete operation snapshot after live planning for both
   direct and reviewed apply. Reject any state or control change. This final
   snapshot, not the initial read, is the CAS authority for begin.
6. CAS an `in_progress` intent containing operation ID, reviewed-plan checksum
   when applicable, start time, actor label, and ordered canonical resource
   action identities. Do not put runtime labels, resource contents,
   credentials, or raw provider errors in the intent.
7. Before every backend action, verify lock health. Execute the already planned
   action and atomically record only safe ordered progress metadata.
8. If all actions succeed, `commit_operation` atomically replaces ownership
   when changed, clears control, and appends state and operation history.
   Advance ownership serial only if owned records changed.
9. Perform a fresh direct verification read using operation ID and expected
   state identity. Do not retry an ambiguous commit automatically.
10. If failure is proven to precede the first runtime action, atomically clear
    the intent and append its failure event without advancing ownership serial.
11. If any mutation may have succeeded, rollback is incomplete, the lock is
    lost, or finalization is unknown, persist or preserve `recovery_required`.
    Never claim success and never automatically retry.
12. Release the lock in `finally`, before emitting or flushing final success. A
    release failure after a freshly verified commit reports a distinct
    `committed: true` result and must not advise replaying the mutation.

An in-flight external API call cannot be fenced by the state database. The
durable operation marker is therefore essential: after a runner loses its lock,
a successor is blocked even if the old call later succeeds.

### Adoption sequence

Adoption uses the same lock and intent protocol. It resolves and observes the
exact live resource and constructs an exact fingerprint for confirmation. After
confirmation it acquires the lock when the local compatibility path does not
already hold it, reads the complete state/control snapshot, and re-observes the
exact target. It must compare that post-confirmation fingerprint with the exact
confirmed evidence; any difference fails closed or requires a new interactive
confirmation. It then performs a final snapshot reread, records a state-only
intent using the canonical resource identity, atomically commits ownership,
control clearing, and history, verifies the result, and releases before
reporting success. Topic or subject APIs are never mutated.

The current local-only path takes a stricter, simpler approach:
it acquires the same-host operation lock before its authoritative observation
and keeps that lock while an interactive confirmation prompt is open. A second
local mutator therefore waits instead of observing the same prior serial. This
behavior is intentional. Adoption writes a durable state-only intent before
its ownership CAS and clears it only after the ownership write succeeds.
Operators should prefer the non-interactive exact-resource and environment
confirmation flags in automation. The local path has durable interruption
detection and explicit reviewed recovery, but still has no cross-host
exclusion. Holding the local state lock does not prevent the independently
managed runtime resource from changing during the prompt; recovery therefore
recompiles and, when live evidence is required, freshly re-observes exact
targets under its lock before finalization.

## Operation and recovery records

Operation metadata is control-plane metadata outside the version 1 ownership
JSON payload. It contains only:

- Operation ID and kind (`apply`, `adopt`, `migration`, or `recovery`).
- Reviewed-plan checksum when applicable.
- Redacted actor label and timestamps.
- Prior state serial and checksum.
- Ordered canonical logical resource identities and action names. Runtime or
  display labels are not durable identities.
- Last safely completed action index.
- Status and a stable, sanitized failure code.

`in_progress` and `recovery_required` both block mutation. There is no automatic
"stale after N minutes" clearing rule. Time is diagnostic evidence, not proof
that an external mutation did not happen.

The implemented local sidecar is
`.streamt/state/<environment>.control.json`. Online plan exposes only its safe
status fields and never modifies it. Operators must not delete or edit an
active sidecar, or roll back streamt versions while one exists. Retain the
evidence and use the explicit reviewed recovery workflow only when every target
is exactly representable. The local file lock and sidecar do not provide
cross-host or distributed safety.

Recovery begins with read-only status and, when live evidence is required, a
fresh live plan. An operator must choose one explicit resolution:

- `observed`: accept freshly observed live reality and write the exact reviewed
  ownership result.
- `rolled_back`: confirm all attempted mutations were reversed and retain the
  prior ownership payload.
- `abandoned_before_mutation`: permitted only when progress proves that no
  runtime action started.

`state recovery-plan` writes strict checksum-bound evidence to a new mode-`0600`
regular file without replacing any path or following a symlink. `state recover`
requires exact operation-ID, resolution, and evidence-checksum confirmations,
then rereads the project and target evidence under the operation lock. Recovery
uses a new operation ID and CAS revision. It never retries a runtime mutation,
lowers a serial, or runs automatically, and it preserves append-only intent and
resolution audit events. A provider may compact history, but not until the
configured retention period has passed.

The three-outcome workflow remains a minimum PostgreSQL factory-enablement
gate, not optional later operations work. Its local/PostgreSQL command-level
E2E and failure-injection coverage has shipped. Local-to-remote migration and
export may ship afterward, while ordinary PostgreSQL mutation remains
unreachable until its remaining topology/HA and final release gates pass.

## First remote backend: PostgreSQL

The first implementation target is PostgreSQL because it can keep state,
operation metadata, history, and locking under one transactional authority. It
must use a dedicated schema with:

- One immutable store metadata row containing `store_id` and schema version.
- One current-state row per canonical address.
- Append-only revision and operation history.
- Strict primary/unique keys over namespace, project, and environment.
- Parameterized values and a separately validated SQL schema identifier.

Initialization is explicit. A missing schema is created; a pre-existing empty
schema must be owned by the initializer identity. In either case streamt creates
the frozen seven-table version-1 catalog, one immutable random store ID, the
requested collision-checked address mapping, absent ownership, and clear
operation control in one transaction. An exact compatible store may register a
previously unregistered empty address, while repeating initialization for the
same compatible empty address is an idempotent no-op. It never imports local
state or adopts, repairs, or migrates a populated target. Partial catalogs,
extra objects, populated or active target addresses, unsupported versions,
identity mismatch, and address-lock-key collision fail closed.

PostgreSQL object ownership and ACLs are part of catalog conformance. The schema
and all seven tables must have one common owner. `PUBLIC` may have no schema,
table, or column privilege. Named status roles may hold only non-grantable
`USAGE` on the schema and non-grantable `SELECT` on tables or columns; mutating
or grantable non-owner access is rejected. For a newly created schema,
initialization revokes all schema access from `PUBLIC`, and it revokes all table
access from `PUBLIC` after every table creation, neutralizing unsafe `PUBLIC`
default table privileges. streamt creates no roles and issues no grants; role
creation and allowed status-reader grants remain an external DBA operation.
Version 1 has no ordinary PostgreSQL runtime role. Its owner-only private
mutation path remains test scaffolding and cannot be selected by normal
commands. Version 2 adds the separate writer contract below without weakening
version-1 validation.

### Mutation schema and role gate

Version 1 remains the frozen administrative catalog above. Private Slice 5
backend tests may directly construct the backend with an isolated schema-owner
credential so the transaction protocol can be developed without exposing it
through normal provider selection. That exception is test scaffolding only: an
owner credential is not an ordinary deployment identity, and a version-1 store
must never be reported as production mutation-ready.

streamt ships an explicit `state migrate-postgres-v2` administrative migration
before enabling the ordinary factory. It uses bounded locks, exact store/role
confirmation, validate-before-commit, fresh read-back, and unknown-outcome
classification. Version 2 records the configured writer-role name in immutable
catalog metadata and validates its exact non-grantable ACL. It does not create
the role, infer authority from the current login, silently upgrade on
`plan`/`apply`/`adopt`, or weaken version-1 validation.

The role name is the portable logical identity; a cluster-local `pg_roles.oid`
must not be persisted in user metadata because logical restore does not remap
it. The administrative migration requires an externally pre-created, exactly
confirmed safe role, then transactionally resets that role's state-schema ACL
and installs the exact grants itself. External writer DML pre-grants are not a
valid source state because version 1 rejects them.

The version-2 ordinary writer ACL is exact:

| Object | Required ordinary writer grants | Forbidden grants |
| --- | --- | --- |
| Schema | `USAGE` | `CREATE`, grant option, `PUBLIC` access |
| Store metadata and migration ledger | `SELECT` | `INSERT`, `UPDATE`, `DELETE` |
| Address and lock-key mapping | `SELECT` | `INSERT`, `UPDATE`, `DELETE` |
| Current ownership | table `SELECT`; column `INSERT` on `namespace`, `project`, `environment`, `revision`, `state_serial`, `state_checksum`, `state_json`, `updated_at`; column `UPDATE` on `revision`, `state_serial`, `state_checksum`, `state_json`, `updated_at` | table-level `INSERT`/`UPDATE`, key-column `UPDATE`, `DELETE`, grant option |
| Operation control | table `SELECT`; column `UPDATE` on `revision`, `status`, `control_json`, `updated_at` | `INSERT`, table-level `UPDATE`, key-column `UPDATE`, `DELETE`, grant option |
| State history | table `SELECT`; column `INSERT` on `namespace`, `project`, `environment`, `revision`, `state_serial`, `state_checksum`, `state_json`, `operation_id`, `recorded_at` | table-level `INSERT`, `UPDATE`, `DELETE`, grant option |
| Operation history | table `SELECT`; column `INSERT` on `namespace`, `project`, `environment`, `operation_id`, `event_index`, `event_kind`, `control_json`, `recorded_at` | table-level `INSERT`, `UPDATE`, `DELETE`, grant option |

Version 2 introduces no sequence or identity objects. The ordinary writer has
no ownership, DDL, role membership,
default-privilege mutation, address registration, metadata/migration mutation,
history rewrite, or schema-migration authority. Catalog validation rejects a
missing, extra, wrong-level, wrong-grantor, grantable, default, owner, or
`PUBLIC` privilege. Private conformance tests run the complete mutation
lifecycle through this least-privilege role, not the version-1 owner identity.

The writer is a direct login distinct from the common owner and has
`NOSUPERUSER`, `NOCREATEDB`, `NOCREATEROLE`, `NOREPLICATION`, `NOBYPASSRLS`,
`NOINHERIT`, and no membership edge in either direction. Its writer ACL grantor
is the common owner. An ordinary writer connection proves both `session_user`
and `current_user` equal the stored name. Existing named status readers remain
limited to direct non-grantable `USAGE` and `SELECT`.

Migration holds the schema initialization lock and every registered address
session lock under one bounded deadline before starting its serializable write
transaction. It validates all current ownership and both histories
semantically, requires every operation control to be clear, and preserves the
durable rows byte-for-byte. DDL, metadata, ledger, and ACL changes commit
atomically and are freshly verified through a new direct-primary connection.
Post-v2 status, lock diagnostics, and owner-only address registration validate
exact v2 without requiring the writer environment variable. Status reports
`mutation_status: catalog_ready` separately from ordinary factory authority,
which remains disabled until the final gate.

The version-2 implementation extends the strict PostgreSQL shape with optional
`writer_role_env`, the name of an environment variable containing the writer
role identifier. Version-1 init and v1/v2 status/lock diagnostics do not require
or resolve it; the version-2 migration does. Migration requires exact role and
immutable store-ID confirmations before mutation, stores the resolved name in
catalog metadata, and freshly verifies its ACL. Configuration retains only the
environment-variable name; resolution uses real environment, selected
environment dotenv, then base dotenv precedence. An ordinary session must
prove that `session_user` and `current_user` are that exact role; membership or
equivalent effective privileges are insufficient. The resolved role, database
login, DSN, and catalog OID remain excluded from reviewed plans and normal
text/structured output.

All administrative paths set transaction-local `search_path` to
`pg_catalog`. All state-object identifiers are validated and schema-qualified.
Initialization takes a bounded schema-scoped session advisory lock before
beginning its serializable transaction, so a concurrent waiter starts from a
fresh snapshot after the prior initializer commits. The complete result is
validated before commit and again through a fresh read-only connection
afterward. A precommit failure rolls back DDL and rows together. An ambiguous
commit is not retried; the operator resolves it with status or the identical
confirmed init request.

Lock diagnostics require a direct, session-affine primary endpoint. PostgreSQL
advisory locks are physical-session, reentrant state, and the future operation
lock must remain on one connection for its complete lifetime. Transaction- and
statement-pooling endpoints are therefore unsupported for this integration.

Ordinary PostgreSQL operations have the same endpoint requirement. The owning
connection checks primary status and its advisory lock before every external
action and transition. For a standalone deployment, a transition must be
durable on that direct primary before acknowledgement. A production HA claim
requires synchronous replication of every intent, progress, ownership,
finalization, and recovery transition to every node eligible for promotion.
Asynchronous promotion can release the old session lock while losing its
durable marker, so it is outside the supported HA safety boundary.

State size has a configured hard limit checked before mutation. The provider
must set statement and lock timeouts, require TLS for non-loopback endpoints by
default, and avoid retrying an unknown transaction commit automatically.

Object-store providers are deferred until they demonstrate equivalent atomic
CAS, fencing, operation markers, and recovery behavior. S3-compatible storage
plus a best-effort lock file does not meet this contract.

## Configuration and CLI surface

Use `deployment_state` to avoid confusion with Flink application state:

```yaml
# Administrative commands and recovery-only v2 authority are available.
# Ordinary PostgreSQL state authority is not.
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

`backend` is `local` or `postgres`. Local remains the default and uses the
existing `.streamt/state/<environment>.json` path; it accepts no remote fields.
The PostgreSQL configuration names environment variables rather than embedding
DSNs or role values in parsed configuration. `dsn_env` names the owner/admin
DSN, `writer_role_env` names the role value bound during v2 migration, and the
optional `writer_dsn_env` names the exact v2 writer DSN used only for recovery.
Every configured name must be a valid environment-variable name;
`writer_dsn_env` must differ from `dsn_env`. Unknown fields and provider-shape
mismatches fail strict validation.

If the entire block is omitted, streamt selects `backend: local`. An explicit
block must contain its `backend` discriminator; an empty block is invalid.
PostgreSQL `namespace` is required and follows the canonical address contract:
it is nonempty and slash-free. `lock_timeout_seconds` defaults to `30` and must
be an integer from 1 through 300. `postgres.schema` defaults to `streamt` and
must match `^[A-Za-z_][A-Za-z0-9_]*$`.

In multi-environment mode, a root `deployment_state` block is inherited when
the selected environment omits it. An environment block replaces the complete
root provider block; provider fields are never deep-merged. This differs
intentionally from root `runtime`, which remains ignored in multi-environment
mode. A partial PostgreSQL environment override therefore fails instead of
borrowing fields from the root. Environment sidecars may select either tagged
provider without a command-line override.

The parser validates names, not secret values. A value is resolved after
`.env`, `.env.<environment>`, and the real process environment have been
applied, and only when a command needs that exact authority. Validation,
compilation, and offline planning neither require nor read a DSN. PostgreSQL
administration requires the optional package extra and an explicit owner
endpoint, enforces TLS for non-loopback connections, and returns only
secret-neutral failures. Recovery resolves only `writer_dsn_env`, proves the
stored v2 writer identity and exact ACL, and never falls back to `dsn_env`.
Ordinary plan/apply/adopt still fail with a sanitized backend-unavailable error
and never fall back to local state.

The final enablement commit may change that ordinary-factory result only for an
exactly compatible version-2 store reached through its validated ordinary
writer role. It remains disabled for version 1, owner-as-runtime credentials,
partial migrations, ACL drift, unsupported endpoints, or any failed command,
topology, or release gate. No ordinary command migrates the schema.

State backend selection belongs to effective environment configuration and
cannot be overridden by `plan`, `apply`, or `adopt` flags. This prevents an
operator from accidentally planning against one authority and applying against
another. The reviewed state reference also detects endpoint changes through the
remote store ID.

The implemented administrative commands are:

```text
streamt state init -p PATH -e ENV \
  --confirm-project PROJECT \
  --confirm-env ENV \
  --confirm-address streamt-state://NAMESPACE/PROJECT/ENV
streamt state status -p PATH -e ENV
streamt state lock-status -p PATH -e ENV
streamt state migrate-postgres-v2 -p PATH -e ENV \
  --confirm-store-id UUID \
  --confirm-writer-role ROLE
streamt state recovery-plan -p PATH -e ENV \
  --resolution OUTCOME \
  --out FILE
streamt state recover -p PATH -e ENV \
  --plan FILE \
  --confirm-operation-id UUID \
  --confirm-resolution OUTCOME \
  --confirm-evidence-checksum sha256:...
```

`init` is PostgreSQL-only and requires all three confirmations to exactly match
the parsed project, effective environment, and canonical address. Confirmation
failure occurs before initializer construction or a database connection. A
successful structured result reports one of `initialized`,
`address_registered`, or `already_initialized`, plus only the safe store ID,
schema version, address, absent ownership, clear operation status, and disabled
ordinary-authority boundary.

For PostgreSQL, `status` uses a separate administrative adapter and one bounded,
repeatable-read, read-only snapshot. It verifies the exact version-1 or
version-2 catalog and reports backend kind, store ID, address, serial, checksum,
and safe operation status without credentials, endpoint details, SQL, raw
exceptions, or ownership payload.

`lock-status` is a separate diagnostic command. It validates the complete
version-1 or version-2 catalog and requires `pg_is_in_recovery()` to report a
primary inside an explicit repeatable-read, read-only transaction. An
unregistered address returns `unregistered` without invoking an advisory-lock
function. A registered address calls `pg_try_advisory_xact_lock(bigint)` once
and reports `available` or `busy`. All three are successful CLI outcomes.
Before returning any result, streamt requires rollback to succeed, releasing a
transaction-scoped lock; the probe therefore reserves nothing and cannot leak
a successful acquisition.

The result contains only `backend`, safe `store_id`, canonical `address`,
`lock_status`, `reservation: none`, and
`ordinary_state_authority: disabled`. It is an instantaneous, racy observation,
not a lock for later work and not evidence that mutation is safe. Full catalog
validation reads the operation-control rows, but the command does not report,
clear, or interpret durable operation control as mutation safety; use `state
status` to view it. `migrate-postgres-v2` requires canonical store-ID and exact
writer-role confirmations, uses only the owner administrative factory, and
returns `migrated` or idempotent `already_migrated` plus schema version `2`,
`mutation_status: catalog_ready`, and
`ordinary_state_authority: disabled`.

The two recovery commands work for local state and for an exact PostgreSQL v2
catalog through the separately configured writer DSN. Planning creates strict,
no-overwrite evidence for one exact blocker. Execution requires the blocked
operation ID, chosen resolution, and evidence checksum again and revalidates
the state/control preimage and any required project fingerprints and fresh
target observation under the address lock. PostgreSQL finalization atomically appends
intent and resolution history, optionally commits reviewed ownership state, and
clears control. Present Flink targets, nonempty or unreconstructible Gateway
targets, and any partial or ambiguous evidence fail closed. See the
[deployment-state recovery runbook](../guides/state-recovery.md).

Ordinary state authority, local-to-remote migration/export, and
ordinary-command wiring remain deferred. Normal commands never initialize or
migrate a remote store implicitly.

The environment-only `safety.require_remote_state` policy defaults to `false`.
When enabled, it fails `apply` and `adopt` before confirmation, compilation,
state access, or runtime deployer construction if the effective backend is
local. It does not block read-only plan or state status. Reviewed-plan-required
and offline-plan-invalid errors retain precedence, and `--force` cannot bypass
the policy. Making remote state implicit for protected environments requires a
documented config migration and release notice.

## Credentials and redaction

- Read each DSN only from the environment variable named for that command's
  authority. Recovery reads only `writer_dsn_env`; administration reads
  `dsn_env`. Do not place either value in a Pydantic model dump or environment
  fingerprint.
- Never expose usernames, passwords, hosts, query parameters, lock tokens,
  database exception detail, SQL text, or provider response bodies by default.
- Use a dedicated initializer identity that owns the configured schema. Keep it
  out of status-only and ordinary deployment jobs. Provision status roles
  externally with only the non-grantable `USAGE`/`SELECT` ACL accepted by the
  exact catalog; streamt never creates or grants a database role.
- Version-1 owner-based mutation remains isolated test scaffolding outside the
  ordinary factory. Migration provisions no role itself: operators create the
  separately identified schema-version-2 writer externally, and conformance
  runs through its exact column/table grants. Do not put the owner credential
  in ordinary CI or deployment jobs.
- Point lock diagnostics and recovery directly at a session-affine primary. Do
  not use a transaction- or statement-pooling endpoint. The probe creates no
  role or grant, and recovery-only authority does not enable ordinary state
  authority.
- Future ordinary PostgreSQL operations must use the same direct-primary
  boundary. Treat asynchronous promotion as unsupported; HA support requires
  the synchronous durability boundary specified above.
- Map provider failures to stable sanitized errors. Detailed diagnostics, when
  explicitly enabled, still pass through central key, URL, authorization, and
  inline-value redaction.
- Reviewed plans bind backend kind, namespace through the state address, and
  remote `store_id` through the existing `StateReference`, not through the
  runtime environment fingerprint. Provider configuration and credentials are
  not added to plan JSON.
- Migration/export files contain ownership state and checksums only. They are
  still operationally sensitive because physical resource names are present,
  and must be created atomically with restrictive permissions.

## Migration and rollback

Local-to-remote migration is copy-and-verify, never move-and-delete:

1. Strictly load local state and configured remote identity.
2. Acquire source and destination locks in deterministic address order.
3. Require the destination address to be absent, or report an identical prior
   completed migration as an idempotent no-op.
4. Show source/destination identities, serial, resource count, and checksum;
   require exact confirmation.
5. Create the destination in one transaction with migration provenance.
6. Read it back and verify identity, serial, checksum, and resource count.
7. Retain the local file as a read-only backup and instruct the user to produce
   a fresh reviewed plan against the remote store.

There is no dual-write period and no automatic local fallback. Rollback to
local is a separate confirmed export/recovery operation. If a populated remote
address already differs, migration stops; it does not merge ownership maps.

Database schema migration follows expand/validate/contract releases. A new
binary reads only explicitly supported schema versions. Destructive database
migrations require an export, backup verification, and a separate admin
command. Ownership-state recovery writes a new higher revision rather than
restoring an old row in place.

## Failure behavior

| Failure | Required behavior |
| --- | --- |
| Remote credentials missing or backend unreachable | Fail; never use local or empty state |
| State identity, version, or checksum invalid | Fail before runtime observation or mutation |
| Lock wait expires | Fail with owner-free sanitized context; do not steal |
| Existing operation marker | Block mutation and direct the user to status/recovery |
| State changes after plan | Reject reviewed plan before mutation |
| State changes while waiting for lock | Re-read and reject stale plan |
| Lock connection is lost | Start no further action; preserve or write recovery-required state if possible |
| Runtime action has unknown result | Mark recovery required; do not retry automatically |
| Ownership commit has unknown result | Re-read by operation ID; if still ambiguous, require recovery |
| Recovery marker persistence is impossible | Emit an indeterminate-operation error and operation ID; later commands must detect the prewritten intent |
| Release fails after a verified commit | Report `committed: true` with the dedicated release-after-commit error; never undo, repeat, or advise replaying the commit |

The error registry must give `state_lock_timeout`, `state_lock_lost`,
`state_conflict`, `state_unknown_outcome`,
`state_release_failed_after_commit`, and `state_recovery_required` distinct
stable machine-readable kinds and dedicated CLI codes. They must not collapse
into a generic provider mutation failure. Each error is sanitized before it is
placed in text, JSON, logs, exception chaining, or telemetry. In particular,
lock timeout is safe to retry only after a new bounded wait; lock loss and
unknown outcome direct the operator to status/recovery; state conflict requires
fresh planning/evidence; and release failure after verified commit explicitly
forbids replaying the mutation.

When a durable intent exists, lock-lost, unknown-outcome, and
release-after-commit errors carry its canonical operation UUID as a distinct
structured `operation_id` and repeat it in sanitized text output. A malformed
provider value is never promoted into that structured recovery field.

## Observability

Structured command output includes the safe state reference, operation ID,
wait duration, and final operation status. It never includes provider revision
tokens or lock handles. Metrics may count acquisition latency, conflicts,
recovery-required operations, and backend failures using backend kind and
environment, but not project/resource names unless explicitly allowed.

Final success output is emitted and flushed only after the operation context
has released its lock. A release failure after verified commit is a non-success
command outcome whose structured payload preserves `committed: true`; it is not
rewritten as an uncommitted failure and does not trigger automatic replay.

Audit history records who initiated an operation only as a caller-supplied,
redacted label. Authentication and durable actor identity belong to the
database or CI platform; streamt must not claim that an arbitrary label is a
verified identity.
