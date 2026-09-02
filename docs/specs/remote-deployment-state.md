# Remote deployment state and operation locking

## Status

Proposed safety contract. Remote state is not supported until an implementation
satisfies this specification and its conformance tests. The existing local JSON
backend remains the default for single-user development.

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
3. Every write compares an opaque backend revision as well as the logical state
   serial. The replacement ownership serial is either unchanged for operation
   metadata or exactly the prior serial plus one for an ownership change.
4. `apply` and `adopt` hold one exclusive operation lock from the final state
   read through completion or recovery recording.
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
reviewed plan stale. A state observation therefore contains:

```text
store_id            immutable backend-instance UUID
address             namespace + project + environment
state               strict LocalState-compatible ownership payload
state_serial        state.serial
state_checksum      sha256 of canonical state JSON
revision            opaque compare-and-swap token
operation           null, in_progress, or recovery_required metadata
```

The opaque `revision` is meaningful only to its backend and must not be used as
the logical serial. The canonical checksum covers the complete strict ownership
payload, including resources, but excludes provider and operation metadata.

An absent address is represented by an explicit `ABSENT` revision and an
in-memory serial-zero state with the requested project and environment. It is
not equivalent to a read error.

## Backend interface

The implementation should introduce a typed boundary equivalent to:

```python
class DeploymentStateBackend(Protocol):
    def describe(self) -> StateStoreIdentity: ...
    def read(self, address: StateAddress) -> StateObservation: ...
    def acquire(self, address: StateAddress, request: LockRequest) -> StateLock: ...
    def check_lock(self, lock: StateLock) -> None: ...
    def begin_operation(
        self,
        lock: StateLock,
        observation: StateObservation,
        intent: OperationIntent,
    ) -> StateObservation: ...
    def record_progress(
        self,
        lock: StateLock,
        observation: StateObservation,
        progress: OperationProgress,
    ) -> StateObservation: ...
    def commit_operation(
        self,
        lock: StateLock,
        observation: StateObservation,
        replacement: LocalState,
    ) -> StateObservation: ...
    def fail_operation(
        self,
        lock: StateLock,
        observation: StateObservation,
        failure: RecoveryRecord,
    ) -> StateObservation: ...
    def release(self, lock: StateLock) -> None: ...
```

The public application layer must not branch on provider-specific ETags,
database transaction IDs, paths, or lock handles. Provider exceptions are
translated into stable errors such as unavailable, invalid state, conflict,
lock timeout, lock lost, recovery required, and unknown commit outcome.

`begin_operation`, `record_progress`, `commit_operation`, and `fail_operation`
are atomic CAS operations over the observation revision and lock ownership. A
remote backend that cannot enforce both predicates does not conform. A generic
`save()` method is not part of the remote interface because it invites
unchecked overwrites.

Read-only online `plan` takes a consistent observation but does not lock.
Offline planning does not construct or read any state backend.

## Compare-and-swap and reviewed plans

Logical serial and backend revision solve different problems:

- The serial is portable review evidence and advances once when owned resource
  content changes.
- The revision detects every backend record change, including operation
  metadata changes that do not change ownership.
- The state checksum detects same-serial content replacement, accidental
  restoration, and a plan/apply switch between snapshots.

The next reviewed-plan format must add a strict state reference:

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
written to the plan. At apply time streamt acquires the lock, reads again, and
requires the state reference to match before live replanning. It then uses the
new observation revision for operation CAS. Offline plan files use `state:
null`; they remain ineligible to authorize mutation.

Changing this envelope requires a plan-format version bump. Older formats are
rejected rather than interpreted with weaker remote-state semantics.

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
3. Read state under the lock. Reject identity, format, store, serial, checksum,
   plan, or recovery-marker mismatch.
4. Observe live infrastructure and rebuild the plan. Reject plan drift,
   ownership requirements, and safety blockers.
5. CAS an `in_progress` intent containing operation ID, reviewed-plan checksum,
   start time, actor label, and ordered resource action identities. Do not put
   resource contents, credentials, or raw provider errors in the intent.
6. Before every backend action, verify lock health. Execute the already reviewed
   action and record only safe progress metadata.
7. If all actions succeed, atomically replace ownership state, clear the intent,
   and advance the ownership serial only if owned records changed.
8. If all actions fail before mutation, clear the intent without advancing the
   ownership serial.
9. If any mutation may have succeeded, rollback is incomplete, the lock is
   lost, or the commit outcome is unknown, persist or preserve
   `recovery_required`. Never claim success and never automatically retry.
10. Release the lock in `finally`. Release failure is reported but does not
    rewrite a committed result.

An in-flight external API call cannot be fenced by the state database. The
durable operation marker is therefore essential: after a runner loses its lock,
a successor is blocked even if the old call later succeeds.

### Adoption sequence

Adoption uses the same lock and intent protocol. It resolves and observes the
exact live resource before confirmation, acquires the lock, re-reads both state
and the exact live resource, repeats confirmation-context checks, then writes
only the ownership record. A stale confirmation or changed resource fails
closed. Topic or subject APIs are never mutated.

## Operation and recovery records

Operation metadata is control-plane metadata outside the version 1 ownership
JSON payload. It contains only:

- Operation ID and kind (`apply`, `adopt`, `migration`, or `recovery`).
- Reviewed-plan checksum when applicable.
- Redacted actor label and timestamps.
- Prior state serial and checksum.
- Ordered resource identities and action names.
- Last safely completed action index.
- Status and a stable, sanitized failure code.

`in_progress` and `recovery_required` both block mutation. There is no automatic
"stale after N minutes" clearing rule. Time is diagnostic evidence, not proof
that an external mutation did not happen.

Recovery begins with read-only status and a fresh live plan. An operator must
choose one explicit resolution:

- `observed`: accept freshly observed live reality and write the exact reviewed
  ownership result.
- `rolled-back`: confirm all attempted mutations were reversed and retain the
  prior ownership payload.
- `abandoned-before-mutation`: permitted only when progress proves that no
  runtime action started.

Recovery uses a new operation ID and CAS revision. It never lowers a serial,
and it preserves an append-only audit event. A provider may compact history,
but not until the configured retention period has passed.

## First remote backend: PostgreSQL

The first implementation target is PostgreSQL because it can keep state,
operation metadata, history, and locking under one transactional authority. It
must use a dedicated schema with:

- One immutable store metadata row containing `store_id` and schema version.
- One current-state row per canonical address.
- Append-only revision and operation history.
- Strict primary/unique keys over namespace, project, and environment.
- Parameterized values and a separately validated SQL schema identifier.

Initialization is explicit and idempotent for an empty compatible schema. The
runtime role should have only the DML and lock privileges needed by streamt;
schema migration uses a separate administrative role. Unsupported database
schema versions fail closed.

State size has a configured hard limit checked before mutation. The provider
must set statement and lock timeouts, require TLS for non-loopback endpoints by
default, and avoid retrying an unknown transaction commit automatically.

Object-store providers are deferred until they demonstrate equivalent atomic
CAS, fencing, operation markers, and recovery behavior. S3-compatible storage
plus a best-effort lock file does not meet this contract.

## Configuration and CLI surface

Use `deployment_state` to avoid confusion with Flink application state:

```yaml
# streamt:skip — proposed configuration, not implemented yet
deployment_state:
  backend: postgres
  namespace: platform
  lock_timeout_seconds: 30
  postgres:
    dsn_env: STREAMT_STATE_POSTGRES_DSN
    schema: streamt
```

`backend` is `local` or `postgres`. Local remains the default and uses the
existing `.streamt/state/<environment>.json` path; it accepts no remote fields.
The PostgreSQL configuration names an environment variable containing the DSN
instead of embedding the DSN in parsed configuration. `dsn_env` must be a valid
environment-variable name. Unknown fields and provider-shape mismatches fail
strict validation.

State backend selection belongs to effective environment configuration and
cannot be overridden by `plan`, `apply`, or `adopt` flags. This prevents an
operator from accidentally planning against one authority and applying against
another. The reviewed state reference also detects endpoint changes through the
remote store ID.

Initial administrative commands are:

```text
streamt state status -p PATH -e ENV
streamt state init -p PATH -e ENV
streamt state migrate -p PATH -e ENV --from local --to configured
streamt state recover -p PATH -e ENV --operation OPERATION_ID ...
streamt state export -p PATH -e ENV --out FILE
```

`status` is read-only and reports backend kind, store ID, address, serial,
checksum, lock availability, and operation status without credentials. `init`,
`migrate`, and `recover` require exact environment and state-address
confirmation in non-interactive use. Normal commands never initialize or
migrate a remote store implicitly.

An optional `safety.require_remote_state` policy should land with the backend.
It fails mutating commands before runtime deployers are constructed when the
effective backend is local. It is opt-in during the alpha compatibility window;
making it implicit for protected environments requires a documented config
migration and release notice.

## Credentials and redaction

- Read the DSN only from the named environment variable at provider
  construction time. Do not place it in a Pydantic model dump or environment
  fingerprint.
- Never expose usernames, passwords, hosts, query parameters, lock tokens,
  database exception detail, SQL text, or provider response bodies by default.
- Map provider failures to stable sanitized errors. Detailed diagnostics, when
  explicitly enabled, still pass through central key, URL, authorization, and
  inline-value redaction.
- Plan fingerprints include backend kind, namespace, state address, and remote
  `store_id`, not credentials.
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
| Release fails after a verified commit | Report the release error without undoing or repeating the commit |

## Observability

Structured command output includes the safe state reference, operation ID,
wait duration, and final operation status. It never includes provider revision
tokens or lock handles. Metrics may count acquisition latency, conflicts,
recovery-required operations, and backend failures using backend kind and
environment, but not project/resource names unless explicitly allowed.

Audit history records who initiated an operation only as a caller-supplied,
redacted label. Authentication and durable actor identity belong to the
database or CI platform; streamt must not claim that an arbitrary label is a
verified identity.
