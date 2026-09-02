# Remote state and distributed locking execution plan

## Objective

Add a production-usable PostgreSQL ownership-state backend without weakening
the existing local path or allowing concurrent runners to mutate infrastructure
from the same prior snapshot. The normative boundary is
`docs/specs/remote-deployment-state.md`.

## Status — 2026-09-02

Slices 1 through 8 are complete. The delivered PostgreSQL boundary includes
explicit v1 store/address initialization, strict v1/v2 status, non-reserving
lock diagnostics, explicit reviewed recovery, the atomic mutation backend, and
the confirmation-gated v1-to-v2 migration with an exact least-privilege writer.
PostgreSQL 14/18 real-server, ACL, commit-ambiguity, lifecycle,
process-concurrency, production-factory command, and installed-wheel gates
pass. Ordinary `plan`, `apply`, and `adopt` use only the exact v2 writer.

The completed prerequisite and final enablement checklist is
`docs/plans/2026-09-02-postgres-slice5-foundation.md`. Factory enablement was
the last implementation boundary, after command E2E/failure gates and the
minimum recovery workflow, rather than a private-backend or migration
milestone.

## Current implementation audit

The current local implementation has a sound single-host commit primitive:

- `LocalState` strictly validates version, project, environment, serial, stable
  resource identities, record fields, and duplicate JSON keys.
- State is isolated at `.streamt/state/<environment>.json`.
- `save()` writes, flushes, fsyncs, and atomically replaces the target.
- `apply` and `adopt` take an adjacent environment `flock` before their final
  state read and retain it through live observation and ownership persistence;
  apply also retains it through runtime mutation and rollback.
- `save_if_serial()` uses that same typed lock boundary for standalone CAS
  writes, while an already-held operation lock can perform the CAS without
  reacquiring and deadlocking.
- `plan`, `apply`, and `adopt` load the same environment-scoped state.
- Reviewed-plan format version 3 binds to the exact backend store, canonical
  address, state serial, and canonical state checksum; apply verifies that
  reference under the operation lock before replanning live actions.
- Failed applies and completed rollback paths do not advance state.
- Targeted apply retains records outside the selection, and absence never
  deletes a record.

The remaining gaps are material:

- Local apply and adoption now persist durable operation intent and progress,
  and uncertain completion leaves a marker that blocks later mutation. There
  is not yet an operator recovery command to resolve that marker.
- Local adoption holds the operation lock while interactive confirmation is
  pending. This preserves authoritative evidence but can intentionally block a
  second same-host mutator until the prompt completes.
- The file lock and control sidecar coordinate only one host. Local warnings
  truthfully say shared CI is unsupported. A session-affine PostgreSQL lock and
  mutation provider now exist privately, but the ordinary factory remains
  disabled until Slices 6 through 8 pass.

## Non-goals for the first implementation

- Enabling deletion, bulk adoption, or Flink update workflows.
- Supporting S3 lock files, Redis locks, Kubernetes Leases, or generic HTTP
  state servers.
- Automatically recovering an interrupted apply.
- Claiming exactly-once mutation across independent runtime APIs.
- Replacing the existing ownership resource schema or local JSON layout.
- Making remote state implicit for every existing protected environment in the
  first compatibility release.

## Delivery order

Each slice is independently reviewable and keeps mutating remote state disabled
until the full protocol is present.

### Slice 1: characterize and extract the backend boundary

Progress: implemented. `plan`, `apply`, and `adopt` now access ownership state
through a provider-neutral service using strict `StateAddress`,
`StateStoreIdentity`, `StateObservation`, and opaque `StateRevision` values.
The local provider retains the version 1 JSON bytes, paths, file mode, warning,
serial rules, and operation-wide lock lifetime. Offline plan bypasses backend
construction, and no remote provider is selectable. The operation-intent
methods in the normative full protocol remain intentionally deferred to Slice
3; the current extracted mutation surface is an exclusive operation with a
revision-and-serial CAS.

1. Add characterization tests for every local load/save/CAS/error behavior,
   file mode, duplicate-key rejection, atomic-replace cleanup, environment
   isolation, and warning.
2. Introduce `StateAddress`, `StateStoreIdentity`, `StateObservation`,
   `StateRevision`, stable backend errors, and the `DeploymentStateBackend`
   protocol.
3. Implement `LocalDeploymentStateBackend` as a wrapper over current version 1
   JSON. Keep `LocalState.load`, `save`, and `save_if_serial` as compatibility
   delegates until callers migrate.
4. Add a state service/factory and route `plan` read-only state access through
   it. Offline plan must not call the factory.
5. Route apply and adopt state reads and final local CAS through the service
   without changing lock lifetime yet.

Acceptance:

- Current local files are byte-for-byte compatible after a round trip, apart
  from already non-semantic formatting behavior covered by existing tests.
- Current CLI JSON fields, warning code, state serial behavior, and error codes
  remain stable.
- A fake backend can exercise commands without filesystem patching.
- No remote backend can be selected yet.

### Slice 2: bind reviewed plans to exact state

Progress: implemented. Online format-version 3 plans contain a strict exact
state reference derived from the `StateObservation` used by planning. Apply
rechecks backend kind, immutable store ID, address, serial, and checksum after
acquiring the operation lock and again on the live-plan verification path.
Format versions 1 and 2 require explicit regeneration. Offline plans encode
`state: null`, cannot authorize apply, and do not construct a state backend.
Provider revisions and credential-shaped provider data are excluded.

1. Add canonical `state_checksum(LocalState)` using the same strict canonical
   JSON rules as persistence.
2. Add backend kind, immutable store ID, canonical address, serial, and checksum
   to the reviewed plan.
3. Bump the reviewed-plan format version. Reject the previous format with a
   precise migration message.
4. Verify the state reference after lock acquisition and again through the
   current-plan comparison path.
5. Ensure offline plans use no state reference and cannot authorize apply.

Acceptance:

- Same serial with different resource content is stale.
- Same state in another backend instance is stale.
- Changing only credentials for the same store does not alter plan contents.
- Provider revisions and credential-shaped data never appear in plan JSON.

### Slice 3: exclusive local operation protocol

This slice closes the existing same-host race before adding remote state.

Progress: steps 1-6 are implemented for the local backend. Apply and adoption
write a strict, atomically replaced control sidecar under the existing
operation-wide lock; apply records ordered started/completed boundaries for all
five resource families and checks lock health around apply and rollback calls.
The version 1 ownership JSON is unchanged. Crash/failure tests prove that a
post-mutation exit and an uncertain ownership commit leave a blocking marker.
This is durable local operation detection, not distributed locking or a
completed recovery workflow: remote configuration, PostgreSQL, and the
operator recovery command remain later slices.

1. Hold the local address lock from the final state read through live replan,
   runtime apply/adoption commit, and state persistence.
2. Add provider-neutral operation intent and recovery-record models.
3. Persist intent before any runtime mutation. For local state, use a strict,
   atomically replaced `<environment>.control.json` sidecar protected by the
   same existing lock while leaving `<environment>.json` version 1 unchanged.
4. Check lock health before every planner apply step.
5. Clear intent only for verified no-mutation failure or verified success.
   Persist `recovery_required` for unknown results or incomplete rollback.
6. Make `plan` expose recovery status but remain read-only. Make `apply` and
   `adopt` reject an active/recovery-required operation.

Acceptance:

- Two process-level applies from serial N cannot both start backend mutation.
- Killing an apply after the first mocked mutation leaves a marker that blocks
  the next apply.
- Failure before the first mutation clears intent and keeps serial N.
- Success writes state before clearing intent under the same lock. A crash
  between those writes leaves a conservative marker that recovery can verify;
  it never leaves a cleared marker with uncommitted ownership state.
- Existing version 1 ownership JSON remains readable by the prior release; the
  new control sidecar is ignored by it, so release notes must prohibit version
  rollback while an operation marker exists.

### Slice 4A: strict configuration and safe administrative CLI

Progress: complete in `2b75090`, building on the read-only local `state status`
command in `0e04112`. The strict tagged local/PostgreSQL configuration
boundary, whole-block environment replacement, environment-only remote-state
policy, central redaction coverage, and safe administrative command boundary
are implemented. Local remains the only ordinary state provider. PostgreSQL
ordinary selection deliberately returns a sanitized unavailable error with no
local fallback. At that boundary, `state init` and lock-availability probing
were reserved for Slice 4B.

1. Add strict `deployment_state` models to base and environment configuration.
2. Support `local` and `postgres` tagged shapes. Local is the default. Reject
   mixed provider fields and unknown keys.
3. Resolve only the configured DSN environment-variable name at construction.
4. Add central redaction tests for provider exceptions, credential URLs,
   authorization text, and structured nested values.
5. Add `streamt state status`; it is read-only. Reserve `state init` and lock
   probing for a separately gated PostgreSQL administrative adapter.
6. Add opt-in `safety.require_remote_state`, evaluated before constructing
   runtime deployers.

Acceptance:

- Missing DSN, malformed provider config, or unavailable remote state fails
  before runtime observation or mutation and never reads local state.
- Command-line flags cannot switch the state backend.
- JSON/text output contains no DSN, password, host, lock token, SQL, or raw
  provider exception.
- Strict docs/schema tests cover the new fields.

### Slice 4B: PostgreSQL initialization and diagnostics

Progress: read-only status landed in `81c97fc`, explicit initialization in
`dacb74b`, and PostgreSQL 14/18 conformance CI in `e620d42`. The optional
Psycopg dependency, strict version-1 schema/owner/ACL contract, bounded
read-only `state status` snapshot, confirmation-gated `state init`,
TLS/endpoint policy, size caps, secret-neutral failure translation, no-fallback
CLI wiring, and separate-process initialization convergence are implemented.
The separate `state lock-status` diagnostic completes Slice 4B with full
catalog validation and a primary-only, instantaneous, transaction-scoped probe.
Ordinary PostgreSQL authority stays disabled.

This slice makes a configured PostgreSQL store administratively inspectable;
it does not make PostgreSQL the state authority for normal commands. Keep
`make_deployment_state_service()` and the online `plan`, `apply`, and `adopt`
paths on their current sanitized unavailable result for `backend: postgres`.
Use separate, narrow administrative factories from `state init`, `state
status`, and `state lock-status` so an incomplete mutation backend cannot be
selected accidentally.

1. Add a lazy optional Psycopg 3 dependency and an administrative adapter under
   `src/streamt/deployer/`. A base installation must still import and operate
   local state without the extra. Missing driver, DSN, connection, and TLS
   failures map to stable, secret-neutral errors.
2. Define schema version 1 with immutable store metadata, an applied-schema
   migration ledger, collision-checked address-to-advisory-lock mapping,
   current ownership/control rows, and append-only state/operation history.
   Validate the configured schema identifier separately, compose identifiers
   with Psycopg SQL objects, parameterize every value, and persist ownership
   payloads as canonical JSON text rather than database-normalized JSON. The
   exact catalog requires one common schema/table owner, rejects every `PUBLIC`
   ACL, and permits named status roles only non-grantable schema `USAGE` and
   non-grantable table or column `SELECT`.
3. Add explicit `streamt state init` for PostgreSQL only. Require exact parsed
   project, effective environment, and canonical address confirmations. Before
   starting the serializable transaction, take a bounded schema-scoped session
   advisory lock so a concurrent waiter begins from a fresh post-lock snapshot.
   Require an absent schema, an empty schema owned by the init identity, or an
   exactly compatible version-1 store. Create one random immutable `store_id`,
   register the current address and collision-checked lock-key mapping, and
   leave ownership absent with clear operation control. Do not import local
   state, migrate populated stores, create roles, or grant privileges.
4. Make initialization exactly idempotent for the same compatible store and
   empty address. Permit an exact compatible store to register a previously
   unregistered address. Reject a nonempty unknown schema, partial installation,
   populated or active target address, incompatible schema version,
   owner/ACL drift, store/address mismatch, and hash-key collision without
   repairing or adopting them. Precommit-validate the complete catalog and
   rows. After commit, open a fresh connection and verify store ID, version,
   address, absent ownership, and clear control state. Never retry an ambiguous
   commit; return a stable unknown-outcome error and direct the operator to
   rerun status/init, whose identity checks make the retry safe.
5. Extend `state status` through the administrative adapter. Read metadata,
   current ownership, and operation control in one bounded, read-only,
   repeatable-read transaction and report only safe fields. Distinguish
   `uninitialized` from `ready`; fail closed on partial or incompatible stores.
6. Add explicit `streamt state lock-status`. Validate the complete version-1
   catalog and require a primary server in an explicit repeatable-read,
   read-only transaction. For a registered address, call
   `pg_try_advisory_xact_lock(bigint)` exactly once; for an unregistered address,
   do not touch the advisory lock. Return `available`, `busy`, or `unregistered`
   as successful diagnostic outcomes. Require rollback to succeed before
   returning so the transaction-scoped lock is released, and report
   `reservation: none`. The full read validates operation-control rows, but the
   command must not report, clear, or interpret durable operation control as
   mutation safety; operators use `state status` to view it. Never imply that
   this instantaneous, racy observation authorizes later mutation.
7. Set connection, statement, and lock timeouts; enforce TLS policy and the
   state-read size cap; set transaction-local `search_path` to `pg_catalog`;
   qualify state objects explicitly; and close/rollback on every failure path.
   Revoke all `PUBLIC` access from every created table and from a newly created
   schema. Redact DSNs, usernames, passwords, hosts, query parameters, SQL,
   schema names, server messages, and raw driver exceptions from text, JSON,
   logs, and chained command errors.

Acceptance:

- Initializing an empty schema twice returns the same store ID and creates no
  duplicate metadata, address, control, or history rows. Concurrent
  initialization produces one compatible store, not two authorities.
- The same valid version-1 store/address is an idempotent no-op; a new address
  can be registered only with absent ownership and clear control. A partial or
  unknown populated schema, populated/active address, unsupported version,
  owner mismatch, or forbidden ACL fails without mutation.
- A simulated connection loss before commit leaves no store. A loss during or
  after commit is reported as unknown, is never retried automatically, and a
  fresh diagnostic/init invocation resolves the actual durable result.
- A poisoned connection `search_path` cannot shadow catalog functions. Every
  table shares its schema owner; `PUBLIC` has no access; a named status reader
  works with non-grantable `USAGE`/`SELECT`; mutating or grantable non-owner ACL
  and owner drift fail closed.
- Status performs no writes. Its snapshot cannot combine metadata, ownership,
  and control from different commits, and oversized or malformed state fails
  closed without returning resource content.
- Lock probing covers available, busy, unregistered, and terminated-session
  behavior in separate-process tests. It never creates an address row or
  operation marker, never invokes an advisory lock for an unregistered address,
  and returns no result unless rollback releases any acquired transaction lock.
- Lock diagnostics require a direct, session-affine primary endpoint.
  Transaction- and statement-pooling endpoints are unsupported: PostgreSQL
  advisory locks are physical-session and reentrant state, and Slice 5 operation
  locking must preserve that affinity for the complete operation.
- Secret-shaped DSNs, driver messages, SQL, identifiers, and nested exception
  values do not appear in text or structured errors.
- CI exercises the minimum and current supported PostgreSQL majors, while a
  clean package without the optional extra continues to pass local workflows.
- Online `plan`, `apply`, and `adopt` with `backend: postgres` still fail before
  runtime observation or mutation and never fall back to local state.

Deferred from Slice 4B: ordinary PostgreSQL state reads/CAS, operation-lock
ownership for mutating commands, operation transitions, apply/adopt selection,
local-to-remote migration, recovery, export, automatic schema upgrade, role
granting, force unlock, and destructive administration.

Implemented surface: the optional dependency metadata, focused
`src/streamt/deployer/postgres_state.py` administrative adapter, narrow wiring
in `src/streamt/cli/commands/state_cmd.py`, unit tests for SQL composition and
failure translation, CLI init/status tests, process-level PostgreSQL
integration tests, the `state lock-status` CLI and probe tests, and the
PostgreSQL CI service matrix. Do not alter the ordinary state-service factory
beyond tests that prove PostgreSQL selection remains disabled.

### Slice 5: PostgreSQL mutation backend

This slice is complete. It builds a private, conforming mutation backend and
the explicit schema-v2 administrative migration without enabling normal
provider selection. Version-1 owner mutation remains test scaffolding;
version-2 conformance runs through the exact writer role. The detailed contract
and remaining enablement checklist are in
`docs/plans/2026-09-02-postgres-slice5-foundation.md`.

1. Add a consistent `OperationSnapshot` containing state and control
   observations with independent opaque revisions; derive store/address
   identity from the contained state observation and require matching control.
   Use the additive `observe` and `clear_before_mutation` operation names.
2. Introduce typed planned actions whose durable `resource_id` string is the
   canonical logical
   `streamt://<project>/<environment>/<kind>/<logical-name>` URI.
   Provider/runtime labels remain separate and never stand in for identity in
   intent, progress, history, or recovery. Reject duplicates and cross-address
   values while constructing the complete planner/operation action set.
3. Migrate local apply and adoption through the provider-neutral snapshot and
   canonical-action protocol, including final rereads, post-confirmation target
   observation, `clear_before_mutation`, recovery marking, commit, and
   release-before-success. Preserve local compatibility and finish this before
   implementing any private PostgreSQL operation path.
4. Implement bounded session advisory-lock acquisition on one direct,
   session-affine primary connection. Store and validate the full
   address-to-lock-key mapping and check connection, primary, and lock ownership
   before every external action and state transition.
5. Implement ordered progress and recovery transitions. Make
   `begin_operation` compare the operation lock, state revision, serial,
   checksum, control revision, and clear-marker state before atomically writing
   intent and its history event. Implement
   `commit_operation` so ownership replacement when needed, control clearing,
   state history, and operation history commit in one database transaction,
   followed by a fresh direct verification read.
6. Preserve the Slice 4B timeout, size, TLS, transaction, unknown-outcome, and
   redaction rules on every mutation path. Do not automatically retry an
   ambiguous commit.
7. Define an explicit schema-version-2 migration and exact non-grantable
   least-privilege ordinary writer role. Version 1 remains the frozen
   administrative contract and is never considered production mutation-ready;
   do not silently weaken its catalog validator.
8. Document and test the topology boundary: one direct standalone primary is
   required for ordinary operation. Every pooler, proxy, replica, promotion,
   failover, and synchronous or asynchronous HA topology is unsupported;
   pooler absence is an operator-verified prerequisite because same-session
   multiplexing is not reliably detectable.
9. Keep `make_deployment_state_service()` and all normal PostgreSQL
   `plan`/`apply`/`adopt` selection on the current sanitized unavailable result.

Acceptance:

- PostgreSQL integration tests run against every supported major version in CI
  or against the minimum and current major with a documented support policy.
- Twenty concurrent contenders yield one operation owner and no unchecked
  state overwrite.
- Terminating the lock session releases the advisory lock but leaves the
  prewritten operation marker, blocking the successor.
- Snapshot/CAS tests cover consistent state/control reads, absent create,
  update, stale state or control revision, stale serial/checksum, same-serial
  metadata transition, atomic finalization, final verification, and unknown
  commit outcome.
- Canonical logical identities remain stable when runtime/display names differ.
- Private owner-only conformance tests pass, then the same suite passes through
  the exact version-2 least-privilege writer role. Missing, extra, wrong-level,
  wrong-grantor, grantable, default, owner, or `PUBLIC` privileges fail closed.
- Standby, transaction-pooling, statement-pooling, and asynchronously promoted
  endpoints cannot be presented as a supported HA operation path.
- An incompatible database schema version fails closed.
- Online `plan`, `apply`, and `adopt` with `backend: postgres` still return the
  sanitized unavailable result and never fall back to local state.

### Slice 6: apply/adopt integration and failure injection

After Slice 5's private backend and schema-version-2 role contract are complete,
this slice exercises the already provider-neutral commands through an injected
private PostgreSQL backend while normal factory selection remains disabled.

1. Exercise the complete apply protocol through acquire, initial snapshot, live
   replan, final direct snapshot reread, begin, mutate/progress, atomic commit/
   recovery transition, final verification, and release.
2. For both direct and reviewed apply, reject any state or control drift found
   by the final post-plan reread and use only that final snapshot as
   `begin_operation` CAS authority. Reviewed apply also preserves the complete
   reviewed-plan comparison.
3. Persist ordered canonical action identities in intent/progress/history while
   keeping runtime/display labels separate.
4. Re-observe the exact adoption target after confirmation, compare its exact
   fingerprint with the confirmed observation, then perform the state-only
   operation. Stale confirmation never authorizes ownership.
5. Release the operation lock before emitting or flushing final success. A
   release failure after verified commit reports a distinct sanitized
   committed outcome and never advises replaying the mutation.
6. Give lock timeout, lock loss, state conflict, unknown outcome, release
   failure after commit, and recovery-required distinct stable error kinds and
   structured results.
7. Add deterministic failure injection at every boundary: acquire, initial
   read, runtime observation, live plan, final reread, intent, each runtime
   action and progress transition, rollback, recovery marking or
   clear-before-mutation, commit before/during/after acknowledgement,
   verification read, and release.
8. Preserve reviewed-plan, ownership, safety-blocker, confirmation, and
   protected-environment ordering guarantees.

Acceptance:

- No failure before `begin_operation` calls a runtime mutation.
- No lock-loss path starts a subsequent mutation.
- Any possibly successful runtime action plus incomplete finalization leaves
  `recovery_required` or an existing `in_progress` marker.
- Apply never reports success until the atomic ownership/control/history commit
  is freshly verified and the operation lock is released.
- A verified commit followed by release failure is distinguishable from an
  uncommitted failure and does not invite an apply retry.
- Direct and reviewed apply both reject final-reread drift before mutation.
- Adoption never mutates runtime infrastructure and cannot write from either a
  stale state snapshot or a stale pre-confirmation target observation.
- The full command E2E and failure-injection matrix passes while the public
  factory still rejects ordinary PostgreSQL selection.

### Slice 7: migration, recovery, and operations documentation

Minimum explicit recovery is a factory-enablement prerequisite. Export and
local-to-remote state migration can follow without delaying that safety gate;
the separate database schema-v2 administrative migration is already delivered.

1. First add `state recover` for `observed`, `rolled-back`, and
   `abandoned-before-mutation`. Require a fresh reviewed observation, exact
   operation ID, exact confirmation, monotonic CAS, and append-only audit.
2. Prohibit `abandoned-before-mutation` when durable progress says any runtime
   action started. Recovery never guesses from age and never automatically
   clears a marker.
3. Pass local and PostgreSQL command-level recovery E2E and failure injection.
   Keep the ordinary PostgreSQL factory disabled until this minimum is shipped.
4. Add read-only `state export` with atomic restrictive file creation.
5. Add local-to-configured `state migrate` with dual locks, destination-absent
   precondition, exact confirmation, copy/read-back verification, provenance,
   and no source deletion.
6. Add append-only history queries to `state status` without exposing resource
   content by default.
7. Document backup, schema-version-2 migration, exact role grants, direct
   standalone-primary/session-affinity, the unsupported pooler/HA boundary,
   monitoring, incident response, version rollback, and disaster recovery.

Acceptance:

- All three explicit recovery resolutions reject wrong operation IDs and stale
  evidence, preserve history, and never lower ownership serial.
- A retained `in_progress` marker after lock-session loss blocks a successor
  until an explicit recovery resolution succeeds.
- Migration is idempotent only for an identical completed copy and rejects
  every divergent populated destination.
- Interrupted migration leaves neither an authoritative partial destination nor
  a deleted source.
- A retained local backup is never selected after configuration points remote.

### Slice 8: release gate and compatibility rollout — complete

1. Run the final checklist in
   `docs/plans/2026-09-02-postgres-slice5-foundation.md`, including schema v2,
   least-privilege writer, command E2E/failure injection, minimum recovery,
   direct standalone-primary/session-affinity evidence and the explicit
   unsupported boundary for every pooler and HA/failover topology.
2. Run the full unit, scenario, packaging, strict documentation, Ruff, and mypy
   gates with both local-only and PostgreSQL-extra installations.
3. Add or retain process/concurrency and supported-major PostgreSQL service jobs
   in CI. Test the base installation without Psycopg independently.
4. Only in the final implementation commit, enable the ordinary PostgreSQL
   factory for an exactly compatible version-2 store and writer role. That
   commit must not add local/empty fallback, automatic schema upgrade, automatic
   operation retry, force unlock, or automatic recovery.
5. Update public configuration, support, operations, recovery, migration,
   backup, and release documentation at the same release boundary. Do not
   publish success or support claims in an earlier commit.
6. Keep remote state opt-in for one alpha compatibility window. Emit a warning
   when a protected/shared mutating workflow still uses local state.
7. After migration documentation and telemetry are validated, propose making
   remote state mandatory for protected environments in a separately announced
   configuration migration.

## Test matrix

| Area | Required coverage |
| --- | --- |
| Parsing | Tagged provider shapes, unknown keys, booleans/integers, env-var name validation, no embedded DSN |
| Identity | Namespace/project/environment isolation, wrong store ID, wrong address, canonical URI validation |
| Integrity | Duplicate fields, unsupported versions, invalid checksums, same-serial different-content drift |
| CAS | Consistent state/control snapshot, absent create, exact update, stale state/control revision, stale serial/checksum, atomic finalization, unknown outcome |
| Locking | Timeout, wait, process contention, session termination, primary loss, session affinity, lock health, release failure |
| Operations | Intent before mutation, canonical action identity, progress ordering, atomic ownership/control/history commit, final verification, recovery-required preservation |
| Apply | Direct/reviewed final reread, reviewed-plan order, live/state/control drift, partial failure, rollback success/failure, state commit failure, release before success |
| Adoption | Confirmation, post-confirm exact-target re-observation/fingerprint, final state reread, idempotency, conflicting claim, no runtime writes |
| Migration | Empty destination, identical retry, divergent destination, interruption, read-back mismatch |
| Recovery | Each explicit resolution, wrong operation ID, stale evidence, append-only history |
| Schema/roles | Private owner-only v1 tests, explicit v2 migration, exact writer ACL, missing/extra/grantable/`PUBLIC` privilege rejection |
| Topology/HA | Direct standalone primary; observable standby/session switching rejection; all poolers and all synchronous/asynchronous HA/failover topologies are explicit operator-enforced unsupported preconditions |
| Security | DSN/provider-error redaction in text, JSON, plans, logs, exceptions, and metrics |
| Compatibility | Existing local files, local CLI outputs/warnings, offline no-read behavior, old plan rejection |

Concurrency and crash tests must use separate processes, not only threads or
mocks. Provider integration tests must include connection termination during an
external-action pause and during transaction commit.

## Commit strategy

Commit each slice separately after its focused tests and the relevant full
gates pass. Do not combine provider implementation, CLI config, and apply
integration in one change. Remote selection remains unreachable through Slice
6 E2E/failure injection and the minimum Slice 7 recovery workflow. The final
implementation commit may enable the ordinary factory only after the complete
checklist in `docs/plans/2026-09-02-postgres-slice5-foundation.md`, including
schema version 2 and its exact least-privilege writer role, passes. Do not enable
destructive removal merely because remote state exists; that requires its own
reviewed workflow and safety tests.

## Suggested follow-up edits when implementation lands

- Mark remote state and locking progress in `ROADMAP.md` only after the
  corresponding slice is executable.
- Replace the local-only warning language in the CLI, deployment-safety spec,
  stabilization plan, support matrix, CI guide, and configuration reference
  only for configurations that actually select a conforming remote backend.
- Add PostgreSQL to the support matrix as a deployment-state integration, not a
  streaming execution backend.
- Keep S3/object-store state listed as unsupported until it meets the same CAS,
  lock, fencing, and recovery contract.
