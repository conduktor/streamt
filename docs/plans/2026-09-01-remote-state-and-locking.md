# Remote state and distributed locking execution plan

## Objective

Add a production-usable PostgreSQL ownership-state backend without weakening
the existing local path or allowing concurrent runners to mutate infrastructure
from the same prior snapshot. The normative boundary is
`docs/specs/remote-deployment-state.md`.

## Status — 2026-09-01

Slices 1 through 3 and the Slice 4A configuration/policy boundary are
complete. Slice 4A landed in `2b75090`, building on the read-only local status
command from `0e04112`. Slice 4B is the next boundary: explicit PostgreSQL
store initialization and read-only diagnostics. Normal PostgreSQL state reads
and mutations for `plan`, `apply`, and `adopt` remain deliberately disabled
until the full backend and operation protocol pass later acceptance gates.

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
  truthfully say shared CI is unsupported; no remote provider or distributed
  operation lock exists.

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
are implemented. Local remains the only working provider. PostgreSQL
configuration deliberately returns a sanitized unavailable error with no local
fallback. `state init` and lock-availability probing remain Slice 4B work.

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

This slice makes a configured PostgreSQL store administratively inspectable;
it does not make PostgreSQL the state authority for normal commands. Keep
`make_deployment_state_service()` and the online `plan`, `apply`, and `adopt`
paths on their current sanitized unavailable result for `backend: postgres`.
Use a separate, narrow administrative factory from `state init` and
`state status` so an incomplete mutation backend cannot be selected
accidentally.

1. Add a lazy optional Psycopg 3 dependency and an administrative adapter under
   `src/streamt/deployer/`. A base installation must still import and operate
   local state without the extra. Missing driver, DSN, connection, and TLS
   failures map to stable, secret-neutral errors.
2. Define schema version 1 with immutable store metadata, an applied-schema
   migration ledger, collision-checked address-to-advisory-lock mapping,
   current ownership/control rows, and append-only state/operation history.
   Validate the configured schema identifier separately, compose identifiers
   with Psycopg SQL objects, parameterize every value, and persist ownership
   payloads as canonical JSON text rather than database-normalized JSON.
3. Add explicit `streamt state init` for PostgreSQL only. Require the exact
   canonical address and environment confirmation. In one serializable
   transaction, take the store initialization lock, require an empty schema or
   an exactly compatible version-1 store, create one random immutable
   `store_id`, register the current address and lock-key mapping, and leave
   ownership absent with no active operation. Do not import local state,
   migrate populated stores, or grant roles implicitly.
4. Make initialization exactly idempotent for the same compatible store and
   address. Reject a nonempty unowned schema, partial installation,
   incompatible schema version, store/address mismatch, and hash-key collision
   without repairing or adopting them. After commit, open a fresh connection
   and verify store ID, version, address, and empty control state. Never retry
   an ambiguous commit; return a stable unknown-outcome error and direct the
   operator to rerun status/init, whose identity checks make the retry safe.
5. Extend `state status` through the administrative adapter. Read metadata,
   current ownership, and operation control in one bounded, read-only,
   repeatable-read transaction and report only safe fields. Distinguish
   `uninitialized` from `ready`; fail closed on partial or incompatible stores.
6. Add an explicit status lock probe that uses an immediate PostgreSQL session
   advisory try-lock and releases it on the same session. Report only
   `available`, `busy`, or `unregistered`; do not expose an owner, token,
   backend key, connection detail, or imply that the racy probe reserves the
   lock for a later command.
7. Set connection, statement, and lock timeouts; enforce TLS policy and the
   state-read size cap; close/rollback on every failure path. Redact DSNs,
   usernames, passwords, hosts, query parameters, SQL, schema names, server
   messages, and raw driver exceptions from text, JSON, logs, and chained
   command errors.

Acceptance:

- Initializing an empty schema twice returns the same store ID and creates no
  duplicate metadata, address, control, or history rows. Concurrent
  initialization produces one compatible store, not two authorities.
- A valid existing version-1 store is an idempotent no-op; a partial, populated
  unknown, or unsupported-version schema fails without mutation.
- A simulated connection loss before commit leaves no store. A loss during or
  after commit is reported as unknown, is never retried automatically, and a
  fresh diagnostic/init invocation resolves the actual durable result.
- Status performs no writes. Its snapshot cannot combine metadata, ownership,
  and control from different commits, and oversized or malformed state fails
  closed without returning resource content.
- Lock probing covers available, busy, unregistered, and terminated-session
  behavior in separate-process tests. It never creates an address row or
  operation marker.
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

Expected implementation surface: the optional dependency metadata, a focused
`src/streamt/deployer/postgres_state.py` administrative adapter, narrow wiring
in `src/streamt/cli/commands/state_cmd.py`, unit tests for SQL composition and
failure translation, CLI init/status tests, process-level PostgreSQL
integration tests, and the PostgreSQL CI service matrix. Do not alter the
ordinary state-service factory beyond tests that prove PostgreSQL selection
remains disabled.

### Slice 5: PostgreSQL mutation backend

This slice consumes the initialized version-1 store from Slice 4B and completes
the mutation protocol before normal provider selection is enabled.

1. Implement consistent ordinary reads and atomic CAS transitions with
   parameterized SQL over the Slice 4B schema.
2. Implement bounded session advisory-lock acquisition. Store and validate the
   full address-to-lock-key mapping.
3. Implement begin/progress/commit/fail operation transitions and append-only
   history in the same transactional authority as ownership state.
4. Check connection and lock ownership before every external action and state
   transition.
5. Preserve the Slice 4B timeout, size, TLS, transaction, idempotency,
   unknown-commit, and redaction rules on every mutation path.
6. Enable the normal PostgreSQL state-service factory only after all backend
   operation and concurrency acceptance tests pass; selection must remain
   unreachable before then.

Acceptance:

- PostgreSQL integration tests run against every supported major version in CI
  or against the minimum and current major with a documented support policy.
- Twenty concurrent contenders yield one operation owner and no unchecked
  state overwrite.
- Terminating the lock session releases the advisory lock but leaves the
  prewritten operation marker, blocking the successor.
- CAS tests cover absent create, update, stale revision, stale serial,
  same-serial metadata transition, and unknown commit outcome.
- An incompatible database schema version fails closed.

### Slice 6: apply/adopt integration and failure injection

1. Route the complete apply protocol through acquire, re-read, live replan,
   begin, mutate, commit/fail, and release.
2. Re-observe adoption targets under the lock after confirmation and before the
   ownership write.
3. Include safe state and operation metadata in structured outputs.
4. Add deterministic failure injection at every boundary: lock, read, intent,
   each runtime action, rollback, commit, verification read, and release.
5. Preserve reviewed-plan, ownership, safety-blocker, confirmation, and
   protected-environment ordering guarantees.

Acceptance:

- No failure before `begin_operation` calls a runtime mutation.
- No lock-loss path starts a subsequent mutation.
- Any possibly successful runtime action plus incomplete finalization leaves
  `recovery_required` or an existing `in_progress` marker.
- Apply never reports success until the ownership commit is verified.
- Adoption never mutates runtime infrastructure and cannot write from a stale
  observation.

### Slice 7: migration, recovery, and operations documentation

1. Add read-only `state export` with atomic restrictive file creation.
2. Add local-to-configured `state migrate` with dual locks, destination-absent
   precondition, exact confirmation, copy/read-back verification, provenance,
   and no source deletion.
3. Add `state recover` for the three specified explicit outcomes. Require a
   fresh reviewed observation and operation ID.
4. Add append-only history queries to `state status` without exposing resource
   content by default.
5. Document backup, database migrations, role grants, monitoring, incident
   response, version rollback, and disaster recovery.

Acceptance:

- Migration is idempotent only for an identical completed copy and rejects
  every divergent populated destination.
- Interrupted migration leaves neither an authoritative partial destination nor
  a deleted source.
- Recovery never lowers state serial or overwrites without CAS.
- A retained local backup is never selected after configuration points remote.

### Slice 8: release gate and compatibility rollout

1. Run the full unit, scenario, packaging, strict documentation, Ruff, and mypy
   gates with both local-only and PostgreSQL-extra installations.
2. Add a process/concurrency test job and PostgreSQL service job to CI.
3. Keep remote state opt-in for one alpha compatibility window. Emit a warning
   when a protected/shared mutating workflow still uses local state.
4. After migration documentation and telemetry are validated, propose making
   remote state mandatory for protected environments in a separately announced
   configuration migration.

## Test matrix

| Area | Required coverage |
| --- | --- |
| Parsing | Tagged provider shapes, unknown keys, booleans/integers, env-var name validation, no embedded DSN |
| Identity | Namespace/project/environment isolation, wrong store ID, wrong address, canonical URI validation |
| Integrity | Duplicate fields, unsupported versions, invalid checksums, same-serial different-content drift |
| CAS | Absent create, exact update, stale revision, stale serial, monotonic serial, unknown outcome |
| Locking | Timeout, wait, process contention, session termination, lock health, release failure |
| Operations | Intent before mutation, progress ordering, verified clear, recovery-required preservation |
| Apply | Reviewed-plan order, live drift, partial failure, rollback success/failure, state commit failure |
| Adoption | Confirmation, under-lock re-observation, idempotency, conflicting claim, no runtime writes |
| Migration | Empty destination, identical retry, divergent destination, interruption, read-back mismatch |
| Recovery | Each explicit resolution, wrong operation ID, stale evidence, append-only history |
| Security | DSN/provider-error redaction in text, JSON, plans, logs, exceptions, and metrics |
| Compatibility | Existing local files, local CLI outputs/warnings, offline no-read behavior, old plan rejection |

Concurrency and crash tests must use separate processes, not only threads or
mocks. Provider integration tests must include connection termination during an
external-action pause and during transaction commit.

## Commit strategy

Commit each slice separately after its focused tests and the relevant full
gates pass. Do not combine provider implementation, CLI config, and apply
integration in one change. Remote selection remains unreachable until the
backend and operation protocol are both complete. Do not enable destructive
removal merely because remote state exists; that requires its own reviewed
workflow and safety tests.

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
