# Remote state and distributed locking execution plan

## Objective

Add a production-usable PostgreSQL ownership-state backend without weakening
the existing local path or allowing concurrent runners to mutate infrastructure
from the same prior snapshot. The normative boundary is
`docs/specs/remote-deployment-state.md`.

## Current implementation audit

The current local implementation has a sound single-host commit primitive:

- `LocalState` strictly validates version, project, environment, serial, stable
  resource identities, record fields, and duplicate JSON keys.
- State is isolated at `.streamt/state/<environment>.json`.
- `save()` writes, flushes, fsyncs, and atomically replaces the target.
- `save_if_serial()` takes an adjacent `flock`, reloads the current serial, and
  only writes `expected_serial + 1`.
- `plan`, `apply`, and `adopt` load the same environment-scoped state.
- Reviewed plans bind to the state serial; apply replans live actions.
- Failed applies and completed rollback paths do not advance state.
- Targeted apply retains records outside the selection, and absence never
  deletes a record.

The remaining gaps are material:

- The `flock` is held only during the final state write, after runtime mutations
  have already happened. Two local processes can both act from the same prior
  snapshot; the loser detects the serial conflict only after mutation.
- `apply` computes `next_state` before runtime actions and writes after them,
  with no durable operation intent or recovery marker.
- `adopt` observes and confirms before taking the final write lock, so live or
  ownership state can change between review and commit.
- State access is coupled to paths and `LocalState` methods rather than a
  provider-neutral application service.
- A plan records only `state_serial`, not the state checksum, backend instance,
  or canonical state address.
- An apply that succeeds in infrastructure but cannot save state is reported as
  invalid local state, but the next command has no durable indication that
  reconciliation is required.
- Local warnings truthfully say shared CI is unsupported; no remote provider or
  distributed operation lock exists.

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

### Slice 4: strict configuration and safe administrative CLI

1. Add strict `deployment_state` models to base and environment configuration.
2. Support `local` and `postgres` tagged shapes. Local is the default. Reject
   mixed provider fields and unknown keys.
3. Resolve only the configured DSN environment-variable name at construction.
4. Add central redaction tests for provider exceptions, credential URLs,
   authorization text, and structured nested values.
5. Add `streamt state status` and `streamt state init`. `status` is read-only;
   `init` uses exact address/environment confirmation.
6. Add opt-in `safety.require_remote_state`, evaluated before constructing
   runtime deployers.

Acceptance:

- Missing DSN, malformed provider config, or unavailable remote state fails
  before runtime observation or mutation and never reads local state.
- Command-line flags cannot switch the state backend.
- JSON/text output contains no DSN, password, host, lock token, SQL, or raw
  provider exception.
- Strict docs/schema tests cover the new fields.

### Slice 5: PostgreSQL backend

1. Add an optional PostgreSQL dependency and a narrow adapter module. Importing
   streamt without the extra must continue to work for local users.
2. Define versioned DDL for immutable store metadata, current state, operation
   metadata, lock-key mapping, and append-only history.
3. Implement explicit idempotent initialization with an administrative role.
4. Implement consistent read and atomic CAS transitions with parameterized SQL.
5. Implement bounded session advisory-lock acquisition. Store and validate the
   full address-to-lock-key mapping.
6. Check connection and lock ownership before every external action and state
   transition.
7. Set lock/statement timeouts, state-size limits, transaction isolation, and
   TLS defaults explicitly.
8. Translate database failures, including unknown commit outcome, into stable
   sanitized backend errors.

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
