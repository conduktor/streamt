# PostgreSQL Slice 5 foundation and enablement plan

## Objective

Implement PostgreSQL ordinary deployment state without making an incomplete
backend selectable. This plan is the implementation-ready prerequisite for
Slices 5 and 6 of `2026-09-01-remote-state-and-locking.md`. It records both the
foundation already delivered and the remaining production gates.

The ordinary PostgreSQL factory remains disabled until the final enablement
commit. Before that commit, the backend is reachable only from focused tests
and explicit administrative commands. The final commit is allowed only after
the protocol, command integration, failure injection, minimum recovery
workflow, schema/role contract, and full release gates below pass together.

## Delivery status (2026-09-02)

Packages 1 through 8 are implemented and pass the current test gates, including
PostgreSQL 14 and 18 conformance. The delivered foundation includes canonical
planned-action identity, snapshot-bound local apply/adopt, stable failure taxonomy and
operation-ID recovery evidence, and a direct-construction-only PostgreSQL v1
owner backend covering consistent reads, session locks, atomic mutation,
histories, commit ambiguity, and crash recovery evidence. Package 6 adds the
explicit `state migrate-postgres-v2` administrative path, exact store/role
confirmation, the portable writer-name metadata/ACL contract, v1/v2
administrative compatibility, and private least-privilege writer execution.
Package 7 adds real command-level apply, reviewed apply, adopt, failure,
contention, and installed-wheel gates against both supported PostgreSQL majors.

Package 8 adds a distinct writer credential binding, strict reviewed recovery
evidence, three explicit outcomes, an integrity-checked no-overwrite plan file,
provider-neutral locked orchestration, crash-safe local audit/finalization,
atomic PostgreSQL finalization, conservative live-target reconstruction, and a
recovery-only factory. The two-command `state recovery-plan`/`state recover`
workflow passes all three local resolution paths, real PostgreSQL finalization
for all three outcomes, PostgreSQL command E2E, and isolated installed-wheel
gates on PostgreSQL 14 and 18. Public configuration, CLI, support, migration,
backup, recovery, and topology documentation now describes this recovery-only
boundary and its fail-closed target limitations.

Package 9 remains prohibited until the remaining topology/HA evidence and final
release gates pass. Ordinary PostgreSQL factory selection remains disabled.

## Dependencies and preserved boundaries

- Slice 4B's version-1 initialization, status, and lock-status behavior is a
  shipped administrative contract. Do not weaken its frozen catalog validation
  or describe ordinary PostgreSQL authority as available while this plan is in
  progress.
- Local version-1 ownership JSON and its sidecar remain compatible. Refactoring
  the provider-neutral protocol must preserve existing local behavior and CLI
  output unless a separately reviewed compatibility change says otherwise.
- Reviewed-plan format version 3 remains the portable state reference. Backend
  revisions, control revisions, DSNs, lock handles, and credentials never enter
  reviewed plans.
- Destructive removal, implicit migration, automatic recovery, and automatic
  schema upgrade remain out of scope.

## Non-negotiable safety decisions

1. One consistent `OperationSnapshot` binds ownership and operation control
   from the same store and address. State-only observations cannot authorize a
   mutation.
2. `begin_operation` compares both state and control revisions, the prior state
   serial and checksum, the operation lock, and the absence of a blocking
   marker before it records intent.
3. `commit_operation` atomically writes the ownership replacement when needed,
   clears control, and appends state and operation history in one database
   transaction. There is no remote sequence in which ownership is committed
   and the marker is cleared by separate transactions.
4. Durable action identity is the canonical logical resource URI. Runtime and
   display labels are separate fields and cannot be substituted for that URI.
5. Every apply, including direct apply, performs a final direct backend reread
   after live planning and immediately before `begin_operation`. The final
   snapshot is the CAS authority.
6. Adoption re-observes the exact target after confirmation, compares the exact
   confirmed fingerprint, then performs its state-only operation. Pre-prompt
   evidence cannot authorize the write.
7. A command releases the operation lock before it emits or flushes final
   success. A release failure after a verified commit reports `committed: true`
   and must not suggest retrying the mutation.
8. Lock timeout, lock loss, state conflict, unknown outcome, release failure
   after commit, and recovery-required are distinct sanitized error classes and
   structured outcomes.
9. Private backend tests may use an isolated schema-owner credential against
   version 1. That exception is test scaffolding only. Package 6 has delivered
   the explicit schema-version-2 migration and exact least-privilege writer
   contract; production enablement additionally requires Packages 7 through 9.
10. Ordinary operations use one direct, session-affine primary connection for
    the whole lock lifetime. Transaction- and statement-pooling endpoints are
    unsupported. A failover-capable production claim additionally requires
    synchronous durability for every intent, progress, finalization, and
    recovery transition; asynchronous promotion is outside the support
    boundary.

## Required protocol

The additive provider-neutral foundation uses the current names and shape
below. `OperationSnapshot` stores only state and control; its address and store
identity are derived from the contained state observation, with construction
requiring the control address to match. The excerpt shows the
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

`StateObservation` and `ControlObservation` have independent opaque revisions.
For the local compatibility provider, `observe()` binds the adjacent files at
one locked workflow boundary without claiming a cross-file atomic read. For a
remote provider, each `observe()` result must come from one consistent database
snapshot and match the derived store/address identity.

The richer remote behavior in this plan is a target contract, not a claim that
the additive foundation already exposes `read_snapshot`, history methods, or
separate fail/abort methods. A PostgreSQL adapter may add a backend
`read_snapshot(address)` for consistent lock-free plan/status reads, and it may
use private history helpers, but application mutation continues through
`observe`, `mark_recovery_required`, `clear_before_mutation`, and
`commit_operation`. Remote finalization provides the stronger atomic ownership,
control, and history semantics behind that existing method.

The local implementation may preserve its conservative write ordering under
the same file lock. Remote conformance requires database-atomic finalization.
Compatibility delegates such as local state-only CAS may remain private to the
local adapter; ordinary mutation commands use the operation protocol.

## Canonical planned actions

Introduce one typed plan action at the planner/application boundary:

```python
@dataclass(frozen=True)
class PlannedAction:
    resource_id: str
    runtime_label: str
    action: str
```

`resource_id` contains the canonical logical URI:

```text
streamt://<project>/<environment>/<kind>/<logical-name>
```

It is used in reviewed action comparisons, operation intent, progress, failure
records, recovery, and history. `runtime_label` may contain a provider-facing
name such as a topic name, but it is used only for callback dispatch and safe
human display. The record validates that `resource_id` parses as a canonical
resource URI. Planner/operation construction, where the complete ordered set
and state address are available, rejects duplicate identities and any
project/environment mismatch with that address.

## PostgreSQL transaction contract

### Consistent reads

- If the remote adapter adds the target-only `read_snapshot` helper for
  lock-free plan/status reads, it uses one bounded, read-only, repeatable-read
  transaction. Locked mutation reads use `observe()` with the same consistency.
- It verifies catalog version, immutable store identity, address registration,
  ownership row, operation-control row, payload size, and canonical checksum
  before returning.
- An absent ownership row is an explicit absent state revision. It pairs with
  clear control before the first operation, but may pair with `in_progress` or
  `recovery_required` after a first-operation interruption; that blocker must
  remain readable. Missing control or address rows are invalid catalog state,
  not absence.

### Lock ownership

- `operation()` opens a dedicated direct-primary connection, checks
  `pg_is_in_recovery()`, takes the collision-checked session advisory lock once,
  and keeps that physical session until release.
- A timeout returns the dedicated lock-timeout error without owner or endpoint
  details. The implementation never steals a lock.
- Every external action and state transition first checks the owning session,
  primary status, and advisory-lock ownership. Loss starts no subsequent
  runtime action.
- Acquisition is not authority by itself. The first snapshot after acquisition
  and the final post-plan snapshot provide the mutation evidence.

### Operation transitions

- `begin_operation` runs in one write transaction and rechecks lock ownership,
  store/address identity, expected state revision/serial/checksum, expected
  control revision, and clear control. It appends the started history event and
  commits the `in_progress` marker together.
- `record_progress` compares the current operation ID, expected control
  revision, state observation, and lock. It advances progress monotonically and
  appends the progress event in one transaction.
- `commit_operation` compares the operation ID, both observations, and lock. It
  writes a new ownership row only when ownership changed, clears control, and
  appends both state history when applicable and the success operation event in
  one transaction. It then performs a fresh direct read by operation ID and
  expected state identity to verify the committed result.
- `clear_before_mutation` is legal only when durable progress proves no runtime
  action started. It clears control and appends a failure event atomically.
- `mark_recovery_required` preserves or writes `recovery_required` and appends
  a sanitized failure event atomically. If the lock or database is unavailable,
  the prewritten `in_progress` marker remains the durable blocker.
- A connection loss during commit is never retried automatically. Verification
  may classify the result as definitely committed or definitely not committed;
  otherwise it returns unknown outcome with the operation ID.

### Durability and topology

Standalone support requires the PostgreSQL commit to be durable on the direct
primary before a transition is acknowledged. Production high-availability
support requires synchronous replication to every node eligible for promotion
for the complete operation-control and ownership schema. If this cannot be
verified operationally, documentation must describe the deployment as
single-primary only; asynchronous failover can lose the marker while releasing
the advisory lock and is not safe.

## Schema version 2 and ordinary writer role

Version 1 remains the frozen administrative catalog accepted by shipped Slice
4 commands. Slice 5 can exercise private mutation code only with an isolated
owner credential and direct factory construction in tests. Owner credentials
must not be recommended for ordinary jobs, and version 1 must never be reported
as mutation-ready.

Before production enablement, add an explicit version-1-to-version-2
administrative migration with fresh verification and unknown-outcome handling.
Version 2 stores the canonical ordinary writer role name in metadata and
validates the exact non-grantable ACL below. It never stores the cluster-local
role OID, which would break logical restore portability. streamt does not
create, alter, or drop the role, infer it from the login, or silently broaden
grants. The explicit migration transaction resets that pre-created role's ACL
on the state schema, issues the exact grants, and validates the result; writer
DML cannot be pre-granted while the source remains a valid v1 catalog.

| Object | Ordinary writer grants | Explicitly forbidden |
| --- | --- | --- |
| Schema | `USAGE` | `CREATE`, grant option, `PUBLIC` access |
| Store metadata and migration ledger | `SELECT` | `INSERT`, `UPDATE`, `DELETE` |
| Address and lock-key mapping | `SELECT` | `INSERT`, `UPDATE`, `DELETE` |
| Current ownership | `SELECT`, `INSERT`, `UPDATE` on the exact required columns | `DELETE`, grant option |
| Operation control | `SELECT`, `UPDATE` on the exact required columns | `INSERT`, `DELETE`, grant option |
| State and operation history | `SELECT`, `INSERT` on the exact required columns | `UPDATE`, `DELETE`, grant option |

Sequence/identity privileges, if schema version 2 introduces such objects,
must be enumerated exactly rather than covered by broad table grants. No
ordinary role has DDL, ownership, role membership, default-privilege mutation,
or schema-migration authority. Status-reader grants remain the version-1
read-only set. Catalog validation rejects missing, extra, grantable, owner, or
`PUBLIC` privileges.

The writer role must be a direct `LOGIN` identity distinct from the common
owner, with no superuser, database creation, role creation, replication,
bypass-RLS, inheritance, or membership authority. Ordinary sessions must later
prove `session_user = current_user = writer_role_name`; `SET ROLE` is not a
substitute. Writer grants use the common owner as grantor. Other named readers
retain the exact non-grantable v1 `USAGE`/`SELECT` contract.

Migration takes the schema initialization lock, then every registered address
lock in deterministic order under one bounded deadline. After all locks are
held it rereads exact v1, requires every control row to be clear, and preserves
populated current state and both histories unchanged. Catalog DDL, metadata,
ledger, and grants commit in one serializable transaction and receive a fresh
direct-primary verification. Address locks are released in reverse order
before the schema lock. Active/recovery controls, a busy address, partial v2,
writer mismatch, or ACL drift fail closed; there is no implicit repair,
downgrade, or automatic migration.

After migration, administrative status, lock diagnostics, and owner-only
address registration must dispatch between exact v1 and exact v2 without an
external writer setting because v2 already stores the role name. Exact v2 may
report the catalog as mutation-ready, but `ordinary_state_authority` remains
disabled until the final factory commit. New stores still initialize as v1.

The schema-version-2 implementation adds a strict optional
`postgres.writer_role_env` configuration field naming the environment variable
that contains the ordinary role identifier. It remains unnecessary for shipped
version-1 `state init`, `state status`, and `state lock-status`, but is required
by the version-2 migration and every ordinary factory construction. The
migration requires an exact writer-role confirmation before connecting, stores
the resolved role name in catalog metadata, and freshly verifies its ACL. An
ordinary session must prove that `session_user` and `current_user` are that
exact role; role membership or equivalent effective privileges are not a
substitute. The role value, database login, and catalog role identifiers remain
excluded from normal text/JSON output and reviewed plans. Add the field to generated configuration
schema/reference material and strict unknown-field/environment-resolution tests
only when the implementation lands.

## Command algorithms

### Apply

1. Parse configuration and reviewed-plan input and enforce policy ordering.
2. Open the operation context and acquire its exclusive lock.
3. Read the initial `OperationSnapshot`; reject identity, recovery, and reviewed
   state-reference mismatches.
4. Observe runtime state and produce typed canonical `PlannedAction` values.
5. For reviewed apply, compare the entire regenerated plan. For direct apply,
   retain the initial state reference as drift evidence.
6. Directly reread the complete snapshot after live planning. Reject any state
   or control change for both reviewed and direct apply. Use only this final
   snapshot as the expected value for `begin_operation`.
7. Persist intent containing ordered canonical identities before the first
   runtime action.
8. Check lock health, execute one action, and record ordered progress for each
   action. Apply the existing rollback policy on failure.
9. Atomically commit ownership, clear control, and append histories. Perform the
   fresh verification read.
10. Release the lock. Only after successful release emit and flush final
    success. If release fails after verified commit, report a committed release
    failure with recovery/status guidance and no retry instruction.

### Adoption

1. Resolve and observe the exact target and construct the exact fingerprint
   shown for confirmation.
2. Obtain exact confirmation, then acquire the operation lock if it is not
   already held by the local compatibility path.
3. Read the complete operation snapshot and reject conflicts or recovery.
4. Re-observe the exact target after confirmation and compare its complete
   fingerprint with the confirmed evidence. Any change requires a new prompt
   or fails non-interactive adoption.
5. Reread the snapshot immediately before begin, write a state-only intent with
   the canonical identity, atomically commit ownership/control/history, verify,
   and release before reporting success. Adoption never calls runtime mutation.

## Stable error taxonomy

The implementation may allocate final numeric codes in the normal registry,
but it must preserve these distinct machine-readable kinds and retry advice:

| Kind | Meaning | Retry guidance |
| --- | --- | --- |
| `state_lock_timeout` | Another operation owns the address lock | Retry after bounded wait or inspect status |
| `state_lock_lost` | The owning session/primary/lock disappeared | Do not continue; inspect operation status |
| `state_conflict` | Expected state or control observation changed | Re-plan or re-observe; do not replay mutation |
| `state_unknown_outcome` | A transition may have committed | Do not retry; resolve by operation ID/status |
| `state_release_failed_after_commit` | Commit is verified but lock release could not be verified | Do not replay; report `committed: true` and inspect status |
| `state_recovery_required` | Durable control blocks mutation | Run the explicit recovery workflow |

All messages and structured details pass central redaction and exclude the DSN,
host, database/schema/user names, SQL, advisory key, lock token, and raw provider
exception. Once intent is durable, lock-lost, unknown-outcome, and
release-after-commit errors preserve its validated operation UUID in both the
structured `operation_id` field and sanitized human output.

## Work packages and commit order

Each package is a separately reviewable logical commit. Packages may be
developed in parallel only where their file ownership does not overlap, but
they merge in this order.

1. **Protocol and local compatibility.** Add `OperationSnapshot`, independent
   state/control observations, and the additive `observe`/transition operation
   surface. Adapt local behavior and fake backends while recording the stronger
   remote atomic-finalization requirement; keep PostgreSQL factory selection
   disabled.
2. **Canonical action identity.** Replace durable/display string coupling with
   typed `PlannedAction` values through planner, plan comparison, local intent,
   progress, and recovery models.
3. **Provider-neutral local command migration.** Route local apply and adoption
   through `observe`, final state/control rereads, canonical actions,
   `clear_before_mutation`, recovery marking, commit, and release-before-success.
   Add post-confirmation adoption re-observation and preserve all local
   compatibility behavior before implementing any private PostgreSQL path.
4. **Private PostgreSQL read and lock path.** Implement consistent snapshots
   and session-affine operation contexts behind a test-only construction path.
   Use isolated owner credentials for this package only.
5. **Private PostgreSQL mutation path.** Implement begin, progress, atomic
   commit, clear-before-mutation, recovery marking, internal history, final
   verification, and unknown-outcome classification. The ordinary factory
   still rejects PostgreSQL.
6. **Schema version 2 administration — complete.** Explicit migration, exact
   store/role confirmation, exact writer identity/ACL validation, safe status
   reporting, operator documentation, and least-privilege backend conformance
   are implemented. The ordinary factory remains disabled.
7. **PostgreSQL command E2E and failure gates — complete.** The private
   PostgreSQL adapter passes real direct apply, reviewed apply, adopt, failure
   injection, process contention, and isolated installed-wheel command tests on
   PostgreSQL 14 and 18. Ordinary factory selection remains disabled.
8. **Minimum recovery — complete.** `state recovery-plan` and `state recover`
   ship for `observed`, `rolled_back`, and `abandoned_before_mutation`, with
   exact confirmation, operation-ID binding, fresh project/target evidence,
   CAS, append-only audit, local crash safety, and atomic PostgreSQL
   finalization. Unrepresentable live states remain blocked rather than being
   guessed. Migration/export may follow independently.
9. **Final factory enablement.** In the last implementation commit only, allow
   the ordinary factory to select a verified version-2 PostgreSQL store with the
   exact writer role. Update public support/configuration/release documentation
   in the same release boundary. No earlier commit may make a partial backend
   reachable from `plan`, `apply`, or `adopt`.

Expected implementation surfaces include
`src/streamt/deployer/state_backend.py`, `state.py`, `postgres_state.py`, a
focused PostgreSQL ordinary adapter if separated, planner/plan action models,
`apply.py`, `adopt.py`, `plan.py`, `state_cmd.py`, configuration, stable errors,
and their unit/integration/scenario tests. Exact file ownership should be set
when agents are assigned so shared state/configuration files have one owner at
a time.

## Required test and failure-injection matrix

| Boundary | Required proof |
| --- | --- |
| Snapshot | State/control from one commit; wrong store/address; absent state; missing control; size/checksum/catalog rejection |
| Begin CAS | Stale state revision, serial, checksum, control revision, operation marker, lock loss, and unknown commit |
| Action identity | Canonical URI, logical/runtime-name divergence, duplicates, cross-project/environment identity, deterministic ordering |
| Final commit | Ownership changed/unchanged, control clear, both histories, all-or-nothing rollback, final direct reread, unknown outcome |
| Locking | 20 separate-process contenders, timeout, waiter freshness, session termination, primary loss, advisory-key collision, release failure |
| Direct apply | State/control drift during live plan, final reread, no mutation before intent, ordered progress, rollback branches |
| Reviewed apply | Backend/store/address/serial/checksum drift, regenerated-plan drift, final reread drift, no mutation on rejection |
| Adoption | Target changes before prompt, during prompt, and after confirmation; exact fingerprint; state conflict; no runtime writes |
| Recovery | All three outcomes, wrong operation ID, stale live evidence, illegal abandoned outcome after start, monotonic serial/history |
| ACL | Owner-only private tests; v1 rejected as mutation-ready; exact v2 writer succeeds; every missing/extra/grantable/`PUBLIC` privilege fails |
| Topology | Direct primary succeeds; standby, transaction pooler, and statement pooler fail; lock-session loss blocks successor |
| HA durability | Synchronous eligible promotion retains marker/state; asynchronous failover is rejected or documented unsupported |
| Output | Release precedes success; committed release failure is distinct; all text/JSON/log/exception paths are secret-neutral |
| Compatibility | Local JSON/sidecar, warnings, offline no-read, reviewed format 3, admin v1 init/status/lock-status, base package without Psycopg |

Concurrency, termination, commit ambiguity, and failover tests use separate
processes and real PostgreSQL, not only threads or mocks. Failure injection
covers acquire, initial read, runtime observation, live plan, final reread,
begin, every action, every progress transition, rollback, fail transition,
commit before/during/after acknowledgement, verification read, and release.

## Final factory-enablement checklist

The final factory diff is prohibited until every item is evidenced in CI or a
reviewed operator test record:

- [x] `OperationSnapshot` and state/control CAS conformance pass for local,
      fake, and PostgreSQL providers.
- [x] PostgreSQL finalization is atomic across ownership, control, and both
      histories, with a fresh verification read.
- [x] Durable actions use canonical logical identities end to end.
- [x] Direct and reviewed apply use the final post-plan reread.
- [x] Adoption uses post-confirmation exact-target re-observation.
- [x] Final success is emitted only after lock release; committed release
      failure has a distinct non-retry outcome.
- [x] Lock timeout, loss, conflict, unknown outcome, release failure, and
      recovery-required have distinct sanitized errors.
- [x] Schema version 2 migration and exact least-privilege writer-role
      validation pass; no ordinary job uses the owner identity.
- [ ] Direct/session-affine primary checks pass, and the documented standalone
      versus synchronous-HA durability boundary matches tested deployment.
- [x] Slice 6 command E2E and the complete failure-injection matrix pass on the
      supported PostgreSQL major versions.
- [x] Minimum explicit recovery ships and passes all three resolution paths.
- [x] Local-only/base and PostgreSQL-extra unit, scenario, packaging, strict
      docs, Ruff, and zero-error mypy gates pass.
- [ ] Public support, configuration, operations, migration, backup, recovery,
      rollback, monitoring, and release notes describe only the enabled
      version-2 boundary.
- [ ] The enablement commit contains no fallback to local/empty state and no
      automatic schema migration, operation retry, force unlock, or recovery.

If any item fails, keep ordinary PostgreSQL selection disabled. A usable
administrative adapter or passing backend unit suite is not sufficient evidence
for production enablement.
