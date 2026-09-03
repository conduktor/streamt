# Explicit Kafka Connect connector removal implementation plan

## Status and objective

Status: in progress. The reserved codes, strict declaration, compiled artifact,
and fail-closed plan/apply boundary are implemented. Connector-removal
preflight, planning, reviewed authorization, mutation, recovery, and release
gates remain open; no deletion capability is claimed yet.

The objective is to implement the frozen
[Connector removal specification](../specs/connector-explicit-removal.md) as
the smallest new Phase 0 destructive lifecycle slice. Delivery is
PostgreSQL-schema-v2-only and does not broaden deletion to Kafka topics,
Schema Registry subjects, or Flink jobs.

The work reuses the existing strict Connector cluster binding, one-request
managed observation, secret-neutral config diffing, PostgreSQL advisory-lock
operation boundary, reviewed plans, durable progress, and recovery machinery.
It does not reuse the bare provider `delete_connector(name)` method as
lifecycle authorization.

## Frozen implementation decisions

1. The public declaration is exactly
   `lifecycle.connector_removals[].{logical_owner,name,cluster}`.
2. The tombstone carries no prior connector class, topics, or config.
3. `cluster` is required and never inferred from the default during removal.
   In the first release it must name the configured default cluster; alias-
   indexed deployer routing is deferred.
4. Only one exact PostgreSQL-v2 `managed` record can authorize an actual
   delete. Adopted, local, legacy, and ambiguous records fail closed.
5. Exact prior checksum proof is reconstructed from the strict live Connector
   observation.
6. A full online reviewed plan is always required. Direct, dry-run, offline,
   targeted, selected, and otherwise partial workflows are forbidden.
7. The reviewed plan advances to version 5. Operation control and recovery
   plans advance to version 3. PostgreSQL schema v2 and ownership state v1 do
   not change.
8. Lifecycle mutation uses `delete_managed_connector(current)` with an
   immediate equality observation, one exact non-retrying DELETE requiring
   HTTP 204 with an empty body, and bounded absence confirmation.
9. A DELETE-time 404 is uncertain drift and enters durable recovery.
10. Only a successfully completed durable delete or reviewed recovery of its
    exact absent candidate removes the ownership record.

These decisions are prerequisites, not choices to revisit in later slices.

## Baseline to preserve

The current tree already provides:

- `ConnectClusterBinding` and canonical
  `kafka-connect:v1:<alias>:sha256:<endpoint>` identities;
- `parse_compiled_connector_artifact()` and exact reserved config fields;
- `ManagedConnectorObservation` with a bounded, redirect-disabled,
  percent-encoded one-resource GET and secret-neutral fingerprint;
- bound desired Connector planning and exact backend state records;
- Connector state-only adoption and generic reviewed recovery normalization;
- PostgreSQL-v2 ordinary plan/apply/adopt, session-affine advisory locks,
  durable operation history, and atomic finalization;
- generic planner dispatch for manually constructed Connector deletes; and
- a bare `delete_connector(name)` provider primitive.

The missing pieces are an explicit declaration and compiled artifact, pure
removal preflight, an ordinary planner delete source, Connector durable action
evidence, managed mutation postconditions, Connector-specific state removal,
strict CLI authorization, recovery without a desired connector artifact, and
release gates.

`LocalState.removal_candidates()` remains informational. Generic manual
`ConnectorChange(action="delete")` test construction and rollback deletion of
a connector created earlier in the same failed operation are not lifecycle
authority and must remain separate.

## Data shapes to implement

### Project and manifest

```text
ConnectorRemovalDeclaration
  logical_owner: str
  name: str
  cluster: str

ConnectorRemovalArtifact
  logical_owner: str
  connector_name: str
  cluster_alias: str
```

The manifest wire representation is exactly:

```json
{
  "logicalOwner": "archive_orders",
  "name": "archive-orders-sink",
  "cluster": "primary-connect"
}
```

The artifact should be immutable and serialize defensive primitive values. It
must not expose a config field or accept ownership/backend values.

### Pure target

```text
ResolvedConnectorRemoval
  resource_id: str
  logical_owner: str
  connector_name: str
  binding: ConnectClusterBinding
  prior_record: ManagedResourceRecord | None
```

The value validates canonical logical and provider identities on construction.
It does not contain endpoint text, credentials, or raw config.

### Plan assessment

```text
ConnectorRemovalAssessmentStatus =
  "already_absent" |
  "state_provider_drift" |
  "ownership_required"

ConnectorRemovalAssessment
  resource_id
  logical_owner
  connector_name
  backend_identity
  status
```

`DeploymentPlan.connector_removal_assessments` is an immutable tuple. Blocker
refresh deterministically maps the two blocking statuses without converting
`already_absent` into a change.

### Action evidence

```text
ConnectorActionSurfaceEvidence
  exists: bool
  fingerprint: sha256 string

ConnectorActionEvidence
  version: Literal[1]
  backend_identity: canonical Connect backend
  connector_name: str
  prior_artifact_checksum: sha256 string
  current: ConnectorActionSurfaceEvidence
  desired: ConnectorActionSurfaceEvidence
```

For this slice, `connector_evidence` is required only when an
`OperationAction` identifies a `connector` with action `delete`. It is forbidden
for other resource kinds and mutually exclusive with `gateway_evidence`.
Create, update, and adopt Connector actions retain their current recovery path
and serialize `connector_evidence: null`.

Version-5 reviewed and version-3 control action JSON uses exact keys:

```json
{
  "index": 0,
  "resource_id": "streamt://payments/prod/connector/archive_orders",
  "action": "delete",
  "gateway_evidence": null,
  "connector_evidence": {
    "version": 1,
    "backend_identity": "kafka-connect:v1:primary-connect:sha256:<digest>",
    "connector_name": "archive-orders-sink",
    "prior_artifact_checksum": "sha256:<digest>",
    "current": {"exists": true, "fingerprint": "sha256:<digest>"},
    "desired": {"exists": false, "fingerprint": "sha256:<digest>"}
  }
}
```

### State projection claim

```text
ManagedConnectorResourceDeletion
  resource_id
  backend_identity
  connector_name
  prior_artifact_checksum
```

Keep this Connector-specific. Do not introduce a generic deletion claim that
could accidentally enable topic, schema, or Flink state removal.

## Codes and stable outcomes

Add to `ErrorCode`:

```text
E427_CONNECTOR_REMOVAL_INVALID
E428_CONNECTOR_REMOVAL_DRIFT
W119_CONNECTOR_REMOVAL_DESTRUCTIVE
```

Reuse existing `E205`, `E209`, `E407` through `E409`, `E417` through `E426`,
`E501`, and `E503` exactly as assigned by the specification. Do not emit
`W106_LOCAL_STATE_ONLY` for a removal attempt: local state is a hard
`E421_REMOTE_STATE_REQUIRED` failure.

Add stable planner blocker identifiers:

```text
connector_removal_state_provider_drift
connector_removal_ownership_required
```

Provider exception strings and response bodies are never used as stable codes
or unsanitized user messages.

## Ordered implementation slices

### Slice 0: freeze contracts and negative gates

Primary files:

- `docs/specs/connector-explicit-removal.md`
- `docs/plans/2026-09-03-connector-explicit-removal.md`
- `src/streamt/core/errors.py`
- `tests/unit/test_cli_connector_removal_authorization.py`

Tasks:

1. Add the three reserved codes.
2. Write failing contract tests proving that local, direct, dry-run, offline,
   targeted, selected, and partial workflows cannot reach deployment-state or
   Connect provider construction.
3. Freeze precedence: malformed YAML is `E501`; direct/partial/offline is
   `E418`; a syntactically authorized reviewed workflow using local or another
   non-PostgreSQL backend is `E421`; configured PostgreSQL v1 or an invalid v2
   authority is `E420`; pending recovery and lock failures retain `E419`/`E422`.
4. Freeze all JSON payload fields and next-step commands as secret-neutral.

Exit gate: negative command behavior is specified by tests before a mutation
source exists. Tests may use a synthetic compiled manifest until Slice 1.

### Slice 1: strict DSL and pure compilation

Primary files:

- `src/streamt/core/models.py`
- `src/streamt/core/parser.py`
- `src/streamt/core/validator.py`
- `src/streamt/compiler/manifest.py`
- `src/streamt/compiler/compiler.py`
- generated project JSON schema
- `tests/unit/test_connector_removal_dsl.py`
- `tests/unit/test_compiler.py`
- `tests/unit/test_doc_yaml_validation.py`

Tasks:

1. Add strict `ConnectorRemovalDeclaration` and the additive lifecycle list.
2. Require all three fields; enforce the frozen text, resource-ID, collection,
   and secret-neutral bounds; and reject every unknown or secret-bearing field.
3. Validate duplicate logical owners and alias-independent normalized-endpoint
   digest plus connector-name provider targets.
4. Compile an immutable `ConnectorRemovalArtifact` into the separate
   `connector_removals` collection only when non-empty.
5. Include that collection in the manifest checksum without adding it to the
   desired connector list, DAG, selection, or ownership projection.
6. Prove that removing a model without a tombstone still creates no delete.

Exit gate: arbitrary config or endpoint material cannot enter the tombstone or
compiled removal artifact, and compilation performs no runtime/state access.

### Slice 2: pure binding, collision, and prior-state preflight

Primary files:

- `src/streamt/compiler/connector_artifact.py`
- `src/streamt/deployer/connect.py`
- `src/streamt/deployer/planner.py`
- a shared pure preflight module if separation improves import boundaries
- `src/streamt/cli/commands/plan.py`
- `src/streamt/cli/commands/apply.py`
- `tests/unit/test_connector_removal_preflight.py`
- `tests/unit/test_planner_connector_artifacts.py`

Tasks:

1. Require the explicitly named cluster to equal `runtime.connect.default`,
   then resolve it to one canonical binding; reject unknown and non-default
   aliases with E209 before provider construction.
2. Resolve canonical logical resource and provider locator identities.
3. Validate desired/removal, removal/removal, and prior-state collisions by
   normalized endpoint digest plus connector name before deployer construction,
   including distinct aliases for the same endpoint.
4. Require an exact managed record when present and reject adopted, legacy,
   wrong-backend, wrong-name, cross-project, and duplicate-provider claims.
5. Return immutable resolved targets in declaration order.
6. Enforce PostgreSQL-v2 intrinsically, independent of environment policy.
7. Ensure every preflight failure makes zero Connect HTTP calls and zero state
   writes.

Exit gate: only provider config reconstruction remains after Connect access;
all identity and authority errors possible from project/state data fail first.

### Slice 3: evidence values and wire-version migration

Primary files:

- `src/streamt/deployer/state_backend.py`
- `src/streamt/deployer/operation_actions.py`
- `src/streamt/deployer/plan_file.py`
- `src/streamt/deployer/recovery_plan.py`
- PostgreSQL and local control serializers
- `tests/unit/test_state_backend.py`
- `tests/unit/test_plan_file.py`
- `tests/unit/test_recovery_plan.py`
- `tests/unit/test_cli_plan_action_wiring.py`

Tasks:

1. Add strict surface/evidence dataclasses, canonical absence fingerprinting,
   exact field validation, and secret scanning.
2. Add nullable `connector_evidence` to `PlannedAction` and
   `OperationAction`; enforce resource-kind, action, and mutual-exclusion
   rules.
3. Advance control to v3 while explicitly retaining strict v1, v2, and v3
   readers everywhere. Clear v2 controls remain valid for ordinary commands
   and upgrade only on write; active v1/v2 controls retain their original
   recovery semantics and byte/checksum shape.
4. Advance reviewed plans to v5. Reject v1-v4 for apply with deterministic
   regeneration guidance.
5. Advance recovery plans to v3 while preserving v1/v2 parsing and semantics.
6. Verify PostgreSQL JSON payloads need no DDL or schema-version change.
7. Prove config, endpoint, DSN, and provider responses cannot enter any wire
   form or checksum presentation.

Exit gate: exact action evidence round-trips through reviewed plans, local
parsers, PostgreSQL control/history, and recovery plans, but no delete is yet
routable.

### Slice 4: strict planning and managed provider mutation

Primary files:

- `src/streamt/deployer/connect.py`
- `src/streamt/deployer/planner.py`
- `tests/unit/test_connector_removal_planning.py`
- `tests/unit/test_connector_managed_mutation.py`
- `tests/unit/test_planner_connector_mutation.py`
- `tests/unit/test_connect_runtime_foundation.py`

Tasks:

1. Observe every removal target with the strict one-resource observer; memoize
   an exact target shared with a desired/recovery observation when applicable.
2. Reconstruct the prior compiled artifact from reserved config, explicit
   cluster, and managed model ownership. Compare its checksum exactly with
   prior state.
3. Produce `delete`, `already_absent`, `state_provider_drift`, or
   `ownership_required` according to the normative matrix.
4. Attach exact Connector evidence to actionable ordered planner actions.
5. Add `delete_managed_connector(current)` and forbid bare names at this
   lifecycle boundary.
6. Immediately re-observe and compare current, issue one encoded, bounded,
   redirect-disabled, non-retrying DELETE, require exact 204/empty-body
   response, then poll to exact absence.
7. Treat DELETE-time 404, every other non-204 or redirected response, nonempty
   204 body, transport uncertainty, changed preimage, and absent-postcondition
   timeout as `E428`, never idempotent success.
8. Preserve the existing bare method only for test cleanup/compatibility and
   same-operation rollback; document that it is not lifecycle authority.

Exit gate: the provider method returns only `"deleted"` after exact absence,
never deletes an unrelated connector, and all uncertain paths fail after
durable progress can be recorded by the caller.

### Slice 5: CLI authorization, destructive gate, and state projection

Primary files:

- `src/streamt/cli/commands/plan.py`
- `src/streamt/cli/commands/apply.py`
- `src/streamt/deployer/planner.py`
- `src/streamt/deployer/state.py`
- `tests/unit/test_cli_connector_removal_authorization.py`
- `tests/unit/test_cli_connector_removal_e2e.py`
- `tests/unit/test_deployment_state.py`
- `tests/unit/test_safety_blockers.py`

Tasks:

1. Require a PostgreSQL-v2 online reviewed plan for every declaration, whether
   or not the fresh result is actionable.
2. Reject target/select/partial execution before selection and provider access.
3. Classify an actual delete as destructive and require `--force` or
   `safety.allow_destructive: true`; emit one aggregate W119 during plan before
   save and during authorized apply after fresh equality but before intent.
4. Freeze the ordered fresh planner actions and compare them exactly with the
   reviewed plan before durable intent.
5. Create `ManagedConnectorResourceDeletion` only from a completed durable
   Connector delete action.
6. Remove exactly the matching managed record and increment ownership serial
   once during atomic operation commit.
7. Prove tombstones, absence, assessments, manual changes, failed mutation,
   and duplicate/colliding claims cannot remove state.
8. Preserve every unrelated record byte-for-byte.

Exit gate: a fake-provider PostgreSQL command lifecycle proves plan, refusal
without destructive authorization, exact successful deletion, state/control
finalization, and safe `already_absent` re-planning.

### Slice 6: exact recovery without a desired artifact

Primary files:

- `src/streamt/deployer/recovery_observer.py`
- `src/streamt/deployer/recovery_plan.py`
- `src/streamt/cli/commands/state_cmd.py`
- `src/streamt/deployer/planner.py`
- `tests/unit/test_recovery_observer.py`
- `tests/unit/test_cli_state_recovery.py`
- `tests/unit/test_recovery_plan.py`

Tasks:

1. Resolve a removed Connector target from durable v3 action evidence, not
   from a current desired connector artifact or retained tombstone.
2. Re-derive the runtime binding and require exact backend/name identity before
   provider access.
3. Classify exact absence as candidate and exact durable-current presence plus
   reconstructed checksum as prior.
4. Reject every third fingerprint, malformed/partial observation, changed
   binding, changed prior state, or incompatible legacy action.
5. Permit `observed` to remove only the exact prior state record; permit
   `rolled_back` to retain it; preserve `abandoned_before_mutation` only for
   zero started progress.
6. Use the PostgreSQL operation lock and atomic state/control/history
   finalization for both resolutions.
7. Prove a DELETE-time 404 remains blocked until a separately reviewed exact
   absence recovery.

Exit gate: process death, timeout, accepted-delete/postcondition failure, and
unknown state commit each leave a durable blocker with one exact recoverable
classification or a stable fail-closed third state.

### Slice 7: PostgreSQL command and concurrency gates

Primary files:

- `tests/postgres/test_postgres_connector_removal_commands_real.py`
- `tests/postgres/test_postgres_state_command_failures_real.py`
- `tests/postgres/test_postgres_recovery_real.py`
- PostgreSQL 14/18 CI jobs

Required cases:

1. Seed one canonical managed Connector record plus unrelated records through
   the production PostgreSQL-v2 writer path.
2. Save an online reviewed v5 deletion plan under the address advisory lock.
3. Refuse apply without destructive authorization with zero provider mutation.
4. Apply with exact durable started/completed history and atomic record
   removal/control clear.
5. Prove two processes cannot plan/apply the same address concurrently and the
   loser never reaches Connect.
6. Terminate or lose the physical PostgreSQL lock session before and after
   provider mutation and verify existing lock-lost/unknown-outcome behavior.
7. Exercise DELETE-time 404 and response loss followed by reviewed recovery to
   exact absence.
8. Verify store ID, state address, serial, state checksum, control revision,
   action equality, and unrelated records throughout.
9. Run every case against PostgreSQL 14 and 18 with no admin-DSN fallback.
10. Cover mixed actions: first Connector delete completed and second failed,
    Connector deletes before/after non-Connector actions, mixed candidate and
    prior recovery targets, exact subset state removal, and preservation of
    not-started records.

Exit gate: the complete command path uses the production state factory,
session-affine lock, exact v2 writer/ACL, history, and recovery implementation.

### Slice 8: installed-wheel, real Connect, secrecy, and documentation gates

Primary files:

- `tests/package/connector_removal_wheel_smoke.py`
- `tests/integration/test_connect_e2e.py` or a focused strict-observer real test
- `.github/workflows/ci.yml`
- `docs/reference/yaml-schema.md`
- `docs/reference/cli.md`
- `docs/reference/support-matrix.md`
- `docs/guides/state-recovery.md`
- `docs/reference/release-notes.md`

Installed-wheel gate:

1. Build wheel and sdist, install the wheel into an isolated environment, and
   invoke only the installed `streamt` executable.
2. Use an exact PostgreSQL-v2 test store and a deterministic loopback fake
   Connect server implementing the strict GET/DELETE contract.
3. Seed ownership through public or production state-service boundaries, not
   checkout imports in the installed process.
4. Run plan, destructive refusal, authorized apply, and recovery cases.
5. Assert the installed process imports no `streamt` module from the checkout.
6. Bound subprocess time, output, server bodies, retries, and cleanup.

Real Connect gate:

1. Run against the pinned supported Connect image/API used by integration CI.
2. Create a target connector and an unrelated connector with deterministic
   unique names and exact configs.
3. Seed the target's canonical PostgreSQL managed record.
4. Execute the public reviewed removal workflow.
5. Prove the target becomes exactly absent, the unrelated connector remains
   byte-for-byte equal, and no topic/schema/external resource is deleted.
6. Exercise percent-encoded connector names where the provider supports them.
7. Clean up test fixtures directly and report bounded sanitized failures.

Documentation and secrecy gate:

- strict MkDocs build and link checks pass;
- every non-skipped YAML example parses through the strict project parser;
- CLI/help/reference/support/recovery/release docs state PostgreSQL-v2-only,
  reviewed-only, and absence-inert boundaries;
- repository-wide secret sentinels prove config, endpoints, DSNs, credentials,
  and raw response bodies never enter persisted or displayed evidence; and
- release notes make no claim for topic, schema, or Flink deletion.

Exit gate: installed artifacts and a real Connect server prove the same public
contract as source tests before the feature is documented as supported.

## Acceptance matrix

| Area | Required evidence | Primary tests |
| --- | --- | --- |
| DSL | Exact fields/default; unknown/null/secret fields rejected; duplicates deterministic | `test_connector_removal_dsl.py` |
| Compilation | Separate immutable artifact; checksum participation; no DAG/desired/state action | `test_compiler.py`, DSL tests |
| Absence inert | Removed model and state removal candidate never produce delete | `test_deployment_state.py`, planner artifact tests |
| Pure preflight | Exact cluster/backend/name/owner; managed only; legacy/adopted/collisions fail with zero HTTP | `test_connector_removal_preflight.py` |
| PostgreSQL only | Local, v1, unavailable, wrong writer/ACL, and fallback attempts fail before Connect | authorization and PostgreSQL tests |
| Planning | Exact checksum/present deletes; absent/mismatch/unowned outcomes match matrix | `test_connector_removal_planning.py` |
| Review | v5 exact actions and fingerprints; v1-v4 regeneration; all drift rejected | `test_plan_file.py`, action-wiring tests |
| Control | v3 evidence/history; v1/v2 compatibility; kind/action exclusivity | `test_state_backend.py` |
| CLI | No direct/dry-run/offline/target/select/partial path; destructive opt-in required | authorization and local-negative tests |
| Mutation | Exact observation, encoded name, one DELETE, bounded absence; 404 uncertain | managed-mutation tests |
| State | Only completed exact claim removes one record; serial and unrelated records exact | `test_deployment_state.py` |
| Recovery | Absent candidate, exact prior, third state, missing tombstone, response/commit uncertainty | recovery observer/CLI tests |
| Concurrency | Address lock spans plan save and complete apply; contenders and lock loss fail closed | PostgreSQL real tests |
| Secrecy | No raw config, endpoint, DSN, credentials, bodies, or unsanitized exceptions | unit, command, package sentinel tests |
| Installed package | Isolated executable completes PostgreSQL reviewed lifecycle and recovery | `connector_removal_wheel_smoke.py` |
| Real provider | Exact target absent; unrelated connector and non-Connector resources preserved | Connect integration gate |
| Documentation | Strict build, parser-backed examples, no overclaim | documentation CI |

## Hard deferrals

This implementation must not add a generic `resource_removals` collection,
generic provider delete dispatcher, or generic state-deletion claim.

- Kafka topic deletion waits for a separate data-loss, consumer-impact,
  asynchronous broker convergence, and protection specification.
- Schema Registry deletion waits for explicit soft/permanent mode, subject
  version/reference closure, compatibility state, and ordering semantics.
- Flink cancellation waits for stable provider job identity, exact artifact and
  cluster evidence, savepoint policy, and recoverable cancellation state.

Tests must assert that manual schema/topic/cancel change objects do not become
public lifecycle sources as a side effect of Connector work.

## Rollout gates

The feature remains undocumented as supported and the Phase 0 roadmap item
remains unchecked until all of the following pass together:

1. strict parser/compiler/schema tests;
2. provider-free identity, state, and collision tests;
3. reviewed-plan v5 and control/recovery v3 compatibility tests;
4. managed mutation and exact state-projection tests;
5. PostgreSQL 14 and 18 command, ACL, lock, history, failure, and recovery
   tests;
6. concurrent-process and terminated-session tests;
7. installed-wheel lifecycle and recovery smoke;
8. focused real Connect exact-deletion preservation gate;
9. repository secrecy scan;
10. full unit/integration/package test suite, Ruff, zero-error mypy, and strict
    MkDocs build.

Completing this plan establishes one additional explicit destructive resource
kind. It does not by itself complete broad deletion coverage for all managed
resources.

## Completion checklist

- [ ] Freeze new codes and negative workflow precedence.
- [ ] Add strict secret-neutral YAML and compiled artifact shapes.
- [ ] Add pure canonical binding/state/collision preflight.
- [ ] Add Connector action evidence and version migrations.
- [ ] Add exact checksum reconstruction and planning outcomes.
- [ ] Add managed observed DELETE plus bounded absence postcondition.
- [ ] Add PostgreSQL-v2-only CLI authorization and destructive warning.
- [ ] Add exact Connector-specific state projection.
- [ ] Add tombstone-independent reviewed recovery.
- [ ] Pass PostgreSQL 14/18 command and concurrency gates.
- [ ] Pass installed-wheel lifecycle/recovery gate.
- [ ] Pass focused real Connect deletion/preservation gate.
- [ ] Update existing reference documentation only after implementation passes.
- [ ] Pass secrecy, formatting, lint, typing, documentation, package, and full
      test gates.
