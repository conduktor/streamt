# Explicit Gateway removal implementation plan

## Status and decision

Status: planned. This plan defines the remaining ordinary, non-recovery delete
source for Package 6 of the
[Gateway normalized aggregate specification](2026-09-02-gateway-normalized-aggregate.md).
It does not mark Package 6 or Gateway adoption complete.

The decision is to add an explicit project-level lifecycle tombstone. A
Gateway delete candidate exists only when the current project contains that
tombstone. A rule disappearing from `models`, compiled `gateway_rules`, or any
partial selection never creates a delete candidate.

The tombstone carries the complete prior compiler-level Gateway artifact rather
than only a logical owner, rule name, and alias. This lets planning recompute
the existing `ManagedResourceRecord.artifact_checksum` and prove that the
declared provider rule namespace is the one represented by prior ownership
state. The effective backend remains derived from runtime configuration and is
never accepted from YAML.

This is a one-resource-at-a-time declarative removal contract that uses the
ordinary `plan` and `apply` transaction machinery. A future CLI convenience may
generate a tombstone, but no direct delete command becomes a second source of
mutation authority.

## Rationale

The current implementation already enforces the boundaries this plan must
preserve:

- ownership state reports absence as inert `RemovalCandidate` data and does not
  produce actions;
- state projection retains records absent from desired state and removes a
  Gateway record only for an exact explicit deletion claim;
- ordinary planning iterates desired `manifest.artifacts.gateway_rules` and has
  no ordinary delete source;
- a Gateway state record stores the logical resource URI, alias, ownership
  mode, artifact checksum, and canonical backend, but not provider
  `rule_name`;
- the normalized delete primitive accepts only a complete present aggregate;
- durable version-1 Gateway action evidence already distinguishes logical
  `resource_id` from provider `rule_name`, alias, and backend;
- reviewed plans bind the complete manifest checksum and exact ownership-state
  backend, store, address, serial, and checksum; and
- reviewed-plan apply cannot be combined with `--target` or `--select`.

A compact tombstone containing only three names would be explicit, but it
would not bind the supplied `rule_name` to the prior artifact represented by
the state checksum. This matters when the logical owner, provider rule name,
and AliasTopic name differ. Carrying the complete prior artifact makes the
existing checksum useful without turning ownership state into a provider
surface journal or requiring a state-format migration.

## Scope

This plan adds:

1. a strict lifecycle-removal DSL block;
2. a distinct compiled Gateway removal artifact;
3. pure, pre-provider validation against desired rules and prior state;
4. one shared live snapshot for desired and explicitly removed rules;
5. canonical ordinary Gateway delete changes;
6. exact reviewed-plan action evidence in plan format version 4;
7. full-plan and destructive-authorization enforcement; and
8. local, PostgreSQL, installed-wheel, and real-Gateway release gates.

The first release supports prior artifacts accepted by the existing strict
compiled Gateway parser. It does not broaden the set of Gateway configurations
that streamt can create or update.

## Non-goals

This plan does not:

- delete a resource because it is absent from the manifest;
- convert `LocalState.removal_candidates()` into deployment actions;
- add cluster-wide orphan or prefix discovery;
- infer `rule_name` from the logical owner, alias, state checksum, or
  interceptor prefix;
- add Gateway adoption;
- authorize a legacy `backend: conduktor-gateway` record;
- add state-only removal for a provider object that disappeared outside a
  durable streamt delete operation;
- treat the two Gateway list endpoints as a provider-atomic snapshot;
- allow offline plans to authorize deletion;
- allow targeted or tag-selected deletion; or
- expose a direct provider-mutating `streamt gateway delete` command.

## Public YAML contract

The exact initial spelling is:

```yaml title="stream_project.yml"
# streamt:skip -- planned lifecycle schema; not accepted until Slice 1 lands
apiVersion: streamt.dev/v1alpha1
project:
  name: payments

lifecycle:
  gateway_rule_removals:
    - logical_owner: orders_view
      prior_artifact:
        name: orders_rule
        virtualTopic: orders.public
        physicalTopic: raw.orders
        interceptors:
          - type: filter
            config:
              where: "region = 'us'"
```

An alias-only removal uses an explicit empty interceptor list:

```yaml
# streamt:skip -- partial planned lifecycle fragment
lifecycle:
  gateway_rule_removals:
    - logical_owner: customer_view
      prior_artifact:
        name: customer_rule
        virtualTopic: customers.public
        physicalTopic: raw.customers
        interceptors: []
```

`prior_artifact` deliberately uses the existing compiled-artifact field names.
Its `name`, `virtualTopic`, `physicalTopic`, and `interceptors` fields can be
copied from `generated/manifest.json`; the enclosing `ownership` field must not
be copied and is rejected. The compiler constructs ownership from the current
project and `logical_owner`.

### Typed schema

The equivalent strict model is:

```text
LifecycleConfig
  gateway_rule_removals: list[GatewayRuleRemovalDeclaration] = []

GatewayRuleRemovalDeclaration
  logical_owner: non-empty string without "/"
  prior_artifact: GatewayRulePriorArtifact

GatewayRulePriorArtifact
  name: non-empty canonical Gateway resource name
  virtualTopic: non-empty canonical Gateway resource name
  physicalTopic: non-empty canonical Gateway resource name
  interceptors: list[GatewayRulePriorInterceptor]

GatewayRulePriorInterceptor
  type: "filter" | "mask"
  config: the exact type-specific configuration accepted by
          parse_compiled_gateway_rule_artifact
```

All models use `extra="forbid"`. Null collections, implicit singleton
interceptors, provider plugin classes, provider scopes, endpoints, backend
identities, credentials, and an author-supplied ownership object are rejected.
The existing strict compiled-artifact parser remains the final canonical parser
for the reconstructed artifact.

The compiler reconstructs this exact artifact before hashing:

```json
{
  "name": "orders_rule",
  "virtualTopic": "orders.public",
  "physicalTopic": "raw.orders",
  "interceptors": [
    {
      "type": "filter",
      "config": {"where": "region = 'us'"}
    }
  ],
  "ownership": {
    "mode": "managed",
    "project": "payments",
    "type": "model",
    "name": "orders_view"
  }
}
```

`logical_owner` is the state identity, `prior_artifact.name` is the provider
rule and generated-interceptor namespace, and `prior_artifact.virtualTopic` is
the AliasTopic name. None may be substituted for another.

## Compiled manifest contract

Removal declarations compile into a separate collection:

```json
{
  "artifacts": {
    "gateway_rules": [],
    "gateway_rule_removals": [
      {
        "logicalOwner": "orders_view",
        "priorArtifact": {
          "name": "orders_rule",
          "virtualTopic": "orders.public",
          "physicalTopic": "raw.orders",
          "interceptors": [],
          "ownership": {
            "mode": "managed",
            "project": "payments",
            "type": "model",
            "name": "orders_view"
          }
        }
      }
    ]
  }
}
```

The removal artifact is part of the manifest checksum. It is not included in
`gateway_rules`, desired managed records, the DAG, or selection closure. It
cannot create or update provider state.

## Non-negotiable invariants

1. **Affirmative intent:** only one strict lifecycle tombstone can source an
   ordinary Gateway delete. Manifest or selection absence is inert.
2. **Separate identities:** logical owner, rule name, alias, backend, and
   generated interceptor locators remain distinct.
3. **Prior provenance:** when prior state exists, the reconstructed prior
   artifact checksum, resource URI, alias, backend, and ownership mode must all
   match exactly.
4. **Runtime-derived backend:** YAML never supplies an endpoint, vCluster, or
   backend identity. The effective project runtime produces the canonical
   binding.
5. **Legacy fail-closed:** `backend: conduktor-gateway` and every malformed or
   mismatched state claim fail before deployer construction or provider access.
6. **Whole-manifest collision safety:** desired rules and removal declarations
   are validated together before provider access. A collision on resource URI,
   logical owner, rule name, alias, or generated interceptor locator blocks the
   complete plan.
7. **One bounded observation:** any positive number of desired and removal
   targets use one shared Gateway snapshot: one AliasTopic list GET followed by
   one Interceptor list GET.
8. **Exact current aggregate:** deletion is planned only from the complete
   present alias plus exact rule-owned interceptor aggregate returned by that
   snapshot.
9. **No partial deletion:** every lifecycle removal requires a full,
   non-targeted reviewed plan. `--target` and `--select` fail before provider
   access when a removal declaration exists.
10. **Reviewed action equality:** the reviewed plan, fresh apply re-plan,
    durable `OperationIntent`, mutation dispatcher, and recovery path use the
    same ordered action identity and Gateway evidence.
11. **Explicit state removal:** ownership state is removed only from a
    successfully validated durable `delete` action. Provider absence by itself
    never removes state.
12. **No adoption by deletion:** a present target without exact prior ownership
    is blocked, even when its tombstone matches the provider surface.
13. **Secret neutrality:** YAML validation errors, plan files, operation
    control, history, logs, and CLI output never contain Gateway endpoints,
    credentials, raw provider response objects, raw transformed interceptor
    configuration, or SQL. The user-authored tombstone and generated manifest
    necessarily retain the prior compiler-level interceptor declaration, but
    downstream evidence carries only fingerprints, counts, and categories.
14. **Concurrency remains visible:** a 404 or changed aggregate after review is
    drift, not idempotent delete success.

## Pure preflight

Add a pure preflight value for every removal:

```text
ResolvedGatewayRuleRemoval
  resource_id
  logical_owner
  prior_artifact
  prior_artifact_checksum
  binding
  rule_name
  alias_name
```

Preflight runs after parsing, compilation, environment resolution, and an
authoritative state/control read, but before `make_gateway_deployer` or any
provider call. A live `streamt plan` containing a removal holds the state
operation lock from that read through pure preflight, live provider planning,
and atomic reviewed-plan save. This closes the control-marker race without
turning the read-only plan into a state mutation. It performs these steps in
durable declaration order:

1. Strictly parse every desired Gateway rule and removal `priorArtifact`.
2. Reconstruct managed ownership from project and `logical_owner`.
3. Resolve the effective Gateway binding from project runtime.
4. Build canonical desired identities and removal identities.
5. Reject duplicate removal resource IDs, logical owners, `(backend,
   rule_name)` locators, `(backend, alias)` locators, and generated interceptor
   locators.
6. Reject every desired/removal collision on those same identities.
7. Resolve `streamt://<project>/<environment>/gateway_rule/<logical_owner>`.
8. If a prior record exists, require:
   - `ownership == "managed"`;
   - exact canonical backend equality;
   - `physical_name == alias_name`;
   - no second prior record claiming `(backend, alias)`; and
   - `artifact_checksum == prior_artifact_checksum`.
9. Reject a legacy unbound backend even if every other field appears to match.
10. Return immutable resolved values. Do not construct provider clients, read
    Gateway collections, create plan changes, or mutate state.

The normal planner repeats or consumes this validated value defensively. It
must not accept an unvalidated dictionary supplied by another caller.

## Online planning algorithm

Online planning resolves the union of desired rules and explicit removals:

1. Verify the supplied Gateway deployer's binding equals the preflight binding.
2. Acquire one strict `observe_managed_gateway_snapshot()` result.
3. Verify the snapshot binding.
4. Memoize `snapshot.rule(rule_name, alias_name)` by the exact pair.
5. Plan desired rules with the existing normalized create/update/no-op path.
6. For each removal with exact prior ownership:
   - obtain the exact current aggregate;
   - require `current.exists is True`;
   - call `plan_managed_gateway_rule_deletion(current)`; and
   - bind the delete change to the tombstone's canonical `resource_id`.
7. For each removal without prior ownership:
   - absent current is an already-satisfied no-op recorded as a separate
     secret-neutral `GatewayRemovalAssessment`, never as an absent-to-absent
     `GatewayRuleChange`;
   - present current produces a blocking `requires_adoption` ownership
     requirement and no action.
8. Compute safety blockers and reviewed action evidence before returning.

The two collection GETs are sequential and not provider-atomic. Strict parsing
and exact current/desired matching make a mixed or third surface fail closed;
they do not convert the two endpoints into a transaction.

Offline planning may validate tombstone syntax and collisions, but it emits no
delete action. It reports a deterministic blocker stating that a live complete
Gateway aggregate and authoritative online state are required. Removal
assessments live on `DeploymentPlan` separately from normalized mutation
changes so blocker refresh and risk classification cannot accidentally erase
or reinterpret them.

## Reviewed-plan version 4

The existing reviewed resource diff is not sufficient for deletion review: its
Gateway `name` is the provider rule name and it does not independently expose
the logical resource URI, alias, or backend identity. Plan format version 4
adds a canonical ordered `actions` array derived from
`planner.planned_actions(deployment_plan)`.

For a Gateway deletion, the exact secret-neutral entry is:

```json
{
  "index": 0,
  "resource_id": "streamt://payments/prod/gateway_rule/orders_view",
  "action": "delete",
  "gateway_evidence": {
    "version": 1,
    "backend_identity": "conduktor-gateway:v1:p:sha256:<endpoint-fingerprint>",
    "rule_name": "orders_rule",
    "alias_name": "orders.public",
    "current": {
      "exists": true,
      "fingerprint": "sha256:<current-aggregate>",
      "managed_interceptor_count": 1
    },
    "desired": {
      "exists": false,
      "fingerprint": "sha256:<canonical-absence>",
      "managed_interceptor_count": 0
    }
  }
}
```

The reviewed plan checksum covers `actions`. Apply recompiles, rereads exact
state, repeats the live plan, regenerates the ordered actions, and compares the
entire version-4 live evidence before writing `OperationIntent`. The durable
intent must reuse equal action values rather than independently rediscovering
identities.

The global plan format advances to version 4. Versions 1 through 3 cannot
authorize any apply after that bump, including lifecycle removals, and are
rejected with the existing regenerate-plan guidance. Version 4 remains strict
about unknown or missing fields.

## CLI authorization contract

The only supported mutation workflow is:

```bash
streamt plan --project-dir . --env prod --out .streamt/gateway-removal.plan.json
# Review the exact resource_id, rule_name, alias_name, backend fingerprint,
# current aggregate fingerprint/count, desired absence, and destructive risk.
streamt apply --project-dir . --env prod \
  --plan .streamt/gateway-removal.plan.json --force
```

Lifecycle removals require an online reviewed plan regardless of environment
defaults. Existing environment confirmation and remote-state policies still
apply. When the fresh plan contains an actual delete, `--force` or an explicit
environment destructive policy is also required; a satisfied no-op tombstone
does not require destructive authorization. The reviewed plan never waives
authorization for a real delete.

The following fail before provider access:

- direct apply with a removal declaration and no `--plan`;
- `--dry-run` direct apply offered as deletion authorization;
- `--target` or `--select` with a removal declaration;
- an offline reviewed plan;
- plan format version 1, 2, or 3;
- a stale manifest, environment, state, or pure target identity; and
- any pending operation-control recovery marker.

Fresh aggregate drift can only be discovered by the bounded two-list Gateway
observation. It fails after those reads but before any provider mutation or
durable intent write.

## Lifecycle and edge cases

| Prior ownership state | Fresh aggregate | Result |
| --- | --- | --- |
| Exact canonical record and matching prior checksum | Present | Plan one exact destructive delete. |
| Exact canonical record and matching prior checksum | Absent | Block as state/provider drift. Do not emit absent-to-absent delete or remove state. |
| Exact record but checksum differs | Not read | Fail preflight; the tombstone is not the last-applied artifact. |
| Exact record but backend or alias differs | Not read | Fail preflight as ownership mismatch. |
| Legacy unbound record | Not read | Fail closed and require explicit migration/reconciliation. |
| No prior record | Absent | Tombstone is already satisfied; visible no-op, no state write. |
| No prior record | Present | Block as unowned/requires adoption; no mutation. |
| Another record claims the alias/backend | Not read | Fail preflight as an ambiguous provider claim. |
| A desired rule claims the owner, rule, alias, or interceptor locator | Not read | Fail the complete plan as desired/removal collision. |
| Reviewed current changed before apply | Present, but different | Reject the reviewed plan as stale. |
| Provider returns 404 after reviewed presence | Concurrent deletion | Mark the action failed/recovery-required; do not call it idempotent success. |
| Delete succeeds and state commit succeeds | Absent, record removed | Tombstone may remain as a safe satisfied no-op until a cleanup commit removes it. |
| Delete succeeds and state commit is uncertain | Unknown | Existing reviewed recovery decides from durable action evidence and history. |
| External actor recreates after successful state removal | Present, no record | Tombstone blocks as unowned; it cannot delete the recreated resource. |

A live-absent aggregate with an exact prior record intentionally remains
blocked. Adding a state-only `forget` transition would require its own explicit
action verb, durable evidence, review, and recovery contract; it is not smuggled
into this delete workflow.

## Mutation, state, and recovery reuse

No new provider mutation primitive is needed. The implementation reuses:

- `plan_managed_gateway_rule_deletion(current)`;
- canonical `GatewayRuleChange(action="delete")` validation;
- `planner.planned_actions()` Gateway action evidence;
- the exact interceptor-then-alias deletion journal;
- `ManagedGatewayResourceDeletion` state projection;
- pre-mutation `OperationIntent` persistence; and
- reviewed recovery's exact current/desired classification.

State removal remains downstream of one successful durable delete action. A
manifest tombstone is only the candidate source; it is not itself permission to
edit state.

## Migration and compatibility

### Project DSL

`lifecycle` is an optional additive field under `streamt.dev/v1alpha1`.
Existing projects parse unchanged. The alpha API version does not need to
change, but generated JSON schema, configuration reference, examples, and
strict parser tests must change together.

### Compiled manifest

`gateway_rule_removals` is an additive artifact collection. It is included in
the existing manifest checksum and ignored by older artifacts only where the
consumer already treats artifact kinds generically. Every lifecycle consumer
in the current release must be upgraded together; silent dropping in planner or
selection code is forbidden.

### Ownership state

No `CURRENT_STATE_VERSION` bump and no PostgreSQL schema migration are required.
The complete prior artifact reconstructs the existing checksum, while alias and
backend use existing record fields.

An existing canonical record is eligible only when its checksum can be
reproduced exactly. The safe migration for a still-declared rule is:

1. apply the existing rule once with the current release and canonical Gateway
   binding;
2. verify the resulting state record;
3. replace the desired model with the generated prior-artifact tombstone; and
4. create a fresh reviewed removal plan.

An already-removed rule whose prior artifact cannot be reconstructed, or a
legacy unbound record, is not auto-migrated. It needs a separately designed
explicit state reconciliation or later adoption workflow.

### Operation control and recovery

No control or recovery-plan version bump is required. The shipped version-1
Gateway evidence already represents present-to-absent deletion with exact
rule, alias, backend, current fingerprint/count, and desired absence.

### Reviewed plans

`PLAN_FILE_VERSION` advances from 3 to 4. Existing plan files remain readable
only for an actionable regeneration error; they do not authorize apply under
the new action-evidence contract.

## Error and secrecy contract

All new errors use fixed semantic descriptions. They may include logical
resource URI, rule name, and alias only when necessary for user action. They do
not include:

- the Gateway admin URL or request path;
- usernames, passwords, headers, tokens, or TLS material;
- raw provider JSON;
- raw interceptor provider configuration;
- physical topic contents; or
- state database DSNs and credentials.

The plan may display endpoint fingerprint, aggregate fingerprints, interceptor
count, and stable drift categories. Redaction is defense in depth, not
permission to place secret-bearing values into presentation or persistence
models.

## Ordered implementation slices

### Slice 1: freeze schema and pure compilation

Primary files:

- `src/streamt/core/models.py`
- `src/streamt/core/parser.py`
- `src/streamt/core/validator.py`
- `src/streamt/compiler/manifest.py`
- `src/streamt/compiler/compiler.py`
- generated schema and configuration documentation

Deliver strict YAML models, immutable compiled removal artifacts, ownership
injection, checksum reconstruction, duplicate declaration validation, and
manifest serialization. No runtime behavior changes in this slice.

### Slice 2: pure identity and prior-state preflight

Primary files:

- `src/streamt/compiler/gateway_artifact.py`
- `src/streamt/deployer/gateway.py`
- `src/streamt/deployer/planner.py`
- shared CLI preflight helper used by `plan` and `apply`

Resolve desired and removed targets together. Prove exact state checksum,
alias, backend, owner, and collision behavior before deployer construction or
provider access. Preserve declaration order after validation.

### Slice 3: one-snapshot planning

Primary files:

- `src/streamt/deployer/planner.py`
- `src/streamt/deployer/gateway.py`

Extend the existing desired/recovery snapshot union with ordinary removal
targets. Emit only canonical present-surface delete changes. Add absent/no-state
no-op and ownership/drift blockers without adding a new mutation primitive.

### Slice 4: reviewed-plan version 4

Primary files:

- `src/streamt/deployer/plan_file.py`
- `src/streamt/cli/commands/plan.py`
- `src/streamt/cli/commands/apply.py`

Persist exact ordered actions, verify them on fresh apply, and reuse equal
values for durable intent. Add version migration errors and secret-scanning
tests.

### Slice 5: CLI safety and durable apply

Primary files:

- `src/streamt/cli/commands/apply.py`
- `src/streamt/deployer/state.py`
- existing operation-control and recovery integration points

Require full online reviewed workflow and destructive authorization. Reuse the
existing state deletion projection and prove tombstone absence alone cannot
remove a record.

### Slice 6: durable command integration

Add local and PostgreSQL v2 command tests for reviewed delete, interruption,
recovery, state commit, conflict, and history. Exercise logical owner differing
from provider rule name and alias.

### Slice 7: packaging, real Gateway, and docs

Add installed-wheel CLI coverage, one focused Gateway 3.15 exact deletion case,
strict documentation validation, and final compatibility/secrecy audit.

Slices are ordered. In particular, no provider mutation wiring lands before
pure identity/state preflight, and no release claim lands before durable local
and PostgreSQL recovery gates.

## Acceptance matrix

| Area | Required evidence | Primary tests |
| --- | --- | --- |
| DSL strictness | Exact spelling; empty list default; missing fields; unknown fields; null collections; author ownership/backend/endpoint rejected | `tests/unit/test_parser.py`, new `tests/unit/test_gateway_removal_dsl.py` |
| Compilation | Exact prior artifact and injected ownership; checksum equals last desired artifact; removal separate from desired rules/DAG | `tests/unit/test_compiler.py`, `tests/unit/test_compiled_gateway_artifact.py` |
| Explicit-only source | Removing a model or manifest artifact produces no delete; inert state removal candidates remain inert | `tests/unit/test_deployment_state.py`, `tests/unit/test_planner_gateway_artifacts.py` |
| Preflight | Owner/rule/alias divergence; checksum/backend/alias/ownership mismatch; legacy record; duplicate and desired/removal collisions all fail with zero Gateway reads | `tests/unit/test_planner_gateway_artifacts.py`, new `tests/unit/test_gateway_removal_preflight.py` |
| Selection | Tombstone with `--target` or `--select` fails before deployer/provider access; unrelated selected apply remains non-destructive | `tests/unit/test_deployment_safety.py`, new CLI removal tests |
| Bounded observation | One removal, many removals, and mixed desired/removal sets each make exactly two sequential list GETs | `tests/unit/test_planner_gateway_artifacts.py`, `tests/unit/test_gateway_runtime_foundation.py` |
| Planning | Exact present aggregate produces delete; exact-prior/absent blocks; no-prior/absent no-ops; no-prior/present requires adoption | new `tests/unit/test_planner_gateway_removal.py` |
| Reviewed plan | Version 4 contains exact ordered action evidence; state/manifest/runtime/action/aggregate drift rejects; versions 1-3 cannot authorize | `tests/unit/test_plan_file.py`, `tests/unit/test_safety_blockers.py` |
| Secrecy | Endpoint, credentials, provider configuration, SQL, and physical addresses absent from plans, control, history, errors, and logs | `tests/unit/test_gateway_plan_secrecy.py`, removal CLI tests |
| Exact mutation | Interceptors and alias deleted from reviewed current surface only; 404/result drift fails; no cross-rule deletion | `tests/unit/test_gateway_managed_mutation.py`, `tests/unit/test_planner_gateway_mutation.py` |
| State | Only successful durable delete removes exact record; desired collision and duplicate claims reject; tombstone/no-op never edits state | `tests/unit/test_deployment_state.py` |
| Recovery | Interrupted ordinary deletion reuses exact action evidence and resolves completed/rolled-back/drift outcomes | `tests/unit/test_cli_state_recovery.py`, `tests/unit/test_recovery_observer.py` |
| Local command E2E | Plan-save/load/apply/state/history/retry lifecycle, including owner != rule != alias | new local CLI removal scenario |
| PostgreSQL v2 | Same reviewed delete/recovery semantics through production factory, lock, writer, history, and clear | `tests/postgres/test_postgres_recovery_commands_real.py`, ordinary factory command tests |
| Installed wheel | Isolated `streamt plan` and `apply --plan --force` complete the lifecycle | packaging workflow smoke test |
| Real Gateway | Gateway 3.15 exact alias/interceptor delete, verified empty post-state, no residue | `tests/integration/test_gateway_e2e.py` |
| Documentation | New YAML parses; links resolve; strict MkDocs build passes | documentation CI |

## Rollout and release gates

The feature remains hidden or explicitly experimental until all of these hold:

1. Strict DSL and compiler tests pass without broadening desired Gateway
   support.
2. Every identity, state, legacy, checksum, collision, and partial-selection
   failure occurs before provider access.
3. Mixed desired/removal planning proves exactly one two-list snapshot, while
   satisfied no-ops and drift use a distinct immutable removal assessment.
4. Reviewed-plan version 4 binds and revalidates exact ordered action evidence.
5. Direct, targeted, selected, offline, stale, and unowned deletion attempts
   are blocked.
6. Mutation, state projection, and recovery pass for local and PostgreSQL v2
   backends.
7. Installed-wheel tests exercise the public commands.
8. A focused real Gateway 3.15 test proves exact deletion and cleanup.
9. Full unit, integration, lint, formatting, zero-error mypy, packaging, secret
   scan, and strict documentation gates pass together.
10. Gateway recovery documentation remains accurate and Gateway adoption stays
    unsupported.

Package 6 may call its ordinary delete-source boundary complete only after all
ten gates pass. Gateway adoption remains separately gated by the alias-only
adoption plan.

## Completion checklist

- [ ] Add strict lifecycle-removal YAML and compiled artifact models.
- [ ] Prove prior-artifact checksum equality without changing state format.
- [ ] Fail legacy, mismatched, colliding, and partial targets before provider
      access.
- [ ] Plan desired and explicit removal targets from one shared snapshot.
- [ ] Emit deletion only from a complete present aggregate.
- [ ] Add safe no-prior/absent no-op and drift/ownership blockers.
- [ ] Add reviewed-plan version 4 exact action evidence.
- [ ] Require full online reviewed plan plus destructive authorization.
- [ ] Reuse exact durable mutation, state projection, and recovery paths.
- [ ] Pass local and PostgreSQL v2 command E2E.
- [ ] Pass installed-wheel and real Gateway 3.15 gates.
- [ ] Pass documentation, secrecy, lint, typing, unit, integration, and package
      checks.
