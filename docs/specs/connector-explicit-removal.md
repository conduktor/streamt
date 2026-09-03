# Explicit Kafka Connect connector removal

## Status

This is the normative target contract for explicit Connector removal. It is
not an implementation claim. Until every release gate in the accompanying
[implementation plan](../plans/2026-09-03-connector-explicit-removal.md) passes,
Connector lifecycle deletion remains unsupported.

The words **MUST**, **MUST NOT**, **REQUIRED**, **SHOULD**, and **MAY** are used
as normative requirements.

## Decision

streamt will support deletion of one exact, previously managed Kafka Connect
connector through a project lifecycle tombstone and the ordinary reviewed
`plan`/`apply` transaction. The first supported release is deliberately
PostgreSQL deployment-state schema version 2 only.

An actionable removal MUST have all of the following:

1. an explicit, strict, secret-neutral lifecycle tombstone;
2. one exact canonical `ownership == "managed"` PostgreSQL state record;
3. one exact runtime-derived Connect cluster binding;
4. one complete fresh managed Connector observation;
5. a full online reviewed plan with exact secret-neutral action evidence;
6. destructive authorization;
7. a PostgreSQL session advisory lock held through re-planning, durable intent,
   provider mutation, postcondition observation, and atomic state commit; and
8. exact reviewed recovery for every uncertain mutation or commit outcome.

Removing a sink model, omitting a connector artifact, selecting another model,
or observing an absent provider resource MUST NOT create deletion authority.

## Scope

The first release supports connectors that can already enter the strict bound
Connector planning and state contract:

- one connector name encoded as one Connect REST path segment;
- the explicitly named default Connect cluster;
- the canonical version-1 Connect backend identity;
- one strict `GET /connectors/<percent-encoded-name>` observation;
- a reconstructible compiled `ConnectorArtifact` whose exact checksum matches
  the managed PostgreSQL ownership record; and
- the current `model` ownership type.

The workflow deletes the Connector object only. It does not delete Kafka
topics, records written by the connector, consumer offsets, schemas, external
systems, credentials, connector plugins, or any provider object inferred from
connector configuration.

## Non-goals and hard boundaries

This specification does not:

- infer deletion from manifest absence or `LocalState.removal_candidates()`;
- support local deployment state, another future remote backend, PostgreSQL
  schema version 1, or a PostgreSQL store whose exact version-2 writer and ACL
  contract cannot be proved;
- add a direct `streamt connector delete` command;
- allow direct apply, direct dry-run, offline planning, partial planning,
  `--target`, or `--select` to authorize a removal;
- delete an adopted, external, legacy-unbound, missing, ambiguous, or
  checksum-mismatched Connector;
- persist a raw connector configuration in the tombstone, reviewed plan,
  operation control, history, output, logs, or recovery plan;
- treat a Connect `DELETE` as transactional with PostgreSQL;
- claim exclusion against a non-streamt Connect writer; or
- enable topic deletion, Schema Registry subject deletion, or Flink job
  cancellation. Those resource kinds are hard-deferred below.

## Identity model

The logical ownership identity, provider name, cluster alias, and canonical
backend identity are distinct values:

```text
resource_id
  streamt://<project>/<environment>/connector/<logical_owner>

provider locator
  (<backend_identity>, <connector_name>)

backend_identity
  kafka-connect:v1:<cluster_alias>:sha256:<normalized-endpoint-digest>
```

`logical_owner` selects the state record. `connector_name` selects the Connect
object. `cluster_alias` selects one configured runtime cluster. The endpoint
digest binds that alias to the normalized runtime endpoint without exposing the
endpoint. None of these values may be substituted for another.

The tombstone MUST contain the cluster alias explicitly. Default-cluster
inference is forbidden for removal, even if omission is accepted for desired
Connector artifacts. In the first release, that explicit alias MUST equal
`runtime.connect.default`; a configured non-default alias fails before provider
construction. Alias-indexed deployer construction, grouping, closing, and
recovery are separate work. YAML MUST NOT contain an endpoint, endpoint
fingerprint, backend identity, state address, ownership object, or credential.

The collision key for the provider object is
`(normalized_endpoint_digest, connector_name)`, not the alias-bearing backend
identity. Two aliases that normalize to the same endpoint address the same
Connector namespace. Desired artifacts, tombstones, and prior state MUST be
checked against this alias-independent key even though the canonical evidence
continues to retain the exact default alias.

## Public YAML contract

The exact additive project shape is:

```yaml title="stream_project.yml"
# streamt:skip -- normative planned schema; unsupported until implementation lands
apiVersion: streamt.dev/v1alpha1
project:
  name: payments

lifecycle:
  connector_removals:
    - logical_owner: archive_orders
      name: archive-orders-sink
      cluster: primary-connect
```

The strict typed shape is:

```text
LifecycleConfig
  connector_removals: list[ConnectorRemovalDeclaration] = []

ConnectorRemovalDeclaration
  logical_owner: 1..128 control-free characters without "/"
  name: 1..256 control-free characters
  cluster: 1..128 control-free characters naming runtime.connect.default
```

All three fields are required within each entry. Models MUST use
`extra="forbid"`. Null collections, null fields, empty or whitespace-only
strings, implicit singleton objects, and author-supplied `config`,
`connector_class`, `topics`, `ownership`, `backend`, `endpoint`, or fingerprint
fields MUST be rejected. Unicode control and surrogate code points are
forbidden. The collection is capped at 256 declarations, and the resolved
canonical resource ID MUST fit the existing 512-character durable-action
boundary. Repository-wide duplicate-YAML-key detection is not introduced by
this slice; the strict typed value produced by the existing YAML loader is the
only input to removal compilation.

Declarations MUST be unique by canonical `resource_id` and provider locator.
The complete project MUST reject a desired Connector and a removal declaration
that claim the same logical resource or provider locator. It MUST also reject
two declarations that reuse a logical owner or `(cluster, name)` pair.

The tombstone contains no prior artifact because Connector configuration may
contain credentials. Exact prior-artifact proof is reconstructed from the
strict live observation and compared with authoritative state instead.

## Compiled manifest contract

Non-empty declarations compile into a separate artifact collection:

```json
{
  "artifacts": {
    "connectors": [],
    "connector_removals": [
      {
        "logicalOwner": "archive_orders",
        "name": "archive-orders-sink",
        "cluster": "primary-connect"
      }
    ]
  }
}
```

`connector_removals` is included in the manifest checksum but is not included
in `connectors`, desired managed records, the DAG, model selection closure, or
provider deployment requirements. The collection SHOULD be omitted when empty
so projects without removals retain their prior manifest checksum.

Compilation validates syntax and deterministic ordering only. It MUST NOT read
deployment state, resolve an endpoint, construct a provider client, observe a
connector, or emit a `ConnectorChange`.

## Remote-state authority and locking

Connector removal intrinsically requires `deployment_state.backend: postgres`
and an exact PostgreSQL schema-version-2 ordinary writer. This requirement does
not depend on `safety.require_remote_state`; setting that option to `false`
cannot enable local removal. `--force` cannot bypass it.

Both online plan creation and apply MUST use the configured PostgreSQL state
service. Plan MUST hold the address advisory lock from its authoritative
state/control observation through target preflight, live Connect observation,
ordered action creation, and atomic reviewed-plan file replacement. It MUST
then release the lock without changing ownership or operation control.

Apply MUST reacquire the same address lock and hold it from authoritative
state/control observation through fresh compilation, fresh live re-planning,
reviewed-plan equality, durable intent and progress, managed Connector
deletion, absence confirmation, atomic ownership/control/history commit, and
verified lock release. Existing lock timeout, lock loss, unknown commit, and
release-after-commit semantics remain mandatory.

A pending `in_progress` or `recovery_required` control record MUST block plan
creation and apply before Connect access. Failure to initialize or strictly
read PostgreSQL MUST never fall back to local or empty state.

## Provider-free preflight

After strict compilation and one locked authoritative state/control read, but
before Connect client construction or provider access, preflight MUST:

1. resolve each declaration to its canonical logical `resource_id`;
2. require the explicit alias to equal `runtime.connect.default`, resolve that
   cluster through project runtime configuration, and reject an unknown or
   non-default alias with `E209_INVALID_CLUSTER_REF`;
3. derive the canonical backend identity from that cluster's normalized
   endpoint;
4. reject malformed, duplicate, desired/removal-colliding, or cross-project
   identities;
5. find at most one prior record for both `resource_id` and the
   alias-independent `(normalized_endpoint_digest, connector_name)` provider
   target;
6. when a prior record exists, require `ownership == "managed"`, exact
   `physical_name == name`, and exact canonical backend equality;
7. reject `backend: kafka-connect`, every other legacy backend, an adopted
   record, or a second state record claiming the provider locator; and
8. return one immutable `ResolvedConnectorRemoval` without creating an action.

The pure resolved shape is:

```text
ResolvedConnectorRemoval
  resource_id
  logical_owner
  connector_name
  binding: ConnectClusterBinding
  prior_record: ManagedResourceRecord | None
```

The state record's artifact checksum cannot be proved until the provider has
returned a complete config. Preflight MUST still finish every identity and
collision check possible without provider data first.

## Strict observation and prior-artifact reconstruction

Planning MUST use `observe_managed_connector(name)`, which performs exactly one
bounded, redirect-disabled resource GET and returns an immutable
`ManagedConnectorObservation`. List discovery, `/config` plus `/status`
assembly, task status, worker identity, traces, and plugin metadata are not
accepted evidence.

For a present observation, the config MUST contain exact scalar values for the
reserved `name`, `connector.class`, and `topics` keys. Planning reconstructs:

```text
ConnectorArtifact
  name = tombstone.name
  connector_class = observed config["connector.class"]
  topics = observed config["topics"].split(",")
  cluster = tombstone.cluster
  config = observed config excluding name, connector.class, and topics
  ownership = {
    mode: "managed",
    project: current project,
    type: "model",
    name: tombstone.logical_owner
  }
```

No trimming, case folding, scalar coercion, default insertion, or key omission
is allowed beyond the established compiled Connector parser. The reconstructed
artifact MUST pass that parser, and `artifact_checksum(artifact.to_dict())`
MUST equal the exact prior state checksum before a delete can be planned.

A provider that masks, expands, drops, or otherwise changes config such that
the prior checksum cannot be reconstructed is unsupported for removal and
fails closed. The user cannot work around this by copying config into YAML.

## Planning outcomes

Planning produces a delete only for an exact managed state record and a
matching complete present observation.

| Prior PostgreSQL record | Fresh observation | Result |
| --- | --- | --- |
| Exact managed locator and checksum | Present and reconstructs checksum | One destructive `ConnectorChange(action="delete")`. |
| Exact managed locator | Absent | Blocking `state_provider_drift`; retain state. |
| Exact managed locator | Present but checksum differs | Blocking `state_provider_drift`; retain state. |
| Adopted, external, legacy, mismatched, or ambiguous record | Not read | Fail provider-free preflight. |
| No prior record | Absent | Visible `already_absent` assessment; no provider action or ownership-state change. |
| No prior record | Present | Blocking `ownership_required`; no mutation. |
| Desired Connector collision | Not read | Fail the complete plan. |

Non-actionable results are represented separately as immutable,
secret-neutral `ConnectorRemovalAssessment` values:

```text
ConnectorRemovalAssessment
  resource_id
  logical_owner
  connector_name
  backend_identity
  status: "already_absent" | "state_provider_drift" | "ownership_required"
```

Offline plans do not produce assessments: a project containing any Connector
removal is rejected as an unsupported authorization path.

## Reviewed-plan and durable evidence

The reviewed plan format advances from version 4 to version 5. Every version-5
action has exact keys `index`, `resource_id`, `action`, `gateway_evidence`, and
`connector_evidence`. The two evidence fields are mutually exclusive. Existing
non-Connector actions serialize `connector_evidence: null`.

An actionable removal has this exact secret-neutral action:

```json
{
  "index": 0,
  "resource_id": "streamt://payments/prod/connector/archive_orders",
  "action": "delete",
  "gateway_evidence": null,
  "connector_evidence": {
    "version": 1,
    "backend_identity": "kafka-connect:v1:primary-connect:sha256:<endpoint-digest>",
    "connector_name": "archive-orders-sink",
    "prior_artifact_checksum": "sha256:<managed-artifact-digest>",
    "current": {
      "exists": true,
      "fingerprint": "sha256:<bound-present-observation>"
    },
    "desired": {
      "exists": false,
      "fingerprint": "sha256:<bound-absence-observation>"
    }
  }
}
```

The evidence version is outside both surface fingerprints. `current.fingerprint`
MUST equal the existing `ManagedConnectorObservation.fingerprint`, whose exact
canonical preimage contains only alias-bearing backend identity, connector
name, exact presence, and canonical exact config. `desired.fingerprint` MUST
equal the same observation fingerprint algorithm applied to an exact absent
observation for that binding and name. Raw config MUST NOT be stored beside the
fingerprint. Golden-vector tests freeze both preimages and digests.

The plan checksum covers the complete ordered action. Fresh apply re-planning,
the reviewed plan, the durable `OperationIntent`, mutation dispatcher, state
projection, and recovery MUST use equal action values. Reconstructing a similar
action independently at mutation time is forbidden.

Operation control advances from version 2 to version 3 with the same nullable
`connector_evidence` field. Recovery-plan format advances from version 2 to
version 3. Every reader MUST explicitly accept versions 1, 2, and 3. Clear v2
control remains readable by ordinary plan/apply/adopt and upgrades only on a
subsequent write; active v1/v2 operations retain their exact legacy recovery
semantics. Neither legacy version can represent or authorize a Connector
delete, and their serialized bytes/checksums do not change. Reviewed plan
versions 1 through 4 cannot authorize apply after the global version-5 bump and
receive regeneration guidance.

## CLI authorization

The only supported workflow is:

```bash
streamt plan --project-dir . --env prod \
  --out .streamt/connector-removal.plan.json

# Review the resource, cluster/backend fingerprint, current fingerprint,
# desired absence, state binding, ordered action, and destructive risk.
streamt apply --project-dir . --env prod \
  --plan .streamt/connector-removal.plan.json \
  --confirm-env prod --force
```

`safety.allow_destructive: true` may replace `--force`; it does not replace the
reviewed plan, environment confirmation, PostgreSQL authority, or exact state
and provider evidence. A tombstone assessed `already_absent` produces no
delete and requires no destructive override.

The following MUST fail before Connect access:

- direct apply, including `--dry-run`, with any Connector tombstone;
- `apply --target`, `apply --select`, or any future partial-selection option;
- offline plan creation or an offline reviewed plan;
- a reviewed plan using format version 1 through 4;
- local state or any non-PostgreSQL-v2 state authority;
- stale manifest, runtime, state, control, action, or provider identity;
- pending recovery; and
- missing or mismatched Connect cluster configuration.

## Managed mutation protocol

The bare `delete_connector(name)` primitive is not sufficient authorization.
The lifecycle path MUST call:

```text
delete_managed_connector(
  current: ManagedConnectorObservation,
) -> Literal["deleted"]
```

The method MUST require a complete present exact observation whose binding
equals the deployer's current binding. It MUST NOT accept a string, state
record, tombstone, dictionary, or absent observation.

After durable `progress_started`, the method MUST:

1. perform one immediate strict resource observation;
2. require it to equal the reviewed `current` observation exactly;
3. issue one redirect-disabled, bounded
   `DELETE /connectors/<percent-encoded-name>`;
4. require exactly HTTP 204 with an empty response body;
5. poll the same strict resource observation with a bounded attempt count and
   deadline until exact absence is returned; and
6. return only the literal `"deleted"` after that postcondition.

The DELETE request is direct and non-retrying. After `progress_started`, a 404,
redirect, any non-204 status, nonempty 204 body, timeout, connection loss,
malformed response, or missing absence postcondition has the single stable
`E428_CONNECTOR_REMOVAL_DRIFT` uncertain outcome. It MUST produce failed
durable progress and `recovery_required`; it MUST NOT remove ownership state,
retry DELETE, or continue to a later runtime action. Existing transport codes
remain available before durable mutation begins, but never downgrade an
uncertain started delete.

The pre-delete GET narrows but cannot eliminate the race with an external
Connect writer because the API offers no conditional delete transaction. This
TOCTOU boundary MUST remain documented. streamt's PostgreSQL advisory lock
serializes streamt writers only.

## Exact state projection

State projection uses an exact Connector-specific value:

```text
ManagedConnectorResourceDeletion
  resource_id
  backend_identity
  connector_name
  prior_artifact_checksum
```

It is constructed only from a successfully completed durable Connector delete
action with valid `ConnectorActionEvidence`. Projection MUST require one exact
prior managed record matching all four fields, no duplicate resource or
provider locator, and no desired record claiming either identity.

Only that record is removed. Unrelated schema, topic, Flink, Connector, and
Gateway records remain byte-for-byte equal. The logical ownership serial
increments exactly once. A tombstone, live absence, plan assessment, provider
response, or manually constructed `ConnectorChange` cannot remove state.

PostgreSQL atomically commits the replacement ownership payload, clears
control, and appends state and operation history. No ownership-state or
PostgreSQL schema migration is required.

## Recovery

Recovery MUST work from the durable version-3 action even if the lifecycle
tombstone or sink model is no longer in the current project. It revalidates the
project/environment, exact PostgreSQL store and address, runtime-derived
binding, action identity, prior state record, and evidence before Connect
access.

For a blocked Connector delete:

| Fresh exact observation | Allowed classification |
| --- | --- |
| Absent | Completed candidate. `observed` removes the exact prior state record and clears control. |
| Present with the exact durable current fingerprint and reconstructible prior checksum | Prior candidate. `rolled_back` retains state and clears control. |
| Present with any other fingerprint | Third state; no resolution is permitted. |
| Partial, malformed, oversized, redirected, unauthorized, or unavailable | Observation failure; retain blocker. |

`abandoned_before_mutation` remains legal only when durable progress proves no
runtime action started. A DELETE-time 404 may later resolve as completed only
through an explicit reviewed recovery plan proving exact absence. The failing
apply itself never calls that 404 success.

Recovery MUST use the normal PostgreSQL advisory lock and version-2 writer,
strictly compare state/control revisions, and atomically finalize state,
control, and history. Local recovery cannot resolve a Connector removal.

Recovery tests MUST cover mixed ordered actions: one completed Connector delete
followed by a failed Connector delete, Connector deletes before and after
non-Connector actions, mixed candidate/prior targets, exact subset state
removal, and preservation of records for actions that never started.

## Errors and warnings

The implementation reuses these existing structured codes:

| Code | Connector-removal use |
| --- | --- |
| `E501_PARSE_ERROR` | Invalid strict YAML shape. |
| `E205_CONNECT_REQUIRED` | No usable Connect runtime is configured. |
| `E209_INVALID_CLUSTER_REF` | Explicit alias is unknown or is not the configured default. |
| `E408_PLAN_FILE_INVALID` | Unsupported or malformed reviewed-plan version/shape. |
| `E409_PLAN_STALE` | Manifest, runtime, state, action, or fresh observation differs from review. |
| `E417_SAFETY_BLOCKED` | A removal assessment blocks apply. |
| `E418_REVIEWED_PLAN_REQUIRED` | Direct, partial, selected, targeted, or offline workflow. |
| `E419_STATE_RECOVERY_REQUIRED` | An unfinished operation already blocks the address. |
| `E420_STATE_BACKEND_UNAVAILABLE` | Configured PostgreSQL is v1, incompatible, unavailable, or cannot prove its exact v2 authority. |
| `E421_REMOTE_STATE_REQUIRED` | Local or another non-PostgreSQL backend is selected. |
| `E422_STATE_LOCK_TIMEOUT` | The PostgreSQL address lock cannot be acquired in time. |
| `E423_STATE_LOCK_LOST` | Lock ownership is lost before finalization. |
| `E424_STATE_CONFLICT` | Locked state/control preconditions change. |
| `E425_STATE_UNKNOWN_OUTCOME` | Ownership/control finalization cannot be confirmed. |
| `E426_STATE_RELEASE_FAILED_AFTER_COMMIT` | Commit is verified but lock release cannot be verified. |
| `E503_ENVIRONMENT_ERROR` | Destructive or environment confirmation is absent. |
| `E407_DEPLOY_ERROR` | Non-classified provider mutation failure. |

Two new codes are reserved:

- `E427_CONNECTOR_REMOVAL_INVALID`: malformed compiled removal, impossible
  identity, legacy/adopted state, collision, or unreconstructible prior
  artifact; and
- `E428_CONNECTOR_REMOVAL_DRIFT`: pre-delete mismatch, DELETE-time 404, or
  failure to prove the absent postcondition after an accepted delete.

An actionable plan emits one aggregate `W119_CONNECTOR_REMOVAL_DESTRUCTIVE`
before an output file is saved; an authorized apply emits the same aggregate
warning after fresh reviewed-plan equality and destructive authorization but
before durable intent. The warning includes only the ordered delete count, not
resource IDs, names, aliases, endpoints, or config. Text/JSON/quiet behavior
uses the existing formatter warning contract. The existing
`W106_LOCAL_STATE_ONLY` warning MUST NOT downgrade the intrinsic PostgreSQL
requirement; removal with local state is an error.

Planner safety blocker codes are
`connector_removal_state_provider_drift` and
`connector_removal_ownership_required`. Both classify the plan as blocked and
destructive authority is never evaluated as a bypass.

## Secrecy and bounded output

The following MUST NOT appear in YAML validation errors, compiled removal
artifacts, reviewed plans, operation control/history, recovery plans, CLI
output, logs, telemetry, or test snapshots:

- raw connector config values;
- passwords, tokens, JAAS strings, secret-provider expansions, or credentials;
- Connect REST endpoints, request headers, TLS paths, or response bodies;
- PostgreSQL DSNs, roles, schemas, lock keys, or connection details; or
- exception text not passed through the established sanitizer.

Allowed evidence is limited to logical resource ID, connector name, cluster
alias, canonical backend identity containing only an endpoint digest, artifact
checksum, observation fingerprints, presence, action, stable categories, and
fixed error codes. Provider bodies and exception text have independent hard
bounds; plan and assessment size is bounded by the 256-declaration project
limit.

## Version and compatibility contract

- Project `apiVersion` remains `streamt.dev/v1alpha1`; the lifecycle field is
  additive and strict.
- Compiled manifest format adds `connector_removals` only when non-empty.
- Ownership `CURRENT_STATE_VERSION` remains 1.
- PostgreSQL deployment-state schema remains version 2; no DDL migration is
  required.
- Reviewed `PLAN_FILE_VERSION` becomes 5 and versions 1 through 4 require
  regeneration.
- `CURRENT_CONTROL_VERSION` becomes 3. Every reader explicitly accepts 1, 2,
  and 3; clear v2 state remains valid for ordinary commands and upgrades only
  on write, while active v1/v2 operations retain legacy recovery semantics.
- `RECOVERY_PLAN_FILE_VERSION` becomes 3. Existing version-1/2 plans remain
  readable only under their original action semantics and cannot represent a
  Connector removal.
- The canonical Connect binding remains version 1.

A legacy `backend: kafka-connect` record is never upgraded implicitly and has
no supported in-place migration in this slice. Re-apply and adoption both fail
closed for that record. A separate reviewed state-reconciliation contract is
required before it can become canonical; no state-only forget operation is
included.

## Hard deferrals

The following deletion kinds require separate normative specifications:

- **Kafka topics:** deletion destroys retained records and needs complete
  consumer-group, downstream graph, protection, and asynchronous broker
  postcondition semantics.
- **Schema Registry subjects:** deletion needs explicit soft versus permanent
  modes, version/reference closure, compatibility-state ownership, and
  multi-subject ordering.
- **Flink jobs:** cancellation is deferred until stable job identity, exact SQL
  and execution-setting evidence, cluster routing, savepoint policy, and
  recoverable cancellation postconditions exist.

Connector removal MUST NOT add generic deletion declarations or state
projection capable of activating any of these kinds.

## Release acceptance

The feature is releasable only when strict DSL, compilation, pure preflight,
plan/action/control serialization, managed mutation, state projection,
recovery, PostgreSQL concurrency, secrecy, installed-wheel, and real Connect
tests all pass. Documentation MUST continue to state that manifest absence is
inert and that only Connector deletion is added by this contract.
