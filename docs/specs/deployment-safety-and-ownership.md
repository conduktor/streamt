# Deployment safety and ownership

## Status

Required specification. Safety requirements override backward compatibility
with the current orphan-deletion behavior.

## Invariants

1. A live resource is never deleted solely because it is absent from the
   desired project.
2. A resource can be mutated only when its ownership mode permits it.
3. Deletion is computed from previously applied streamt state, not by subtracting
   desired resources from the entire cluster.
4. A partial or selected plan cannot alter resources outside its selection.
5. `apply` executes the exact reviewed plan and rejects stale or modified plans.
6. Destructive behavior defaults to disabled in every environment mode.
7. Unsupported migrations are explicit plan blockers and cannot be bypassed by
   destructive flags.

## Ownership modes

Every declared runtime resource has one of three modes:

| Mode | Meaning | Observe | Create/update | Delete |
|---|---|---:|---:|---:|
| `external` | Exists outside streamt ownership | yes | no | never |
| `managed` | Created and lifecycle-managed by this project | yes | yes | explicit only |
| `adopted` | Existing resource explicitly claimed by this project | yes | yes | explicit only |

Sources default to `external`. Output resources default to `managed` only after
the project successfully creates them. Existing output resources require
explicit adoption before mutation unless the user selects a backend-specific
create-if-absent policy.

### Planned external declaration behavior

The 2026-09-04 [developer workflow](developer-workflow.md) adds an explicit
requirement for external declarations without automatic drift checks. The
table's observation permission does not imply continuous monitoring or ownership.
The exact split between local reference validation and opt-in live inspection
is pending confirmation in the
[execution plan](../plans/2026-09-04-developer-experience-execution.md).
Current commands may still perform live reads; documentation changes do not
implement a no-network mode. Any implementation must preserve the evidence
required for managed mutations and keep import separate from adoption.

## Stable resource identity

Compiled artifacts carry a stable identity independent of display names:

```text
streamt://<project>/<environment>/<kind>/<logical-name>
```

They also carry:

- Project and environment.
- Artifact kind and physical target name.
- Owning source/model/test/exposure.
- Ownership mode.
- Source file and logical configuration path when available.
- Content checksum.

Backend APIs may not support storing this metadata. The state backend is the
authority for ownership; discovery heuristics are never sufficient to delete.

## State model

The last applied state records only resources streamt owns or has adopted:

```json
{
  "state_version": 1,
  "project": "payments",
  "environment": "prod",
  "serial": 12,
  "resources": {
    "streamt://payments/prod/topic/payments_clean": {
      "physical_name": "payments.clean.v1",
      "ownership": "managed",
      "artifact_checksum": "sha256:...",
      "backend": "direct-kafka"
    }
  }
}
```

Local state is acceptable for development but must warn that it is unsuitable
for shared CI. A production direct-apply backend requires remote state and
locking. External deployment backends may use their own state authority.
Local snapshots are isolated by environment at
`.streamt/state/<environment>.json`. On one host, `apply` and `adopt` hold an
exclusive environment operation lock from their authoritative state read
through live observation and state commit; apply also holds it through runtime
mutation and rollback. This closes the prior same-host stale-serial mutation
race. A strict control sidecar records durable intent before mutation, ordered
progress, and conservative recovery-required state, so an interrupted mutation
blocks later local apply/adopt. The file lock is still not distributed, and the
explicit recovery workflow does not make local state safe for shared runners.

PostgreSQL schema version 2 is the supported remote authority for ordinary
plan/apply/adopt and recovery. The production factory resolves only
`writer_dsn_env`, requires the exact stored least-privilege login and ACL, and
reproves catalog conformance plus a direct standalone primary at operation
boundaries. It never falls back to `dsn_env`, local state, or empty state.
Version 1 remains administrative only. Every pooler/proxy and every HA or
failover topology is unsupported; pooler absence is an operator prerequisite
because it cannot be detected reliably from a session.

Operational use additionally requires active, tested schema/data backup and
restore, documented restore-based rollback, rehearsed reviewed recovery, and
monitoring for blocked operation control and state-authority errors. A status
label is not a substitute for those controls or for an ordinary writer
preflight.

## Explicit recovery

`streamt state recovery-plan` and `streamt state recover` resolve one exact
unfinished operation through a reviewed, two-command protocol. The planning
command binds the state/control preimage, ordered intent and progress, selected
resolution, project fingerprints and exact target evidence when required, and a
candidate ownership state into a no-overwrite, mode-`0600` evidence file. The
execution command requires the blocked operation UUID, resolution, and evidence
checksum again, then revalidates state and, when required, project and live
evidence under the operation lock before finalization.

The supported outcomes are `observed`, `rolled_back`, and
`abandoned_before_mutation`. Observed recovery may accept an exact reviewed mix
of targets at prior and candidate state. Rollback requires every target at its
exact prior state. Abandonment is legal only when durable progress proves that
no action started. Recovery never retries runtime mutations, lowers a state
serial, force-unlocks, expires a marker by age, or runs automatically.

Local finalization uses a crash-safe, checksum-chained recovery history under
the same-host lock. PostgreSQL finalization uses the same exact schema-v2 writer
authority as ordinary commands and atomically commits history, optional
ownership state, and control clearing. Present Flink jobs and nonempty or
unreconstructible Gateway rules fail closed; other partial or ambiguous target
observations do as well. The complete operator contract is in the
[deployment-state recovery runbook](../guides/state-recovery.md).

## Planning algorithm

For each desired resource:

- Desired + no live resource: propose create when ownership permits.
- Desired + live + prior ownership: propose an update or no-op.
- Desired + live + no prior ownership: report `requires_adoption`; do not mutate.

For each resource in prior owned state but absent from the full desired project:

- Report a potential removal.
- Propose deletion only when destructive changes are enabled and explicitly
  requested.

For every other live resource:

- Ignore it for lifecycle planning.
- It may still be used as read-only evidence for impact analysis.

When the plan is selected or targeted, removal detection is disabled outside
the selected closure.

## Deterministic safety blockers

Plans carry an ordered, machine-readable `safety_blockers` array. The initial
blockers are:

- `kafka_partition_reduction` when desired partitions are lower than live
  partitions.
- `schema_incompatible` when Schema Registry rejects the desired schema under
  the subject's compatibility policy.
- `flink_update_requires_savepoint` for every existing Flink job update until a
  savepoint-safe or explicitly stateless upgrade workflow exists.
- `flink_resubmit_requires_state_evidence` when a non-running existing job would
  be submitted again without equivalent state evidence.

The canonical order follows backend apply order (Schema Registry, Kafka, then
Flink), followed by physical resource name and blocker code. Verified new-resource
creates and no-ops do not produce safety blockers. An ownership decision that neutralizes a
resource to observe-only/no-op also does not produce one.

Planning succeeds when blockers exist so reviewers and automation can inspect
them. Applying does not: direct apply and reviewed-plan apply both fail with
`E417_SAFETY_BLOCKED` before any backend mutation. `--force` does not override
these blockers.

## Plan/apply protocol

A version 2 saved plan contains the desired manifest checksum, prior-state
serial, environment fingerprint, live-state observations used by the plan,
proposed actions, ordered ownership and safety decisions, and plan checksum.
Version 1 files are rejected as unsupported rather than interpreted without
safety-blocker semantics.

Protected environments always require `apply --plan`. Other shared deployment
workflows opt in explicitly with `safety.require_reviewed_plan: true`; streamt
does not infer sharing or criticality from an environment name. A direct apply
under either policy fails with `E418_REVIEWED_PLAN_REQUIRED` before deployers
are constructed, including when `--confirm`, `--force`, or `--dry-run` is
present. Structured output includes executable `plan --out` and `apply --plan`
next steps with the resolved `--project-dir` preserved, shell-quoted paths, and
a fixed `.streamt/reviewed-plan.json` output name. A reviewed-plan apply still
requires protected-environment confirmation and all checksum, project,
environment, state-serial, live-drift, ownership, and safety-blocker checks
below.

`apply` must reject a plan when:

- Project content changed after planning.
- State serial changed.
- The environment differs.
- The plan is expired under configured policy.
- Required approval or destructive confirmation is absent.
- Safety blockers differ from the reviewed plan or remain unresolved.

## Adoption

`streamt import` discovers resources and emits `external` declarations.

The Kafka import MVP reads the selected environment's runtime configuration,
discovers topics with repeatable include/exclude filters, and optionally reads
the conventional `{topic}-value` Schema Registry subject. It emits explicit
`external` source declarations only. Exact topic matches already present in the
project are skipped; generated-name collisions with sources or models fail the
whole operation. The output must be a new direct child of `sources/`, is strict-
validated before creation, and is durably staged then atomically installed without
replacement through a verified directory handle. Import never overwrites a file,
mutates infrastructure, or writes ownership state. Avro and JSON Schema may populate
columns; Protobuf is retained as a pinned external reference.

`streamt adopt`:

1. Reads the live resource.
2. Shows the exact attributes streamt will begin managing.
3. Requires explicit resource and environment confirmation.
4. Writes an ownership entry without changing the resource.
5. Produces a new plan before any later mutation.

Bulk adoption requires a saved selection and non-interactive confirmation token
suitable for CI review.

### Single-resource adoption

The command intentionally supports one resource at a time. Kafka topic adoption
uses:

```text
streamt adopt -p PATH -e ENV --kind topic --name LOGICAL_NAME
```

`LOGICAL_NAME` must resolve through compiled artifact ownership to exactly one
physical topic, and the declaration must explicitly use
`ownership.mode: adopted`. The command performs only Kafka topic observation;
it never creates, updates, or deletes a topic. It shows the live and desired
managed attributes plus pending differences before confirmation, with secrets
redacted.

Interactive confirmation uses an exact token containing the canonical resource
ID and environment. Non-interactive confirmation requires both
`--confirm-resource streamt://...` and `--confirm-env ENV` to match. On success,
only the adopted record is added to the configured environment-scoped state,
all other records are retained, and the serial advances once. An identical
existing claim is idempotent. Conflicting state fails closed. Users must produce
and review a fresh plan before later mutation; normal apply also replans against
the new state.

Schema Registry subject adoption uses the same protocol:

```text
streamt adopt -p PATH -e ENV --kind schema --name LOGICAL_NAME
```

The logical owner must resolve to exactly one compiled subject with explicit
`ownership.mode: adopted`. streamt reads that exact subject and validates its
version, schema ID, type, compatibility, and references. Review output contains
only metadata and deterministic hashes, never schema bodies. The state record
uses the planner-identical artifact checksum and `schema-registry` backend, so a
fresh plan can safely distinguish the adopted subject from unowned live data.
No register, compatibility-update, list, or delete API is called.

Kafka Connect connector adoption uses the same state-only protocol:

```text
streamt adopt -p PATH -e ENV --kind connector --name LOGICAL_NAME
```

The compiled owner must resolve to exactly one adopted Connector. An omitted
cluster or explicit cluster equal to the configured default is accepted; an
explicit non-default cluster fails closed. A Connector's logical identity uses
`ownership.owner_name`; its provider locator
binds the effective Connect cluster alias, a normalized REST-endpoint
fingerprint, and the connector name. The strict observer uses one
`GET /connectors/<encoded-name>` response and fingerprints stable name and
configuration only. The command performs that exact read before confirmation
and again afterward for a new claim, rejects fingerprint drift, and calls no
Connect mutation, list, status, or task endpoint. An identical claim returns
after the first read, without confirmation or a state write. Exact case and
JSON types are preserved, reserved generated configuration keys cannot be
overridden, and raw configuration never enters output or durable control data;
review uses checksums for the whole configuration and sanitized changed-key
categories/directions, never per-value fingerprints. Legacy
`backend: kafka-connect` records are unbound and fail closed rather than
silently authorizing the selected cluster.

Connector adoption inherits the same local and PostgreSQL v2 state-operation,
locking, intent, compare-and-swap, and idempotency boundaries as topic and
schema adoption. Reviewed Connector recovery uses the same strict managed
observation and rejects partial, status/task-based, or unbound evidence.

Alias-only Gateway adoption now uses the same state-only protocol. It accepts
one exact compiled adopted rule only when both the desired artifact and
selected live aggregate have zero owned Interceptors. Two complete sequential
Gateway observations bracket confirmation, exact action evidence makes an
uncertain state commit recoverable, and no mutation endpoint is called. The
safe sequence is specified in the [extended resource adoption
plan](../plans/2026-09-02-extended-resource-adoption.md).

A Gateway rule has compound provider identity: the backend and vCluster-scoped
AliasTopic key plus the exact deterministic set of scoped Interceptor keys. Its
physical topic and interceptor plugin class, priority, scope, and transformed
configuration are managed content. Planning, status, apply, rollback,
recovery, and adoption share one strict normalized aggregate;
list order, broad name prefixes, or a logical rule name substituted for an
alias are not identity evidence. The shipped adoption slice is limited to an
exact alias whose desired and observed interceptor sets are both empty. Full
interceptor adoption remains unsupported.

Flink adoption is deferred further. Runtime status and a display-name match do
not prove SQL or execution settings. Stable per-job provider identity, strict
cluster routing, collision-free ownership for every emitted job,
provider-visible artifact fingerprints, exact discovery, savepoint/stateless
lifecycle semantics, and evidence-gated state advancement are prerequisites.
Reviewed recovery must pass on that same evidence before Flink adoption can be
offered.

## Destructive operations

Topic deletion, subject deletion, connector deletion, state reset, and
state-incompatible Flink replacement are destructive. Partition reduction is
not an overridable destructive operation because Kafka does not support it.

They require:

- A full, non-targeted plan.
- Previous streamt ownership.
- Environment policy permitting destructive changes.
- An explicit destructive flag or dedicated destroy command.
- A reviewed plan checksum.

Topic and stateful-job deletion should support an environment-level policy that
forbids them entirely.

Until stateful migration semantics land, every planned Flink update is blocked;
it cannot reach the current cancel-and-resubmit implementation.

## Immediate compatibility behavior

Local ownership state is persisted after successful direct applies. Automatic
orphan deletion remains disabled until removal is an explicit, reviewed
workflow backed by state appropriate to the deployment environment. This
intentionally trades cleanup convenience for safety.
