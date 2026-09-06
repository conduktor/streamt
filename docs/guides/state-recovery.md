---
title: Deployment-State Recovery
description: Reviewed, explicit recovery of unfinished streamt operations
---

# Deployment-state recovery

Use this runbook when `streamt state status` reports an `in_progress` or
`recovery_required` operation. Recovery resolves one exact blocked operation;
it is not a general state editor, a retry of runtime mutations, or a stale-lock
cleanup command.

For an interrupted Kafka Streams predicate replacement, use the
[dedicated runner workflow](#interrupted-kafka-streams-replacement) below.
It can continue the original deployment; generic recovery does not retry
runtime mutations.

Recovery is deliberately a two-command, reviewed workflow:

1. `state recovery-plan` locks the state address, rereads the blocked intent,
   and creates integrity-checked evidence. For `observed` and `rolled_back`, it
   also compiles the current project and observes every intended runtime target.
2. A different operator reviews that evidence and the live systems.
3. `state recover` requires the blocked operation ID, selected resolution, and
   evidence checksum again. For an active blocked operation, it locks the
   address, revalidates the project and live targets, then finalizes only if
   they still exactly match the reviewed plan. An exact retry whose reviewed
   result is already present and whose control is already clear skips provider
   observation and verifies the completed resolution from recovery history.

There is no timeout after which a marker becomes safe to clear. Do not edit or
delete a control marker, hand-edit ownership state or history, force-unlock the
store, or retry the blocked runtime action. streamt never recovers
automatically.

## Interrupted Kafka Streams replacement

Retain the original reviewed format-6 plan and the operation UUID returned by
apply. Keep the project's SQL, environment, package version and provider
configuration unchanged. Inspect the exact operation first:

```bash
streamt -o json state runner-status -p . -e prod \
  --plan filter-change.json --operation-id '<original UUID>'
```

Replace the quoted placeholder with the reported canonical UUID. For the
single-environment starter, omit `-e prod` and the confirmation flag below.
This command locks the state address, checks the complete journal/history and
recompiles the current project before inspecting the live generation. It makes
no runtime, ownership or audit writes. Local lock acquisition may create a lock
file. A missing or ambiguous observation cannot authorize continuation.

If the report permits resume, explicitly continue the same operation:

```bash
streamt -o json state resume -p . -e prod --confirm-env prod \
  --plan filter-change.json --operation-id '<original UUID>' --timeout 60
```

Resume may cleanly stop the original runner, remove that exact stopped
container, create its sole candidate, or start a verified candidate. Each step
requires fresh evidence and durable progress under the same operation ID.
Resume preserves application identity, the existing volume and offsets; it
does not initialize a new group. The timeout bounds each wait, not the entire
command, and never permits force kill.

| Observed result | Action |
| --- | --- |
| Interrupted with a safe next boundary | Resume the original plan and operation UUID |
| Old removed, new absent | Resume may create the exact candidate; diagnosis cannot mark this complete |
| Candidate already created after a lost response | Verify and reuse it; do not create another generation |
| Runtime complete, final state/control write interrupted | Resume verifies the terminal evidence before finalizing; an already-written local result is not written twice |
| Control clear with exact completion receipt and ready candidate | Status reports completed; repeated resume verifies without writes |
| Changed SQL, identity, volume, history, or unavailable retained offsets | Stop; retain evidence and investigate the mismatch |

`committed: null` means the invocation cannot establish the outcome. It does
not mean that the write failed. A fresh command can verify an already-committed
receipt even when the previous process missed the final response. A later
project or ownership change can prevent verification of that older result;
these commands are not a general history viewer.

Local finalization archives completion before writing ownership and clearing
control. PostgreSQL commits those changes atomically. Both retain incidents and
resume authorizations. Read-only diagnosis uses the configured operation writer
connection for PostgreSQL locking and its read-only evidence queries; it does
not bypass writer authority using the administrative credential.

Do not delete state, reset offsets, overwrite the reviewed file, create a new
plan against partial runtime state, or retry ordinary apply. Keep external
provider writers paused while investigating. These commands do not recover
arbitrary runner creation failures, stateful migrations or unsupported changes.

## Choose one resolution

The option values use underscores exactly as shown.

| Resolution | Use only when | State result |
| --- | --- | --- |
| `observed` | Fresh observations prove every target is exactly either its prior state or the intended candidate state. One operation may contain a reviewed mix of both. | Writes the exact reviewed ownership result. Its serial increases by one only when the ownership map changes. |
| `rolled_back` | Fresh observations prove every target exactly matches prior ownership state. | Retains the prior ownership payload and serial. |
| `abandoned_before_mutation` | Durable progress is empty, proving that no runtime action started. | Retains the prior ownership payload and serial without contacting runtime targets. |

Age, a stopped client, an expired CI job, or an apparently healthy target is
not proof of `abandoned_before_mutation`. If durable progress contains any
completed action, that resolution is rejected.

`observed` does not import arbitrary live state. For every action it accepts
only an exact prior or exact candidate representation derived from the current
project and a fresh deployment plan. Anything partial, ambiguous, unsupported,
or changed during observation fails closed.

## Before creating a plan

Freeze project changes, streamt mutations, and manual provider changes for the
affected environment. Keep the project checkout and streamt package version
pinned until recovery finishes.

Inspect the blocker without modifying it:

```bash
streamt -o json state status -p . -e prod
```

Record the canonical blocked `operation_id`, backend/store identity, state
address, state serial and checksum, operation kind/status, failure code, and
`last_completed_action_index`. Confirm that they describe the incident you are
investigating. The later recovery plan contains the complete durable progress
and ordered action intent; `state status` intentionally exposes only the safe
summary.

Take and test a backup before recovery:

- For local state, retain the environment's ownership JSON, control sidecar,
  and, if present, recovery-history sidecar together while no streamt command
  is running. They live under `.streamt/state/`.
- For PostgreSQL, take a schema-and-data backup of the complete configured
  state schema using the separate administrative/owner process, retain the
  relevant cluster-role definition, and test restoration into another
  database. A representative backup command is:

  ```bash
  pg_dump "$STREAMT_STATE_ADMIN_DSN" \
    --schema=streamt \
    --format=custom \
    --file=streamt-state-before-recovery.dump
  ```

A backup is incident evidence and a disaster-recovery boundary. Restoring or
editing it is not part of `state recover` and must not be used to bypass an
active marker.

For `observed` and `rolled_back`, also verify that the current project can be
validated and that every runtime provider needed by the blocked action is
reachable with read authority. Recovery observes targets; it never repeats the
blocked runtime mutation.

For Conduktor Gateway, one fresh observation is one bounded pair of sequential
list requests: AliasTopics first, then Interceptors. All desired rules and all
explicit Gateway actions in the blocked intent are resolved from that shared
pair. The two requests are deliberately read-only and bounded, but Gateway does
not make them provider-atomic. Keep Gateway changes frozen throughout planning,
review, and first execution; a change between the two lists can only be rejected
when it produces inconsistent or nonmatching aggregate evidence.

For Kafka Connect, a blocked explicit Connector deletion can be resolved only
with PostgreSQL deployment-state schema version 2. Recovery validates every
durable action, prior managed record, current runtime binding, and competing
desired/removal claim before Connect access. It then performs one strict
resource GET per normalized endpoint/name locator. Current desired Connectors,
retained removal tombstones, and recovery actions that share that locator reuse
the same observation. Freeze manual and non-streamt Connect writers through
planning, independent review, and the first execution; Connect offers no
conditional delete transaction and the PostgreSQL lock serializes only streamt
writers.

## Create the reviewed evidence

The planning syntax is:

```bash
streamt state recovery-plan --resolution RESOLUTION --out PATH
```

`RESOLUTION` must be exactly one of `observed`, `rolled_back`, or
`abandoned_before_mutation`. For example:

```bash
streamt state recovery-plan -p . -e prod \
  --resolution observed \
  --out /secure/recovery/payments-prod-observed.json
```

Use a new destination every time. The command creates the plan atomically as a
regular file with mode `0600`; it refuses to overwrite any existing path and
refuses symlinks. It does not weaken permissions on a copy made later, so keep
transferred copies at `0600` and use a trusted review channel.

The plan binds all of the following:

- the exact store, state address, blocked operation, prior state, control
  marker, durable progress, and ordered action intent;
- the chosen resolution and a new recovery operation ID;
- fresh, normalized presence and fingerprint evidence for every target when
  live observation is required;
- the current environment and manifest fingerprints;
- the exact candidate ownership state for `observed`; and
- an `evidence_checksum` covering the complete plan envelope.

Provider revision tokens, database credentials, and raw provider errors are
not written to the plan. The file still contains operational metadata and must
be treated as sensitive. Its checksum detects modification or truncation; it
is not a signature and does not replace trusted reviewer identity.

Creating a plan does not clear the marker or change ownership state. It may be
discarded safely. Do not edit a plan: create a new file from a fresh observation
instead.

## Independent review

The reviewer should have the incident record, read access to the plan, the
current project revision, and independent read access to the relevant runtime
systems. They should verify:

- `blocked_operation_id` is the UUID currently reported by `state status`;
- the store/address, prior serial/checksum, progress, and ordered actions match
  the status summary, provider audit evidence, and incident timeline;
- `resolution` matches what actually happened, not what was intended;
- every target's `presence`, `accepted_as`, and fingerprint classification is
  credible for the selected resolution;
- the candidate ownership state changes only resources in the blocked intent
  and preserves all unrelated records;
- project/environment/manifest fingerprints correspond to the pinned inputs;
  and
- the complete `evidence_checksum` has not changed between review and
  execution.

The reviewer supplies the exact blocked operation ID, resolution, and checksum
to the executor through the approved change-control channel. Do not copy these
values from a different plan, even if it concerns the same environment.

## Execute the reviewed plan

The execution syntax is:

```bash
streamt state recover \
  --plan PATH \
  --confirm-operation-id BLOCKED_UUID \
  --confirm-resolution RESOLUTION \
  --confirm-evidence-checksum sha256:...
```

For example:

```bash
streamt state recover -p . -e prod \
  --plan /secure/recovery/payments-prod-observed.json \
  --confirm-operation-id 00000000-0000-4000-8000-000000000000 \
  --confirm-resolution observed \
  --confirm-evidence-checksum sha256:0000000000000000000000000000000000000000000000000000000000000000
```

The UUID and checksum above are placeholders. Always paste the exact values
from the one reviewed plan.

Before changing state for an active blocked operation, the command verifies all
three confirmations, the plan checksum and strict schema, configured
store/address identity, current state and control preimage, current project
fingerprints, and fresh live target evidence. Drift produces a non-success
result; it does not partially accept a plan. If the exact reviewed result is
already present and control is already clear, an identical retry does not
re-observe providers: the backend verifies the expected state and exact
recovery operation in its checksum-chained or transactional recovery history.

A successful PostgreSQL recovery atomically appends recovery intent and
resolution history, optionally writes the reviewed ownership revision, and
clears operation control in one transaction. Local recovery uses a crash-safe,
checksum-chained sequence under its file lock. In either backend, recovery
never lowers the ownership serial and an exact completed retry can verify the
same resolution without creating a different result.

After success, inspect status again:

```bash
streamt -o json state status -p . -e prod
```

Require clear operation control, the expected serial/checksum, and the same
store/address. Retain the reviewed plan, command result, status output, and
backup according to the incident-retention policy. Only then unfreeze normal
work. For PostgreSQL, separately confirm that an ordinary writer preflight
succeeds; administrative status does not probe that credential.

## Recovery credentials

Local recovery uses the existing local state authority. PostgreSQL recovery
requires an exact schema-version-2 catalog and the separately bound,
least-privilege writer credential:

```yaml
deployment_state:
  backend: postgres
  namespace: platform
  lock_timeout_seconds: 30
  postgres:
    dsn_env: STREAMT_STATE_ADMIN_DSN
    writer_dsn_env: STREAMT_STATE_RECOVERY_WRITER_DSN
    schema: streamt
```

`postgres.writer_dsn_env` names the environment variable containing the exact
v2 writer DSN used by ordinary plan/apply/adopt and recovery. Inject its value
through the normal secret manager; never
put a DSN in project YAML, a plan, shell history, command output, or an incident
ticket.

Recovery resolves only `writer_dsn_env`. The administrative `dsn_env` is not
used and is never a fallback for recovery. `dsn_env` remains required in the
PostgreSQL configuration shape and is used by separate administration/status
commands such as initialization, status, lock diagnostics, and migration. The
writer and admin variable names must be different.

The writer connection must prove that it is the exact role stored by the v2
catalog, that the catalog and ACL are exact, and that the endpoint is a direct
standalone primary. Every pooler/proxy and every HA or failover topology is
unsupported. streamt cannot reliably detect that a DSN bypasses all poolers,
so direct endpoint control is an operator prerequisite. Ordinary and recovery
commands share this writer boundary; neither can fall back to the owner/admin
credential, local state, or empty state.

Local recovery remains valid for its existing non-removal action boundaries,
but it cannot resolve an explicit Connector deletion. Such a delete is created
only under PostgreSQL v2 and must be recovered under the same exact store,
address, writer identity, catalog/ACL, and direct-primary authority.

## Supported observation boundaries

Support means that a complete fresh planner observation can reconstruct the
canonical evidence required by the ownership checksum. It does not mean every
provider response or every action is recoverable.

| Target | Present target | Absent target | Important boundary |
| --- | --- | --- | --- |
| Schema Registry subject | Exact prior or candidate content can be proven for supported register/update/adopt paths. | Exact absence can prove a not-yet-created prior state or a completed delete candidate. | Partial schema metadata, identity mismatch, or a checksum that cannot be reconstructed fails closed. |
| Kafka topic | Exact partitions, replication factor, and complete config can prove prior or candidate state for supported create/update/adopt paths. | Exact absence can prove a not-yet-created prior state or a completed delete candidate. | Recovery requires a strict, complete config read; filtered or partial broker config is rejected. |
| Kafka Connect connector | Exact connector config can prove supported create/update/adopt prior or candidate state. For an explicit deletion, only exact presence matching the durable current fingerprint and reconstructible prior managed checksum proves the prior result. | Exact absence can prove a not-yet-created prior state. It proves a deletion candidate only for an exact durable control-version-3 Connector `delete` action; manifest/model absence alone is inert. | Explicit deletion recovery is PostgreSQL-v2-only and does not require the lifecycle tombstone to remain in the current project. Evidence is bound to the exact managed prior record, effective default alias, normalized-endpoint fingerprint, and connector name. Partial config/status/task observations, control versions 1/2, legacy or adopted state, competing claims, wrong binding, or any third state fails closed before resolution. |
| Conduktor Gateway rule | The complete alias plus rule-owned interceptor aggregate must match exact durable evidence. A desired match proves a completed create/update candidate; a current match proves the prior result for mutation actions. Alias-only adoption requires the exact reviewed current present surface: `observed` records its candidate ownership and `rolled_back` retains prior absence. | Exact current absence proves an unapplied or rolled-back create. Exact desired absence proves a completed delete candidate. Adoption does not accept absence. | Evidence is bound to the endpoint/vCluster backend identity, provider rule name, alias, aggregate fingerprint, and owned-interceptor count. Anything outside the action-specific exact surfaces fails closed. |
| Flink job | Unsupported. Current live status and job ID cannot prove the managed SQL artifact or execution settings. | Exact absence can prove a not-yet-submitted prior state or a completed cancel candidate. | Any present Flink target fails closed, even when its runtime status looks healthy. |

Additional action constraints apply:

- `rolled_back` requires **every** target to prove its exact prior state. A
  create/register/submit that remains absent is representable. A supported
  schema/topic/connector update may be representable when its exact prior
  artifact checksum can be reconstructed. A Gateway update or delete can prove
  rollback only by matching its exact durable current aggregate surface.
- A non-Gateway, non-Connector delete or cancel plan normally omits the old
  desired artifact. If its target is still present, recovery cannot reconstruct
  the prior artifact checksum and fails closed; presence alone is not proof of
  rollback. Gateway and explicit Connector removal actions instead carry exact
  current and desired fingerprints in durable pre-mutation intent. Connector
  rollback additionally reconstructs and verifies its prior artifact checksum
  from the complete live config.
- Exact absence can prove the candidate result of a delete/cancel only when the
  blocked durable action carries that explicit absent desired surface. A
  Connector deletion specifically requires control-version-3 evidence. Absence
  can also prove the prior result when a create-like action never produced a
  target.
- `observed` may mix prior and candidate classifications across the ordered
  action list. Every individual classification must still be exact.
- Adoption recovery is limited to exact schema-subject, Kafka-topic, bound
  default-cluster Connector, and alias-only Gateway observations declared with
  adopted ownership.

Gateway recovery never infers deletion from the current manifest. An absent
manifest rule is accepted as a deletion target only when the blocked intent
already contains that exact durable `delete` action; only its desired-absent
surface can remove the corresponding ownership record. Ordinary planning does
not currently perform broad removed-rule discovery, so removing a Gateway rule
from the project does not itself create a delete plan or action. Full adoption
of Gateway rules with Interceptors remains unsupported.

Control-version-1 Gateway actions predate the exact current/desired aggregate
evidence. They therefore fail closed for live `observed` or `rolled_back`
recovery before Gateway is contacted. `abandoned_before_mutation` remains
available for such an action only when durable progress is empty, because that
resolution needs no provider observation.

Connector recovery follows the same absence-inert principle with a narrower
state boundary. The lifecycle tombstone may be retained or removed after the
blocked operation because exact control-version-3 action evidence—not current
manifest absence—is the recovery authority. `observed` may remove only the
matching prior managed record when one fresh strict GET proves exact absence.
`rolled_back` requires exact present prior evidence and never accepts absence.
Recovery never issues or retries DELETE. Any different present fingerprint,
unreconstructible checksum, malformed document, wrong endpoint binding,
desired/removal collision, or provider failure preserves the blocker.

Control versions 1 and 2 have no Connector deletion evidence and cannot be
upgraded or inferred from state. Live `observed` or `rolled_back` recovery of
such a deletion fails before Connect is contacted. A generic
`abandoned_before_mutation` outcome remains available only when durable
progress is empty and requires no provider observation.

When a target is outside these boundaries, do not choose the closest-looking
resolution. Preserve the marker and evidence, freeze mutations, and escalate to
the provider-specific incident/disaster-recovery procedure. A manual state edit
or marker deletion would erase the very ambiguity recovery is designed to
contain.

## Generic recovery failure and retry guidance

This table applies to `state recovery-plan` and `state recover` only. For
`state runner-status` or `state resume`, retain the original deployment plan
and follow the [runner workflow](#interrupted-kafka-streams-replacement);
do not create a replacement plan after a partial runner deployment.
All failures are non-success command outcomes with secret-neutral error codes.

| Code | Meaning and safe response |
| --- | --- |
| `E408_PLAN_FILE_INVALID` | The plan or confirmation format is malformed, modified, incomplete, unsupported, too large, a symlink, or mismatched. Do not repair a plan in place; correct a transcription error or create and review a new plan at a new path. |
| `E409_PLAN_STALE` | Project inputs or freshly observed targets no longer match the reviewed evidence. Keep the marker, investigate the drift, and create a new reviewed plan. |
| `E411_STATE_INVALID` | State, control, history, catalog, or address is incompatible. Stop and correct the external cause; never edit streamt metadata as repair. |
| `E419_STATE_RECOVERY_REQUIRED` | Recovery planning found no active unfinished operation, or abandonment is forbidden after durable progress. Inspect `state status`; preserve any marker and choose only a provable outcome. |
| `E420_STATE_BACKEND_UNAVAILABLE` | A dependency, named credential, endpoint, provider, or exact PostgreSQL writer authority is unavailable. Restore the same authority; no local or admin fallback occurs. |
| `E422_STATE_LOCK_TIMEOUT` | The bounded state lock wait expired. Identify the legitimate holder, leave the marker intact, recheck status, and retry only after contention is resolved. Never force-unlock. |
| `E423_STATE_LOCK_LOST` | The command lost state authority. Start no new mutation. Preserve the plan and inspect status for the reported operation ID before deciding whether an exact retry is appropriate. |
| `E424_STATE_CONFLICT` | State or operation control changed after observation. Preserve the incident evidence, inspect status, and create a fresh reviewed plan for the remaining blocker. |
| `E425_STATE_UNKNOWN_OUTCOME` | Recovery may have committed. Do not generate a different resolution or blindly replay. Inspect status, preserve the original file, and use only the exact same plan and confirmations for idempotent verification. |
| `E426_STATE_RELEASE_FAILED_AFTER_COMMIT` | The reviewed recovery commit was verified, but authority release was not. Treat it as committed (`committed: true`), investigate the session/endpoint, and do not replay it as an uncommitted write. |
| `E428_CONNECTOR_REMOVAL_DRIFT` | A started Connector deletion failed to prove exact absence. Preserve the managed ownership record and blocker, then create reviewed recovery evidence; never retry DELETE as incident cleanup. |

Never turn a failed recovery into an `apply` retry. If status is still blocked
after an ordinary validation, observation, availability, or lock-timeout
failure, a fresh `recovery-plan` is the safe default. Exact retry is reserved
for the same recovery file and confirmations after an indeterminate finalization;
changing any of the three confirmations requires a new review. When that exact
result and clear control are already present, retry verification uses recovery
history and does not require the provider surface to remain unchanged after the
completed recovery.

## Topology and HA boundary

PostgreSQL advisory locks belong to one physical session, so recovery must use
a direct connection to one standalone primary for the entire operation. The
primary must durably commit state, control, and history before acknowledgement.
All poolers, replicas, promotion, failover, multi-primary, and other HA
topologies are unsupported, including synchronous replication. Recovery is
not a failover mechanism.

Keep backup and restore monitoring active throughout the incident, and alert
on blocked operation status and `E419`, `E423`, `E425`, or `E426`. The recovery
runbook complements tested restore-based disaster recovery; it does not
replace it or provide an in-place database rollback.

Local state remains single-host authority. Its file lock and sidecars do not
provide cross-host exclusion, shared-runner fencing, or HA durability.
